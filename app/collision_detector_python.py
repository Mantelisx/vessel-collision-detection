"""
Pure Python collision detection (avoids Spark UDF issues on Windows).
Uses pandas + h3 for spatial indexing without Spark UDFs.
"""

import logging
import pandas as pd
import h3
import numpy as np

from config import (
    H3_RESOLUTION, COLLISION_THRESHOLD_NM, MAX_SOG, MIN_SOG, MIN_SOG_DIFF,
    EARTH_RADIUS_NM, TIME_BUCKET_MINUTES, DEBUG_MODE
)

logger = logging.getLogger(__name__)


class PythonCollisionDetector:
    """Pure Python collision detector using pandas + h3."""
    
    def __init__(self):
        self.h3_resolution = H3_RESOLUTION
        self.collision_threshold = COLLISION_THRESHOLD_NM
    
    def detect(self, df_spark):
        """
        Detect collisions using pure Python (avoids Spark UDF issues).
        
        Args:
            df_spark: Spark DataFrame with cleaned AIS data
        
        Returns:
            Pandas DataFrame with collision candidates
        """
        # Convert Spark DF to Pandas for processing
        logger.info("Converting Spark DF to Pandas for collision detection...")
        df = df_spark.toPandas()
        logger.info(f"Converted {len(df):,} records to pandas")
        
        # Parse timestamps
        df['Timestamp_dt'] = pd.to_datetime(df['Timestamp'])
        
        # Add H3 cells
        logger.info("Computing H3 cells...")
        df['h3_cell'] = df.apply(
            lambda row: h3.latlng_to_cell(row['Latitude'], row['Longitude'], self.h3_resolution)
            if row['Latitude'] is not None and row['Longitude'] is not None
            else None,
            axis=1
        )
        
        # Time bucketing
        df['time_bucket'] = df['Timestamp_dt'].dt.floor('1min')
        
        # Find collision pairs per H3 cell and time bucket
        logger.info("Finding collision candidates...")
        collisions = []
        
        for h3_cell in df['h3_cell'].dropna().unique():
            df_cell = df[df['h3_cell'] == h3_cell]
            
            for time_bucket in df_cell['time_bucket'].unique():
                df_bucket = df_cell[df_cell['time_bucket'] == time_bucket]
                
                if len(df_bucket) < 2:
                    continue
                
                # Find all pairs within this bucket
                mmsis = df_bucket['MMSI'].values
                for i in range(len(mmsis)):
                    for j in range(i + 1, len(mmsis)):
                        if mmsis[i] != mmsis[j]:  # Different vessels
                            collision = self._check_collision_pair(
                                df_bucket.iloc[i],
                                df_bucket.iloc[j]
                            )
                            if collision:
                                collisions.append(collision)
        
        if not collisions:
            logger.info("No collision candidates found")
            return pd.DataFrame()
        
        df_collisions = pd.DataFrame(collisions)
        logger.info(f"✓ Found {len(df_collisions):,} collision candidates")
        return df_collisions
    
    def _check_collision_pair(self, row_a, row_b) -> dict:
        """Check if two vessel pings represent a collision."""
        
        # Distance calculation
        lat1 = float(row_a['Latitude'])
        lon1 = float(row_a['Longitude'])
        lat2 = float(row_b['Latitude'])
        lon2 = float(row_b['Longitude'])
        
        distance_nm = self._haversine(lat1, lon1, lat2, lon2)
        
        # Quick reject if too far
        if distance_nm > self.collision_threshold * 2:
            return None
        
        # DCPA/TCPA calculation
        sog_a = float(row_a.get('SOG', 0) or 0)
        sog_b = float(row_b.get('SOG', 0) or 0)
        cog_a = float(row_a.get('COG', 0) or 0)
        cog_b = float(row_b.get('COG', 0) or 0)
        
        # Speed differential check
        sog_diff = abs(sog_a - sog_b)
        if sog_diff < MIN_SOG_DIFF:
            return None  # Similar speed = unlikely collision
        
        # DCPA calculation via relative velocity projection
        dcpa_nm = self._calculate_dcpa(
            lat1, lon1, lat2, lon2,
            sog_a, cog_a, sog_b, cog_b
        )
        
        # Collision threshold check
        if dcpa_nm > self.collision_threshold:
            return None
        
        return {
            'MMSI_A': int(row_a['MMSI']),
            'MMSI_B': int(row_b['MMSI']),
            'Name_A': row_a.get('Name', ''),
            'Name_B': row_b.get('Name', ''),
            'Lat_A': lat1,
            'Lon_A': lon1,
            'Lat_B': lat2,
            'Lon_B': lon2,
            'SOG_A': sog_a,
            'SOG_B': sog_b,
            'COG_A': cog_a,
            'COG_B': cog_b,
            'Distance_NM': distance_nm,
            'DCPA_NM': dcpa_nm,
            'Timestamp': row_a['Timestamp'],
        }
    
    def _haversine(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance in nautical miles."""
        lat1_rad = np.radians(lat1)
        lat2_rad = np.radians(lat2)
        dlon_rad = np.radians(lon2 - lon1)
        
        a = np.sin((lat2_rad - lat1_rad) / 2) ** 2 + \
            np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon_rad / 2) ** 2
        
        c = 2 * np.arcsin(np.sqrt(a))
        return EARTH_RADIUS_NM * c
    
    def _calculate_dcpa(self, lat1, lon1, lat2, lon2, sog1, cog1, sog2, cog2) -> float:
        """
        Calculate Distance of Closest Point of Approach.
        Simplified version using Cartesian approximation.
        """
        # Cartesian approximation for short distances
        dy = (lat2 - lat1) * 60  # Convert to nautical miles
        dx = (lon2 - lon1) * 60 * np.cos(np.radians((lat1 + lat2) / 2))
        
        # Relative velocity
        cog1_rad = np.radians(cog1)
        cog2_rad = np.radians(cog2)
        
        vx1 = sog1 * np.sin(cog1_rad)
        vy1 = sog1 * np.cos(cog1_rad)
        vx2 = sog2 * np.sin(cog2_rad)
        vy2 = sog2 * np.cos(cog2_rad)
        
        dvx = vx1 - vx2
        dvy = vy1 - vy2
        
        # Avoid division by zero
        speed_rel_sq = dvx**2 + dvy**2
        if speed_rel_sq < 0.0001:  # Parallel courses
            return np.sqrt(dx**2 + dy**2)
        
        # Time to closest approach
        t = -(dx * dvx + dy * dvy) / speed_rel_sq
        
        if t < 0:  # Already passing
            return np.sqrt(dx**2 + dy**2)
        
        # Position at closest approach
        x_closest = dx + dvx * t
        y_closest = dy + dvy * t
        
        return np.sqrt(x_closest**2 + y_closest**2)
