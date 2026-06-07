"""
Post-detection collision verification module.
Implements physics-based verification to eliminate false positives.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import (
    MAX_COLLISION_DURATION_SEC, MIN_PING_COUNT, MAX_PRE_EVENT_COG_STDDEV,
    PRE_EVENT_WINDOW_MIN
)
from utils import setup_logger


logger = setup_logger(__name__)


class CollisionVerifier:
    """Verifies collision candidates using post-detection filters."""
    
    def verify(self, collisions: DataFrame, df_filtered: DataFrame) -> DataFrame:
        """
        Execute post-detection verification pipeline.
        
        Two physics-based filters:
        1. Collision duration: instantaneous contact (~30sec max)
        2. Pre-event trajectory: at least one vessel on straight course
        
        Args:
            collisions: Raw collision candidates from detector
            df_filtered: Original cleaned AIS DataFrame
        
        Returns:
            Verified collision DataFrame
        """
        # Cache collisions for multiple operations
        collisions.cache()
        
        # Duration filter
        duration_check = self._duration_filter(collisions)
        logger.info(f"After duration filter: {duration_check.count():,} events")
        
        # Trajectory linearity filter
        duration_survivors = duration_check.select(
            "MMSI_A", "MMSI_B", "first_ts"
        ).cache()
        
        pre_a = self._pre_event_trajectory(
            duration_survivors, df_filtered, "A"
        )
        pre_b = self._pre_event_trajectory(
            duration_survivors, df_filtered, "B"
        )
        
        # Combine filters
        collisions_verified = duration_check \
            .join(pre_a, on=["MMSI_A", "MMSI_B"], how="left") \
            .join(pre_b, on=["MMSI_A", "MMSI_B"], how="left") \
            .withColumn("min_cog_stddev",
                F.least(
                    F.coalesce(F.col("cog_stddev_A"), F.lit(999.0)),
                    F.coalesce(F.col("cog_stddev_B"), F.lit(999.0))
                )
            ).filter(
                F.col("min_cog_stddev") <= MAX_PRE_EVENT_COG_STDDEV
            )
        
        logger.info(f"After trajectory verification: {collisions_verified.count():,} events")
        
        # Join back to get full collision details
        final_result = collisions_verified.join(
            collisions.select(
                "MMSI_A", "MMSI_B", "Name_A", "Name_B",
                "ShipType_A", "ShipType_B",
                "Lat_A", "Lon_A", "Lat_B", "Lon_B",
                "SOG_A", "SOG_B", "COG_A", "COG_B",
                "HDG_A", "HDG_B", "Length_A", "Length_B",
                "vessel_distance_nm", "TCPA", "DCPA"
            ).distinct(),
            on=["MMSI_A", "MMSI_B"],
            how="inner"
        ).orderBy("vessel_distance_nm")
        
        return final_result
    
    # ─── DURATION FILTER ────────────────────────────────────
    
    @staticmethod
    def _duration_filter(collisions: DataFrame) -> DataFrame:
        """
        Filter by collision duration.
        
        Real collision: instantaneous contact (~30 seconds max)
        Operational proximity (boarding, escort): lasts minutes
        
        Also filters by minimum ping count to exclude single coincidences.
        """
        duration_check = collisions.groupBy("MMSI_A", "MMSI_B").agg(
            F.min("Timestamp").alias("first_ts"),
            F.max("Timestamp").alias("last_ts"),
            F.count("Timestamp").alias("ping_count"),
            (F.unix_timestamp(F.max("Timestamp")) -
             F.unix_timestamp(F.min("Timestamp"))).alias("duration_seconds"),
            F.min("vessel_distance_nm").alias("min_distance")
        ).filter(
            (F.col("duration_seconds") <= MAX_COLLISION_DURATION_SEC) &
            (F.col("ping_count") >= MIN_PING_COUNT)
        )
        
        return duration_check
    
    @staticmethod
    def _pre_event_trajectory(survivors: DataFrame, df_filtered: DataFrame,
                              vessel: str) -> DataFrame:
        """
        Analyze pre-event trajectory for straight-line movement.
        
        Calculates Course Over Ground standard deviation.
        
        Args:
            survivors: Collision duration survivors
            df_filtered: Original cleaned AIS data
            vessel: "A" or "B" for vessel identification
        
        Returns:
            DataFrame with COG standard deviation
        """
        mmsi_col = f"MMSI_{vessel}"
        cog_col = f"pre_COG_{vessel}"
        stddev_col = f"cog_stddev_{vessel}"
        
        pre_event = survivors.join(
            df_filtered.select(
                F.col("MMSI").alias(mmsi_col),
                F.col("Timestamp").alias("pre_ts"),
                F.col("COG").alias(cog_col)
            ),
            on=mmsi_col,
            how="inner"
        ).filter(
            (F.col("pre_ts") < F.col("first_ts")) &
            (F.col("pre_ts") >= F.col("first_ts") - F.expr(f"INTERVAL {PRE_EVENT_WINDOW_MIN} MINUTES"))
        ).groupBy("MMSI_A", "MMSI_B").agg(
            F.stddev(cog_col).alias(stddev_col)
        )
        
        return pre_event
