"""
Collision detection module using spatial and temporal analysis.
Pure Spark SQL implementation - no Python UDFs for Windows compatibility.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from config import (
    COLLISION_THRESHOLD_NM, EARTH_RADIUS_NM,
    TIME_BUCKET_MINUTES, DEBUG_MODE, MAX_SOG, MIN_SOG,
    MIN_SOG_DIFF, MIN_SOG_MAX, MAX_PAIR_BUCKETS, NM_PER_DEG_LAT,
    ENABLE_PROXIMITY_VALIDATION
)
from utils import setup_logger


logger = setup_logger(__name__)


class CollisionDetector:
    """Detects potential vessel collisions using pure Spark SQL operations."""
    
    def __init__(self, spark):
        self.spark = spark
    
    def detect(self, df_filtered: DataFrame) -> DataFrame:
        """
        Execute collision detection pipeline using pure Spark SQL.
        
        Args:
            df_filtered: Cleaned AIS DataFrame
        
        Returns:
            DataFrame with collision candidates
        """
        # Cache for multiple uses
        df_filtered.cache()
        
        # Stage 1: Add time bucketing for temporal grouping
        df_indexed = self._add_time_index(df_filtered)
        logger.info(f"Added temporal indexing to {df_indexed.count():,} records")
        
        # Stage 2: Coarse spatial filtering using coordinate ranges
        # Instead of H3 (UDF-based), use geographic grid cells
        df_indexed = self._add_spatial_grid(df_indexed)
        logger.info("Added spatial grid indexing")
        
        # Stage 3: Repartition for efficient self-join
        df_indexed = df_indexed.repartition(200, "TimeBucket", "GridCell")
        df_indexed.persist(StorageLevel.MEMORY_AND_DISK)
        logger.info("Repartitioned for self-join")
        
        # Stage 4: Self-join to generate candidate pairs
        candidates = self._self_join_candidates(df_indexed)
        logger.info(f"Generated {candidates.count():,} candidate pairs")
        
        # Stage 5: Calculate distances and DCPA/TCPA
        candidates = self._calculate_distances_and_collision_metrics(candidates)
        
        # Stage 6: Filter by collision threshold
        collisions = self._filter_collisions(candidates)
        logger.info(f"After collision threshold filter: {collisions.count():,} candidates")
        
        # Stage 6b: Add stricter proximity validation (optional, pure Spark SQL)
        if ENABLE_PROXIMITY_VALIDATION:
            collisions = self._validate_pair_proximity(df_indexed, collisions)
            logger.info(f"After pair proximity validation: {collisions.count():,} candidates")
        else:
            logger.info("Proximity validation skipped (disabled in config)")
        
        # Stage 7: Deduplicate by vessel pair
        collisions = self._deduplicate_candidates(collisions)
        logger.info(f"After deduplication: {collisions.count():,} final candidates")
        
        return collisions
    
    # ─── TEMPORAL INDEXING ──────────────────────────────────
    
    @staticmethod
    def _add_time_index(df: DataFrame) -> DataFrame:
        """Add minute-level time bucketing for temporal grouping."""
        return df.withColumn(
            "TimeBucket",
            F.date_trunc("minute", F.col("Timestamp"))
        )
    
    # ─── SPATIAL GRID INDEXING (REPLACES H3 UDF) ───────────
    
    @staticmethod
    def _add_spatial_grid(df: DataFrame) -> DataFrame:
        """
        Add spatial grid cells based on coordinate ranges.
        
        Uses simple lat/lon binning instead of H3 to avoid UDFs.
        ~0.5 degree grid ≈ 30nm, suitable for 50nm search radius.
        """
        GRID_SIZE = 0.5  # degrees
        
        df = df.withColumn(
            "GridCell",
            F.concat(
                F.floor(F.col("Latitude") / F.lit(GRID_SIZE)).cast("string"),
                F.lit("_"),
                F.floor(F.col("Longitude") / F.lit(GRID_SIZE)).cast("string")
            )
        )
        
        return df
    
    # ─── SELF-JOIN FOR CANDIDATE GENERATION ─────────────────
    
    @staticmethod
    def _self_join_candidates(df: DataFrame) -> DataFrame:
        """
        Self-join on time bucket + spatial grid.
        
        Only compares vessels in:
        - Same minute bucket
        - Same grid cell
        - MMSI_A < MMSI_B (each pair appears once)
        """
        df_a = df.alias("a")
        df_b = df.alias("b")
        
        candidates = df_a.join(
            df_b,
            (F.col("a.TimeBucket") == F.col("b.TimeBucket")) &
            (F.col("a.GridCell") == F.col("b.GridCell")) &
            (F.col("a.MMSI") < F.col("b.MMSI")),
            how="inner"
        )
        
        return candidates
    
    # ─── DISTANCE AND COLLISION METRICS ─────────────────────
    
    @staticmethod
    def _calculate_distances_and_collision_metrics(candidates: DataFrame) -> DataFrame:
        """
        Calculate Haversine distance and DCPA/TCPA in pure Spark SQL.
        
        All calculations use Spark's built-in math functions.
        """
        # Haversine distance formula
        # d = R * acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1))
        candidates = candidates.withColumn("vessel_distance_nm",
            F.acos(
                F.sin(F.radians(F.col("a.Latitude"))) * 
                F.sin(F.radians(F.col("b.Latitude"))) +
                F.cos(F.radians(F.col("a.Latitude"))) * 
                F.cos(F.radians(F.col("b.Latitude"))) *
                F.cos(F.radians(F.col("b.Longitude")) - F.radians(F.col("a.Longitude")))
            ) * F.lit(EARTH_RADIUS_NM)
        )
        
        # DCPA/TCPA calculation using relative velocity
        # Relative velocity components (nm/hr)
        candidates = candidates.withColumn("rel_vel_east",
            F.col("b.SOG") * F.sin(F.radians(F.col("b.COG"))) -
            F.col("a.SOG") * F.sin(F.radians(F.col("a.COG")))
        ).withColumn("rel_vel_north",
            F.col("b.SOG") * F.cos(F.radians(F.col("b.COG"))) -
            F.col("a.SOG") * F.cos(F.radians(F.col("a.COG")))
        )
        
        # Relative position components (nm)
        candidates = candidates.withColumn("rel_pos_east",
            (F.col("b.Longitude") - F.col("a.Longitude")) * F.lit(60.0)
        ).withColumn("rel_pos_north",
            (F.col("b.Latitude") - F.col("a.Latitude")) * F.lit(60.0)
        )
        
        # Relative speed squared
        rel_vel_mag_sq = (F.col("rel_vel_east") * F.col("rel_vel_east") +
                          F.col("rel_vel_north") * F.col("rel_vel_north"))
        
        # TCPA in hours, then convert to seconds
        candidates = candidates.withColumn("tcpa_hours",
            F.when(rel_vel_mag_sq > F.lit(0.01),
                -(F.col("rel_pos_east") * F.col("rel_vel_east") +
                  F.col("rel_pos_north") * F.col("rel_vel_north")) / rel_vel_mag_sq
            ).otherwise(F.lit(0.0))
        )
        
        # DCPA: distance at closest point
        candidates = candidates.withColumn("dcpa",
            F.when(rel_vel_mag_sq > F.lit(0.01),
                F.sqrt(
                    F.pow(F.col("rel_pos_east") + F.col("rel_vel_east") * F.col("tcpa_hours"), 2) +
                    F.pow(F.col("rel_pos_north") + F.col("rel_vel_north") * F.col("tcpa_hours"), 2)
                )
            ).otherwise(F.col("vessel_distance_nm"))
        )
        
        return candidates.drop("rel_vel_east", "rel_vel_north", "rel_pos_east", "rel_pos_north")
    
    # ─── COLLISION FILTERING ────────────────────────────────
    
    @staticmethod
    def _filter_collisions(candidates: DataFrame) -> DataFrame:
        """
        Apply collision thresholds and extract result columns.
        
        DCPA ≤ 0.03nm (collision threshold = ~55 meters)
        TCPA ≥ -1.0 hours (within 1 hour of closest point)
        Actual distance at collision time ≤ 100m (0.054nm)
        Both vessels must be moving (SOG > MIN_SOG)
        """
        # Convert 100 meters to nautical miles: 100m / 1852 = 0.054nm
        MAX_DISTANCE_NM = 0.054
        
        return candidates.filter(
            (F.col("dcpa") <= F.lit(COLLISION_THRESHOLD_NM)) &
            (F.col("tcpa_hours") >= F.lit(-1.0)) &
            (F.col("a.SOG") > F.lit(MIN_SOG)) &  # Changed to > (not >=) for moving vessels only
            (F.col("b.SOG") > F.lit(MIN_SOG)) &
            (F.col("vessel_distance_nm") > F.lit(0.0)) &
            (F.col("vessel_distance_nm") <= F.lit(MAX_DISTANCE_NM))  # NEW: Filter out distances > 100m
        ).select(
            F.col("a.MMSI").alias("MMSI_A"),
            F.col("b.MMSI").alias("MMSI_B"),
            F.col("a.Timestamp").alias("first_ts"),
            F.col("a.Timestamp").alias("Timestamp"),
            F.col("a.TimeBucket").alias("TimeBucket"),
            F.col("a.Latitude").alias("Lat_A"),
            F.col("a.Longitude").alias("Lon_A"),
            F.col("b.Latitude").alias("Lat_B"),
            F.col("b.Longitude").alias("Lon_B"),
            F.col("a.SOG").alias("SOG_A"),
            F.col("b.SOG").alias("SOG_B"),
            F.col("a.Heading").alias("HDG_A"),
            F.col("b.Heading").alias("HDG_B"),
            F.col("a.Name").alias("Name_A"),
            F.col("b.Name").alias("Name_B"),
            F.col("a.ShipType").alias("ShipType_A"),
            F.col("b.ShipType").alias("ShipType_B"),
            F.col("vessel_distance_nm"),
            F.col("tcpa_hours").alias("TCPA"),
            F.col("dcpa").alias("DCPA"),
            F.lit(0).alias("duration_seconds"),
            F.lit(0).alias("ping_count")
        )
    
    # ─── PAIR PROXIMITY VALIDATION ───────────────────────────
    
    @staticmethod
    def _validate_pair_proximity(df_indexed: DataFrame, collisions: DataFrame) -> DataFrame:
        """
        Validate that collision pairs have multiple close time/location points.
        Pure Spark SQL implementation - no driver-side collection.
        
        Requirement: Pair must have at least 2 records within collision time bucket
        for EACH vessel (A and B).
        """
        # Count records per MMSI per time bucket
        vessel_counts = df_indexed.groupBy("MMSI", "TimeBucket").count()
        
        # Join collision pairs with vessel counts
        # For vessel A
        collisions_with_counts_a = collisions.join(
            vessel_counts.select(
                F.col("MMSI").alias("MMSI_A"),
                F.col("TimeBucket"),
                F.col("count").alias("count_a")
            ),
            on=["MMSI_A", "TimeBucket"],
            how="inner"
        )
        
        # For vessel B
        collisions_with_counts_ab = collisions_with_counts_a.join(
            vessel_counts.select(
                F.col("MMSI").alias("MMSI_B"),
                F.col("TimeBucket"),
                F.col("count").alias("count_b")
            ),
            on=["MMSI_B", "TimeBucket"],
            how="inner"
        )
        
        # Filter: require >= 2 records for both vessels
        valid_collisions = collisions_with_counts_ab.filter(
            (F.col("count_a") >= 2) & (F.col("count_b") >= 2)
        ).select(collisions.columns)
        
        # Failsafe: if this eliminates too many, skip validation
        valid_count = valid_collisions.count()
        original_count = collisions.count()
        
        if valid_count == 0:
            logger.warning(f"Proximity validation eliminated {original_count} collisions, skipping validation")
            return collisions
        
        logger.info(f"Proximity validation: {original_count} -> {valid_count} collisions (kept {100*valid_count/max(original_count,1):.1f}%)")
        
        return valid_collisions
    
    # ─── DEDUPLICATION ──────────────────────────────────────
    
    @staticmethod
    def _deduplicate_candidates(collisions: DataFrame) -> DataFrame:
        """
        Deduplicate by vessel pair (MMSI_A, MMSI_B).
        Keep record with smallest DCPA (closest approach).
        """
        w = Window.partitionBy("MMSI_A", "MMSI_B").orderBy(F.col("dcpa"))
        
        return collisions.withColumn("rn", F.row_number().over(w)) \
            .filter(F.col("rn") == 1) \
            .drop("rn")

