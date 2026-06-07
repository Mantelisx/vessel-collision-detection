"""
Data cleaning module for AIS collision detection.
Implements geographic filtering, stationary vessel detection, and noise removal.
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

from config import (
    CENTER_LAT, CENTER_LON, RADIUS_NM, GEO_LAT_DELTA, GEO_LON_DELTA,
    EARTH_RADIUS_NM, MAX_SOG, MIN_SOG, STATIONARY_STATUSES,
    MIN_MMSI, MAX_MMSI, EXCLUDED_MMSI_RANGES, TEST_MMSI,
    DEBUG_MODE
)
from utils import setup_logger


logger = setup_logger(__name__)


class DataCleaner:
    """Orchestrates data cleaning and filtering for AIS data."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def clean(self, df: DataFrame) -> DataFrame:
        """
        Execute full cleaning pipeline.
        
        Args:
            df: Raw AIS DataFrame
        
        Returns:
            Cleaned DataFrame with all filters applied
        """
        df = self._parse_timestamp(df)
        logger.info(f"parsing")
        
        df = self._filter_mobile_type(df)
        logger.info(f"mobile type filter records")
        
        df = self._filter_class_a_only(df)
        logger.info(f"Class A vessels only")
        
        df = self._filter_valid_mmsi(df)
        logger.info(f"MMSI validation records")
        
        df = self._filter_valid_coordinates(df)
        logger.info(f"coordinate validation records")
        
        df = self._filter_gps_anomalies(df)
        logger.info(f"GPS anomaly filter")
        
        df = self._geographic_filter(df)
        logger.info(f"geographic filter")
        
        df = self._filter_port_zones(df)
        logger.info(f"port zone exclusion")
        
        df = self._filter_ship_types(df)
        logger.info(f"ship type filtering")
        
        df = self._filter_stationary_vessels(df)
        logger.info(f"stationary vessel filter")
        
        return df
    
    # ─── INDIVIDUAL FILTERS ─────────────────────────────────
    
    @staticmethod
    def _parse_timestamp(df: DataFrame) -> DataFrame:
        """Parse timestamp strings to Spark timestamp type."""
        return df.withColumn(
            "Timestamp",
            F.to_timestamp(F.col("Timestamp"), "dd/MM/yyyy HH:mm:ss")
        ).filter(F.col("Timestamp").isNotNull())
    
    @staticmethod
    def _filter_mobile_type(df: DataFrame) -> DataFrame:
        """
        Keep only Class A and Class B vessels.
        Excludes base stations, other device types.
        """
        return df.filter(F.col("TypeOfMobile").isin(["Class A", "Class B"]))
    
    @staticmethod
    def _filter_class_a_only(df: DataFrame) -> DataFrame:
        """
        Keep only Class A vessels (stricter filtering).
        Excludes Class B and all other device types.
        """
        return df.filter(F.col("TypeOfMobile") == "Class A")
    
    @staticmethod
    def _filter_valid_mmsi(df: DataFrame) -> DataFrame:
        """
        Validate MMSI per ITU-R M.585-9.
        
        Excludes:
        - Non-9-digit MMSIs
        - Repeated digit patterns (test MMSIs)
        - Known test MMSI (123456789)
        - Special ranges (SAR aircraft, navigational aids, AtoN)
        """
        for min_range, max_range in EXCLUDED_MMSI_RANGES:
            df = df.filter(
                ~((F.col("MMSI") >= min_range) & (F.col("MMSI") <= max_range))
            )
        
        return df.filter(
            F.col("MMSI").isNotNull() &
            (F.col("MMSI") >= MIN_MMSI) &
            (F.col("MMSI") <= MAX_MMSI) &
            ~((F.col("MMSI") % 111111111) == 0) &  # Repeated digit patterns
            (F.col("MMSI") != TEST_MMSI)
        )
    
    @staticmethod
    def _filter_valid_coordinates(df: DataFrame) -> DataFrame:
        """Validate lat/lon are in valid ranges and not null."""
        return df.filter(
            (F.col("Latitude").between(-90, 90)) &
            (F.col("Longitude").between(-180, 180)) &
            F.col("Latitude").isNotNull() &
            F.col("Longitude").isNotNull()
        )
    
    @staticmethod
    def _filter_gps_anomalies(df: DataFrame) -> DataFrame:
        """
        Detect GPS jumps/teleportation using implied speed.
        
        Calculates speed between consecutive positions.
        Speeds > MAX_SOG indicate GPS errors, not real movement.
        
        Per Liu et al. (2023): AIS errors occur during collection,
        transmission, and reception phases.
        """
        speed_window = Window.partitionBy("MMSI").orderBy("Timestamp")
        
        df = df.withColumn("prev_lat", F.lag("Latitude", 1).over(speed_window)) \
               .withColumn("prev_lon", F.lag("Longitude", 1).over(speed_window)) \
               .withColumn("prev_ts", F.lag("Timestamp", 1).over(speed_window)) \
               .withColumn("time_diff_hrs",
                   (F.unix_timestamp("Timestamp") - F.unix_timestamp("prev_ts")) / 3600.0
               )
        
        # Haversine distance calculation
        df = df.withColumn("implied_speed_nm",
            F.when(F.col("time_diff_hrs") > 0,
                F.acos(
                    F.sin(F.radians(F.col("prev_lat"))) * F.sin(F.radians(F.col("Latitude"))) +
                    F.cos(F.radians(F.col("prev_lat"))) * F.cos(F.radians(F.col("Latitude"))) *
                    F.cos(F.radians(F.col("Longitude")) - F.radians(F.col("prev_lon")))
                ) * F.lit(EARTH_RADIUS_NM) / F.col("time_diff_hrs")
            ).otherwise(None)
        )
        
        # Filter: implied speed must be null (first record) or <= MAX_SOG
        df = df.filter(
            F.col("implied_speed_nm").isNull() |
            (F.col("implied_speed_nm") <= MAX_SOG)
        )
        
        return df.drop("prev_lat", "prev_lon", "prev_ts", "time_diff_hrs", "implied_speed_nm")
    
    @staticmethod
    def _geographic_filter(df: DataFrame) -> DataFrame:
        """
        Geographic filter: 50nm radius from center point (55.225°N, 14.245°E).
        
        Two-stage approach:
        1. Fast bounding box pre-filter (arithmetic comparison)
        2. Precise Haversine distance calculation
        """
        # Stage 1: Bounding box pre-filter (fast, rough)
        df = df.filter(
            (F.col("Latitude").between(CENTER_LAT - GEO_LAT_DELTA, CENTER_LAT + GEO_LAT_DELTA)) &
            (F.col("Longitude").between(CENTER_LON - GEO_LON_DELTA, CENTER_LON + GEO_LON_DELTA))
        )
        
        # Stage 2: Haversine distance (precise, computed only on candidates)
        df = df.withColumn("dist_from_center",
            F.acos(
                F.sin(F.radians(F.lit(CENTER_LAT))) * F.sin(F.radians(F.col("Latitude"))) +
                F.cos(F.radians(F.lit(CENTER_LAT))) * F.cos(F.radians(F.col("Latitude"))) *
                F.cos(F.radians(F.col("Longitude")) - F.radians(F.lit(CENTER_LON)))
            ) * F.lit(EARTH_RADIUS_NM)
        )
        
        df = df.filter(F.col("dist_from_center") <= RADIUS_NM)
        
        return df.drop("dist_from_center")
    
    @staticmethod
    def _filter_port_zones(df: DataFrame) -> DataFrame:
        """
        Exclude vessels in port zones.
        Ports typically have NavigationalStatus indicating mooring/anchoring.
        Also exclude known major port coordinates in Baltic Sea.
        """
        # Known port zones in Baltic (lat/lon bounding boxes)
        # Copenhagen, Stockholm, Helsinki, Gdansk, etc.
        port_zones = [
            # Copenhagen area
            (55.3, 55.8, 12.4, 12.9),
            # Stockholm area  
            (58.9, 59.5, 17.8, 18.5),
            # Helsinki area
            (59.9, 60.5, 24.8, 25.5),
            # Gdansk area
            (54.3, 54.9, 18.5, 19.1),
            # Malmö area
            (55.5, 55.7, 12.9, 13.1),
        ]
        
        # Filter out port zones
        for lat_min, lat_max, lon_min, lon_max in port_zones:
            df = df.filter(
                ~((F.col("Latitude").between(lat_min, lat_max)) &
                  (F.col("Longitude").between(lon_min, lon_max)))
            )
        
        return df
    
    @staticmethod
    def _filter_ship_types(df: DataFrame) -> DataFrame:
        """
        Exclude undefined and special ship types.
        Filters out: undefined, pilot boats, tugs (often stationary/anchored), and those that are close to each other for helping purposes.
        """
        # Ship types to exclude (from IEC 61162-1 standard)
        excluded_types = [
            "Undefined",
            "Pilot",
            "Pilot Vessel",
            "Undefined - Default Type",
            "Law enforcement",#
            # "RESCUE GAD RAUSING",#double check
            # "RESCUE MADS JAKOBSEN",#double check
            "SAR",#
            ""
        ]
        
        return df.filter(
            (~F.col("ShipType").isin(excluded_types)) &
            (F.col("ShipType").isNotNull()) &
            (F.col("ShipType") != "")
        )
    
    @staticmethod
    def _filter_stationary_vessels(df: DataFrame) -> DataFrame:
        """
        Filter out stationary vessels.
        
        Dual criteria:
        1. SOG > MIN_SOG (maritime standard: 0.5 knots minimum)
        2. NavigationalStatus not in STATIONARY_STATUSES
        
        This ensures only moving vessels participate in collision detection.
        """
        return df.filter(
            (F.col("SOG") > MIN_SOG) &
            (~F.col("NavigationalStatus").isin(STATIONARY_STATUSES))
        )


def clean_ais_data(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Public interface for data cleaning.
    
    Args:
        spark: SparkSession instance
        df: Raw AIS DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    cleaner = DataCleaner(spark)
    return cleaner.clean(df)
