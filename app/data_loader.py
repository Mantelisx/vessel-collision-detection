"""
Data loading module for AIS collision detection.
Handles CSV reading, schema definition, and initial data ingestion.
"""

import os
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)

from config import (
    DATA_DIR, AIS_PATTERN, H3_RESOLUTION, MIN_MMSI, MAX_MMSI,
    EXCLUDED_MMSI_RANGES, TEST_MMSI, DEBUG_MODE, SAMPLE_DAY
)
from utils import setup_logger, discover_ais_files


logger = setup_logger(__name__)


def get_ais_schema() -> StructType:
    """
    Define schema for AIS CSV files.
    
    Column mapping from "# Timestamp" header (positional assignment).
    This avoids inferSchema double-scan and ensures type safety.
    
    Returns:
        PySpark StructType schema
    """
    return StructType([
        StructField("Timestamp", StringType(), True),
        StructField("TypeOfMobile", StringType(), True),
        StructField("MMSI", LongType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("NavigationalStatus", StringType(), True),
        StructField("ROT", DoubleType(), True),
        StructField("SOG", DoubleType(), True),
        StructField("COG", DoubleType(), True),
        StructField("Heading", DoubleType(), True),
        StructField("IMO", StringType(), True),
        StructField("Callsign", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("ShipType", StringType(), True),
        StructField("CargoType", StringType(), True),
        StructField("Width", DoubleType(), True),
        StructField("Length", DoubleType(), True),
        StructField("TypeOfPositionFixingDevice", StringType(), True),
        StructField("Draught", DoubleType(), True),
        StructField("Destination", StringType(), True),
        StructField("ETA", StringType(), True),
        StructField("DataSourceType", StringType(), True),
        StructField("A", DoubleType(), True),
        StructField("B", DoubleType(), True),
        StructField("C", DoubleType(), True),
        StructField("D", DoubleType(), True),
    ])


def load_ais_data(spark: SparkSession, data_dir: str = DATA_DIR, sample_day: str = None) -> tuple:
    """
    Load AIS CSV files into Spark DataFrame.
    
    Handles two scenarios:
    1. Pre-extracted CSV files in data_dir
    2. ZIP archive in data_dir (auto-extracts)
    
    Args:
        spark: SparkSession instance
        data_dir: Directory containing CSVs or ZIP file
        sample_day: Optional day number to load (1-31). If None, uses SAMPLE_DAY config.
    
    Returns:
        Tuple of (DataFrame, list of loaded files, was_extracted: bool)
    
    Raises:
        FileNotFoundError: No data files found
        Exception: Spark read error
    """
    # Use provided sample_day or fall back to config
    if sample_day is None:
        sample_day = SAMPLE_DAY
    
    # Discover data files (extracts ZIP if needed)
    csv_files, was_extracted = discover_ais_files(data_dir, sample_day=sample_day)
    
    if not csv_files:
        raise FileNotFoundError(f"No AIS CSV files found in {data_dir}")
    
    # Build glob pattern for CSV(s)
    if sample_day != "all":
        # Ensure day is zero-padded (e.g., "13" or "03")
        day_padded = str(sample_day).zfill(2)
        data_path = os.path.join(data_dir, f"aisdk-2021-12-{day_padded}.csv")
        logger.info(f"SAMPLE MODE: Reading single day (Dec {day_padded})")
    else:
        data_path = os.path.join(data_dir, AIS_PATTERN)
    
    logger.info(f"Reading {len(csv_files)} CSV file(s) from {data_path}")
    
    try:
        # Read with predefined schema (avoids inferSchema scan)
        df = spark.read.csv(
            data_path,
            header=True,
            schema=get_ais_schema(),
            ignoreLeadingWhiteSpace=True,
            ignoreTrailingWhiteSpace=True
        )
        
        row_count = df.count()
        logger.info(f"✓ Loaded {row_count:,} AIS records from {len(csv_files)} files")
        
        if DEBUG_MODE:
            logger.debug(f"Schema:\n{df.printSchema()}")
            logger.debug(f"First row:\n{df.first()}")
        
        return df, csv_files, was_extracted
        
    except Exception as e:
        logger.error(f"Failed to read AIS data: {e}")
        raise


def validate_schema(df) -> bool:
    """
    Validate DataFrame has expected AIS schema columns.
    
    Args:
        df: Spark DataFrame to validate
    
    Returns:
        True if valid, raises Exception otherwise
    """
    required_cols = {
        "Timestamp", "MMSI", "Latitude", "Longitude", "NavigationalStatus",
        "SOG", "COG", "Heading", "Name", "ShipType", "TypeOfMobile"
    }
    
    actual_cols = set(df.columns)
    
    if not required_cols.issubset(actual_cols):
        missing = required_cols - actual_cols
        raise ValueError(f"Missing required columns: {missing}")
    
    logger.info(f"✓ Schema validation passed ({len(actual_cols)} columns)")
    return True
