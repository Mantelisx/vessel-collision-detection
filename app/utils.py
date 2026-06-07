"""
Utility functions for AIS data processing and collision analysis.
"""

import math
import os
import sys
import glob
import zipfile
import logging
from typing import List, Tuple, Optional

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType

from config import (
    DATA_DIR, OUTPUT_DIR, LOG_LEVEL, DEBUG_MODE,
    H3_RESOLUTION, EARTH_RADIUS_NM, NM_PER_DEG_LAT,
    COLREG_HEADON_THRESHOLD, COLREG_OVERTAKING_MIN, COLREG_OVERTAKING_MAX,
    CENTER_LAT, CENTER_LON
)


# ─── LOGGING SETUP ──────────────────────────────────────────

def setup_logger(name: str) -> logging.Logger:
    """Configure logger for pipeline module."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, LOG_LEVEL))
    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    
    if not logger.handlers:
        logger.addHandler(handler)
    
    return logger


logger = setup_logger(__name__)


# ─── DATA FILE DISCOVERY ────────────────────────────────────

def discover_ais_files(data_dir: str = DATA_DIR, sample_day: str = "all") -> Tuple[List[str], bool]:
    """
    Discover AIS CSV files or ZIP archive.
    
    Args:
        data_dir: Directory to search
        sample_day: 'all' for all files, or day number (1-31) for single day
    
    Returns:
        Tuple of (list of CSV paths, bool indicating if extracted)
    """
    # Determine which files to look for
    if sample_day != "all":
        # Pad day number with leading zero
        day_str = str(sample_day).zfill(2)
        file_pattern = f"aisdk-2021-12-{day_str}.csv"
        search_pattern = file_pattern
    else:
        search_pattern = "aisdk-2021-12-*.csv"
    
    csv_files = glob.glob(os.path.join(data_dir, search_pattern))
    zip_files = glob.glob(os.path.join(data_dir, "*.zip"))
    
    if csv_files:
        logger.info(f"Found {len(csv_files)} CSV file(s) in {data_dir}")
        return csv_files, False
    
    if zip_files:
        logger.info(f"No CSVs found. Extracting {zip_files[0]}...")
        try:
            with zipfile.ZipFile(zip_files[0], "r") as z:
                z.extractall(data_dir)
            logger.info("Extraction complete.")
            
            csv_files = glob.glob(os.path.join(data_dir, search_pattern))
            return csv_files, True
        except Exception as e:
            logger.error(f"Failed to extract ZIP: {e}")
            raise
    
    logger.error(f"No CSV or ZIP files found in {data_dir}")
    raise FileNotFoundError(f"No AIS data found in {data_dir}")


# ─── SPARK UDFs FOR SPATIAL OPERATIONS ──────────────────────

# Note: H3 UDFs removed - using pure Spark SQL grid-based spatial indexing instead
# This avoids Python UDF communication issues on Windows while maintaining efficiency.


# ─── GEOGRAPHIC CALCULATIONS ────────────────────────────────

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate great-circle distance using Haversine formula.
    
    Args:
        lat1, lon1: First point (degrees)
        lat2, lon2: Second point (degrees)
    
    Returns:
        Distance in nautical miles
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlon_rad = math.radians(lon2 - lon1)
    
    a = math.sin((lat2_rad - lat1_rad) / 2) ** 2 + \
        math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon_rad / 2) ** 2
    
    c = 2 * math.asin(math.sqrt(a))
    return EARTH_RADIUS_NM * c


def bearing_to_point(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate initial bearing from point 1 to point 2 (degrees, 0-360).
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlon_rad = math.radians(lon2 - lon1)
    
    x = math.sin(dlon_rad) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - \
        math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon_rad)
    
    bearing = (math.degrees(math.atan2(x, y)) + 360) % 360
    return bearing


# ─── COLREG COLLISION CLASSIFICATION ───────────────────────

def classify_colreg(lat_a: float, lon_a: float, hdg_a: float,
                     lat_b: float, lon_b: float) -> str:
    """
    Classify collision scenario per COLREG.

    
    Args:
        lat_a, lon_a: Vessel A position
        hdg_a: Vessel A heading (degrees)
        lat_b, lon_b: Vessel B position
    
    Returns:
        COLREG classification string with details
    """
    if hdg_a is None or lat_b is None:
        return "Unknown (missing heading)"
    
    abs_bearing = bearing_to_point(lat_a, lon_a, lat_b, lon_b)
    rel_bearing = (abs_bearing - hdg_a) % 360
    
    if rel_bearing < COLREG_HEADON_THRESHOLD or rel_bearing > 360 - COLREG_HEADON_THRESHOLD:
        return f"relative bearing {rel_bearing:.1f}°"
    elif COLREG_OVERTAKING_MIN <= rel_bearing <= COLREG_OVERTAKING_MAX:
        return f"relative bearing {rel_bearing:.1f}°"
    else:
        return f"relative bearing {rel_bearing:.1f}°"


# ─── DATA VALIDATION ────────────────────────────────────────

def validate_mmsi(mmsi: int, min_val: int, max_val: int, 
                  excluded_ranges: List[Tuple[int, int]]) -> bool:
    """
    Validate MMSI per ITU-R M.585-9 https://www.itu.int/dms_pubrec/itu-r/rec/m/R-REC-M.585-10-202604-I!!PDF-E.pdf.
    
    Args:
        mmsi: MMSI value
        min_val: Minimum valid MMSI
        max_val: Maximum valid MMSI
        excluded_ranges: List of (min, max) ranges to exclude
    
    Returns:
        True if valid, False otherwise
    """
    if mmsi < min_val or mmsi > max_val:
        return False
    
    for min_range, max_range in excluded_ranges:
        if min_range <= mmsi <= max_range:
            return False
    
    return True


# ─── SPARK DATAFRAME UTILITIES ──────────────────────────────

def format_collision_report(collision_data: dict) -> str:
    """
    Format a collision record into human-readable report.
    
    Args:
        collision_data: Dictionary with collision details
    
    Returns:
        Formatted string
    """
    report = f"""
{'─'*70}
COLLISION EVENT DETAILS
{'─'*70}
Vessel A:        {collision_data.get('Name_A', 'N/A')} (MMSI: {collision_data['MMSI_A']})
Vessel B:        {collision_data.get('Name_B', 'N/A')} (MMSI: {collision_data['MMSI_B']})
Timestamp:       {collision_data['first_ts']} UTC
Location:        {collision_data.get('Lat_A', 0):.6f}°N, {collision_data.get('Lon_A', 0):.6f}°E
Distance:        {collision_data.get('vessel_distance_nm', 0)*1852:.1f} meters
DCPA:            {collision_data.get('DCPA', 0)*1852:.1f} meters
TCPA:            {collision_data.get('TCPA', 0)*60:.1f} minutes
SOG A / B:       {collision_data.get('SOG_A', 0):.1f} / {collision_data.get('SOG_B', 0):.1f} knots
Ship Type A/B:   {collision_data.get('ShipType_A', 'N/A')} / {collision_data.get('ShipType_B', 'N/A')}

{'─'*70}
"""
    return report

# COLREG Scenario: {collision_data.get('scenario', 'Unknown')}
# Duration:        {collision_data.get('duration_seconds', 0)} seconds
# Ping Count:      {collision_data.get('ping_count', 0)} pings

def log_pipeline_stage(stage_name: str, record_count: int, elapsed_time_sec: float = None) -> None:
    """Log completion of a pipeline stage."""
    msg = f"✓ {stage_name}: {record_count:,} records"
    if elapsed_time_sec:
        msg += f" ({elapsed_time_sec:.1f}s)"
    logger.info(msg)


# ─── OUTPUT DIRECTORY MANAGEMENT ─────────────────────────────

def ensure_output_dir() -> str:
    """Ensure output directory exists and return path."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info(f"Output directory: {OUTPUT_DIR}")
    return OUTPUT_DIR


def get_output_path(filename: str) -> str:
    """Get full path for output file."""
    return os.path.join(OUTPUT_DIR, filename)
