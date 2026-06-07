"""
Configuration module for AIS Collision Detection Pipeline.
Defines constants, tuning parameters, and validation thresholds.
"""

import os
from dataclasses import dataclass

# ─── ENVIRONMENT VARIABLES ───────────────────────────────────

# DATA_DIR: Resolves in this order:
# 1. Environment variable $DATA_DIR if set
# 2. Relative path "aisdk-2021-12" if it exists in current directory
# 3. Otherwise fallback to relative path "aisdk-2021-12" (will error if missing)
_data_dir_env = os.environ.get("DATA_DIR", None)
if _data_dir_env:
    DATA_DIR = _data_dir_env
elif os.path.isdir("aisdk-2021-12"):
    DATA_DIR = "aisdk-2021-12"
else:
    DATA_DIR = "aisdk-2021-12"  # Will error gracefully if not found

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# Sample day mode: Set to 'all' for full pipeline or '13' to run only Dec 13
SAMPLE_DAY = os.environ.get("SAMPLE_DAY", "all").lower()

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─── DATA PATHS ──────────────────────────────────────────────

AIS_PATTERN = "aisdk-2021-12-*.csv"
AIS_YEAR_MONTH = "2021-12"  # For logging/filtering


# ─── SPARK CONFIGURATION ─────────────────────────────────────
@dataclass
class SparkConfig:
    """PySpark session configuration."""
    app_name: str = "AIS Collision Detection"
    master: str = "local[*]"
    driver_memory: str = "8g"
    executor_memory: str = "4g"
    shuffle_partitions: int = 50  # Reduced for Windows stability
    adaptive_enabled: bool = True
    broadcast_threshold: str = "50mb"
    
    def to_dict(self):
        """Convert to Spark config dict."""
        return {
            "spark.driver.memory": self.driver_memory,
            "spark.executor.memory": self.executor_memory,
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.adaptive.enabled": str(self.adaptive_enabled).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.autoBroadcastJoinThreshold": self.broadcast_threshold,
            "spark.port.maxRetries": "100",
            # Python worker configuration (Windows compatibility)
            "spark.python.worker.reuse": "true",
            "spark.python.worker.memory": "2g",
            "spark.python.daemon.module": "pyspark.worker",
            "spark.executorEnv.PYTHONUNBUFFERED": "1",
            # Disable Arrow to avoid Windows socket timeout issues
            "spark.sql.execution.arrow.pyspark.enabled": "false",
        }


# ─── H3 SPATIAL INDEXING ─────────────────────────────────────

# H3 resolution 8 → ~0.5km hexagon diameter
# Balances spatial accuracy vs computational overhead
H3_RESOLUTION = 8


# ─── GEOGRAPHIC FILTERING ────────────────────────────────────

# Center coordinate for 50nm circular filter
CENTER_LAT = 55.225000
CENTER_LON = 14.245000

# Geographic filter radius in nautical miles
RADIUS_NM = 50.0

# Bounding box margins (slightly larger than 50nm circle)
# Avoids edge clipping effects before Haversine refinement
GEO_LAT_DELTA = 0.8
GEO_LON_DELTA = 1.2

# Earth radius in nautical miles (standard value)
# Used in Haversine and DCPA/TCPA calculations
EARTH_RADIUS_NM = 3440.065

# 1 degree latitude = 60 nautical miles (exact)
# Used in relative position calculations
NM_PER_DEG_LAT = 60.0


# ─── VESSEL STATE FILTERS ────────────────────────────────────

# Minimum speed to be considered a "moving" vessel (knots)
# User requirement: SOG > 1.0 knot minimum
MIN_SOG = 1.0

# Maximum plausible vessel speed (knots)
# Used to detect GPS anomalies/teleportation
MAX_SOG = 50.0

# Navigational statuses indicating stationary vessels
# These are filtered out before collision detection
STATIONARY_STATUSES = [
    "At anchor",
    "Moored",
    "Aground",
    "Not under command"
]


# ─── COLLISION CANDIDATE FILTERS ─────────────────────────────

# Distance threshold for collision (nautical miles)
# 0.03nm ≈ 55 meters: captures only actual close collision risk
# Eliminates false positives at 2+ km distances
# Real collision: vessels within 50-100 meters for contact risk
COLLISION_THRESHOLD_NM = 0.03

# Minimum speed differential between pair (knots)
# Filters out convoy/fleet pairs moving at similar speeds
MIN_SOG_DIFF = 2.0

# Minimum speed of faster vessel in pair (knots)
# Ensures at least one vessel is moving meaningfully
MIN_SOG_MAX = 5.0

# Maximum distinct minute buckets a pair can appear in
# Real collision: 1-2 buckets max
# Convoy/fleet: multiple consecutive buckets
MAX_PAIR_BUCKETS = 2


# ─── POST-DETECTION VERIFICATION ────────────────────────────
# Enable stricter pair proximity validation (requires >= 2 points per vessel)
# Set to False for faster processing, True for higher-quality results
ENABLE_PROXIMITY_VALIDATION = True
# Maximum collision duration (seconds)
# Real collision: instantaneous contact (~30 sec max)
# Operational proximity (boarding, escort): sustains for minutes
MAX_COLLISION_DURATION_SEC = 30

# Minimum ping count to verify collision
# Eliminates single GPS coincidences
MIN_PING_COUNT = 2

# Maximum COG (Course Over Ground) standard deviation (degrees)
# Used to verify at least one vessel on straight trajectory
# Per COLREG Rule 7: constant bearing = collision course
MAX_PRE_EVENT_COG_STDDEV = 2.0

# Pre-event window for trajectory analysis (minutes)
PRE_EVENT_WINDOW_MIN = 10


# ─── MMSI VALIDATION ────────────────────────

# Valid MMSI range: 9 digits
MIN_MMSI = 100000000
MAX_MMSI = 999999999

# Test MMSI to exclude
TEST_MMSI = 123456789

# MMSI ranges to exclude:
EXCLUDED_MMSI_RANGES = [
    (111000000, 111999999),  # SAR aircraft
    (970000000, 999999999),  # Navigational aids + AtoN
]


# ─── TIME BUCKETING ──────────────────────────────────────────

# Time bucket granularity for grouping AIS pings
# 1-minute buckets: AIS Class A transmits every 2-10s
# so multiple pings typically fall per bucket
TIME_BUCKET_MINUTES = 1


# ─── VISUALIZATION ───────────────────────────────────────────

# Map zoom level for collision maps
MAP_ZOOM_START = 13

# Trajectory colors (Matplotlib palette)
TRAJECTORY_COLOR_A = "#d62728"  # Red
TRAJECTORY_COLOR_B = "#1f77b4"  # Blue

# Icon colors for marker points
ICON_COLOR_START = "red"
ICON_COLOR_END = "darkred"
ICON_COLOR_COLLISION = "orange"

# Trajectory window (minutes before/after collision)
TRAJECTORY_WINDOW_MIN = 10


# ─── LOGGING AND DEBUG ───────────────────────────────────────

# Enable detailed logging of filtering operations
DEBUG_MODE = os.environ.get("DEBUG", "false").lower() == "true"

# Cache strategy for frequently reused DataFrames
CACHE_STRATEGY = "MEMORY_AND_DISK"  # or "MEMORY_ONLY"


# ─── COLREG CLASSIFICATION ──────────────────────────────────

# Relative bearing thresholds for COLREG rules (degrees)
COLREG_HEADON_THRESHOLD = 22.5  # Rule 14: Head-on
COLREG_OVERTAKING_MIN = 112.5   # Rule 13: Overtaking
COLREG_OVERTAKING_MAX = 247.5   # Rule 13: Overtaking
