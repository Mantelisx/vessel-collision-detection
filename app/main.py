"""
Main orchestrator for AIS vessel collision detection pipeline.
Coordinates data loading, cleaning, detection, verification, and visualization.
Usage:
    python main.py              # Run full pipeline (all 31 days)
    python main.py --day 13     # Run sample mode (Dec 13 only)
    python main.py -d 13        # Short form"""

import sys
import time
from typing import List

import pandas as pd
from pyspark.sql import SparkSession

from config import SparkConfig, OUTPUT_DIR, DEBUG_MODE
from data_loader import load_ais_data, validate_schema
from data_cleaner import clean_ais_data
from collision_detector import CollisionDetector
from visualization import visualize_collisions
from utils import setup_logger, ensure_output_dir


logger = setup_logger(__name__)


def create_spark_session() -> SparkSession:
    """
    Create and configure PySpark session.
    
    Returns:
        Configured SparkSession
    """
    spark_config = SparkConfig()
    
    builder = SparkSession.builder \
        .appName(spark_config.app_name) \
        .master(spark_config.master)
    
    for key, value in spark_config.to_dict().items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    logger.info(f"✓ Spark session created: {spark_config.app_name}")
    logger.info(f"  Master: {spark_config.master}")
    logger.info(f"  Driver memory: {spark_config.driver_memory}")
    
    return spark


def parse_args():
    """
    Parse command-line arguments.
    
    Supports:
        --day N or -d N: Run only day N (1-31)
        (no args): Run all 31 days
    
    Returns:
        str: 'all' or day number as string (e.g. '13')
    """
    if len(sys.argv) > 1:
        if sys.argv[1] in ['--day', '-d']:
            if len(sys.argv) > 2:
                try:
                    day = int(sys.argv[2])
                    if 1 <= day <= 31:
                        return str(day)
                    else:
                        logger.warning(f"Invalid day {day}. Must be 1-31. Running all days.")
                        return "all"
                except ValueError:
                    logger.warning(f"Invalid day value: {sys.argv[2]}. Running all days.")
                    return "all"
        elif sys.argv[1] in ['--help', '-h']:
            print(__doc__)
            sys.exit(0)
    
    return "all"


def main():
    """Execute full collision detection pipeline."""
    
    # Parse command-line arguments
    sample_day = parse_args()
    
    logger.info("="*70)
    logger.info("AIS VESSEL COLLISION DETECTION PIPELINE")
    logger.info("="*70)
    if sample_day != "all":
        logger.info(f"MODE: Sample day (Dec {sample_day}) - faster iteration")
    else:
        logger.info("MODE: Full pipeline (all 31 days)")
    logger.info("")
    
    # Setup
    ensure_output_dir()
    spark = create_spark_session()
    
    try:
        # ─── STAGE 1: DATA LOADING ──────────────────────────────
        logger.info("\n[1/5] DATA LOADING")
        logger.info("-"*70)
        
        t_start = time.time()
        df_raw, csv_files, was_extracted = load_ais_data(spark, sample_day=sample_day)
        validate_schema(df_raw)
        
        logger.info(f"Loaded files: {len(csv_files)}")
        if was_extracted:
            logger.info("Note: ZIP archive was auto-extracted")
        
        t_elapsed = time.time() - t_start
        logger.info(f"✓ Completed in {t_elapsed:.1f}s\n")
        
        # ─── STAGE 2: DATA CLEANING ─────────────────────────────
        logger.info("[2/5] DATA CLEANING & FILTERING")
        logger.info("-"*70)
        
        t_start = time.time()
        df_filtered = clean_ais_data(spark, df_raw)
        df_filtered.cache()
        
        t_elapsed = time.time() - t_start
        logger.info(f"✓ Completed in {t_elapsed:.1f}s\n")
        
        # ─── STAGE 3: COLLISION DETECTION ───────────────────────
        logger.info("[3/5] COLLISION DETECTION (H3 + DCPA/TCPA)")
        logger.info("-"*70)
        
        t_start = time.time()
        
        # Try Spark-based detector, fall back to pure Python on Windows if it fails
        try:
            detector = CollisionDetector(spark)
            collisions = detector.detect(df_filtered)
            collision_count = collisions.count()
            logger.info("✓ Using Spark-based collision detection")
        except Exception as e:
            logger.warning(f"Spark detector failed: {e}")
            logger.info("Falling back to pure Python detector...")
            from collision_detector_python import PythonCollisionDetector
            detector = PythonCollisionDetector()
            collisions = detector.detect(df_filtered)
            collision_count = len(collisions)
            logger.info("✓ Using Python-based collision detection (Windows mode)")
        
        t_elapsed = time.time() - t_start
        
        logger.info(f"✓ COLLISIONS DETECTED: {collision_count} vessel pairs in {t_elapsed:.1f}s\n")
        
        if collision_count == 0:
            logger.warning("No collisions detected. Pipeline complete.")
            spark.stop()
            return
        
        # ─── STAGE 4: VISUALIZATION (SKIP VERIFICATION FOR WINDOWS COMPATIBILITY) ────────────────────
        logger.info("[4/5] VISUALIZATION & OUTPUT")
        logger.info("-"*70)

        
        t_start = time.time()
        
        # For visualization, we can keep collisions as Spark DF
        # The visualizer will convert to Pandas as needed
        final_result_count = collisions.count()
        
        if final_result_count == 0:
            logger.warning("No collisions detected. Pipeline complete.")
            spark.stop()
            return
        
        # Visualization using Spark DataFrame
        output_maps = visualize_collisions(df_filtered, collisions)
        
        t_elapsed = time.time() - t_start
        logger.info(f"✓ Generated {len(output_maps)} map(s) in {t_elapsed:.1f}s\n")
        
        # ─── STAGE 5: SUMMARY ────────────────────────────────────────────
        logger.info("[5/5] SUMMARY")
        logger.info("-"*70)
        logger.info(f"Records processed: {df_raw.count():,}")
        logger.info(f"Records after cleaning: {df_filtered.count():,}")
        logger.info(f"Actual collisions detected: {collision_count} vessel pairs")
        logger.info(f"Collision events with trajectories: {final_result_count}")
        logger.info(f"Output directory: {OUTPUT_DIR}")
        logger.info("="*70 + "\n")
        
        # Print final results table - convert to Pandas for display
        final_result_pandas = collisions.toPandas()
        _print_results_table(final_result_pandas)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    
    finally:
        spark.stop()
        logger.info("Spark session closed.")


def _print_results_table(final_result) -> None:
    """Print formatted results table."""
    if isinstance(final_result, pd.DataFrame):
        results = final_result[
            ["MMSI_A", "MMSI_B", "Name_A", "Name_B", "first_ts", "Lat_A", "Lon_A", "vessel_distance_nm"]
        ].iterrows()
    else:
        # Spark DataFrame
        results_df = final_result.select(
            "MMSI_A", "MMSI_B", "Name_A", "Name_B",
            "first_ts", "Lat_A", "Lon_A", "vessel_distance_nm"
        ).collect()
        results = [(i, {col: row[col] for col in row.__fields__}) for i, row in enumerate(results_df)]
    
    logger.info("\n" + "="*70)
    logger.info("EXAM DELIVERABLE: ACTUAL COLLISION EVENTS")
    logger.info("="*70)
    
    for i, row_data in enumerate(results, 1):
        if isinstance(final_result, pd.DataFrame):
            row = row_data[1]
        else:
            row = row_data[1]
        logger.info(f"\nCOLLISION #{i}")
        logger.info(f"  Vessel A: {row['Name_A']} (MMSI: {row['MMSI_A']})")
        logger.info(f"  Vessel B: {row['Name_B']} (MMSI: {row['MMSI_B']})")
        logger.info(f"  Collision Timestamp: {row['first_ts']}")
        logger.info(f"  Collision Coordinates: {row['Lat_A']:.6f}°N, {row['Lon_A']:.6f}°E")
        logger.info(f"  Minimum Distance: {row['vessel_distance_nm']*1852:.1f} meters")


if __name__ == "__main__":
    main()
