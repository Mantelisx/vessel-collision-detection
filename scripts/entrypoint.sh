#!/bin/bash
# Docker entrypoint script for AIS Collision Detection Pipeline

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print header
echo -e "${GREEN}═══════════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}AIS VESSEL COLLISION DETECTION PIPELINE${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════════════${NC}\n"

# Validate environment
echo "Validating environment..."

# Default values if not set
DATA_DIR="${DATA_DIR:-/app/aisdk-2021-12}"
OUTPUT_DIR="${OUTPUT_DIR:-/app/output}"

if [ ! -d "$DATA_DIR" ]; then
    echo -e "${RED}✗ Data directory not found: $DATA_DIR${NC}"
    exit 1
fi

if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Creating output directory..."
    mkdir -p "$OUTPUT_DIR"
fi

# Check for data files
CSV_COUNT=$(find "$DATA_DIR" -name "aisdk-2021-12-*.csv" 2>/dev/null | wc -l)
ZIP_COUNT=$(find "$DATA_DIR" -name "*.zip" 2>/dev/null | wc -l)

if [ $CSV_COUNT -eq 0 ] && [ $ZIP_COUNT -eq 0 ]; then
    echo -e "${RED}✗ No CSV or ZIP files found in $DATA_DIR${NC}"
    echo "Expected one of:"
    echo "  - Individual CSV files: aisdk-2021-12-*.csv"
    echo "  - ZIP archive: *.zip"
    exit 1
fi

if [ $CSV_COUNT -gt 0 ]; then
    echo -e "${GREEN}✓ Found $CSV_COUNT CSV file(s)${NC}"
elif [ $ZIP_COUNT -gt 0 ]; then
    echo -e "${GREEN}✓ Found $ZIP_COUNT ZIP file(s) (will be extracted)${NC}"
fi

# Print configuration
echo -e "\nConfiguration:"
echo "  Data directory: $DATA_DIR"
echo "  Output directory: $OUTPUT_DIR"
echo "  Log level: ${LOG_LEVEL:-INFO}"
echo "  Debug mode: ${DEBUG:-false}"
echo ""

# Run Python application
echo "Starting pipeline..."
echo -e "${YELLOW}───────────────────────────────────────────────────────────────────────${NC}\n"

cd /app
exec python -u app/main.py "$@"
