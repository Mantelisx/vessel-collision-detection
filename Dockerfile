# Multi-stage Dockerfile for AIS Collision Detection Pipeline
# Optimized for size and performance

# ─── BUILD STAGE ────────────────────────────────────────────────────────
FROM python:3.11 as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Build Python wheels for faster installation
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /build/wheels \
    -r requirements.txt


# ─── RUNTIME STAGE ──────────────────────────────────────────────────────
FROM python:3.11

LABEL maintainer="Examination Submission"
LABEL description="AIS Vessel Collision Detection Pipeline"
LABEL version="1.0.0"

WORKDIR /app

# Install runtime dependencies (including Java for Spark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Copy wheels from builder
COPY --from=builder /build/wheels /wheels
COPY --from=builder /build/requirements.txt .

# Install Python packages from wheels
RUN pip install --no-cache /wheels/* && rm -rf /wheels

# Create data and output directories
RUN mkdir -p /data /app/output && chmod 755 /data /app/output

# Copy application code
COPY app/ /app/app/
COPY scripts/entrypoint.sh /app/entrypoint.sh

# Create data and output directories (no CSV files included)
RUN mkdir -p /app/aisdk-2021-12

# Ensure entrypoint is executable
RUN chmod +x /app/entrypoint.sh

# Create non-root user
RUN useradd -m -u 1000 spark && chown -R spark:spark /app /data
USER spark

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV LOG_LEVEL=INFO
ENV DEBUG=false

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["python", "-u", "/app/app/main.py"]
