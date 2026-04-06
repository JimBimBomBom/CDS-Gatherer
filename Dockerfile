# ── Build stage ────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Copy and install the application
COPY cityfetch/ ./cityfetch/
COPY setup.py .
COPY README.md .
RUN pip install --no-cache-dir --prefix=/install .

# ── Runtime stage ───────────────────────────────────────────────────────────────
FROM python:3.12-slim

# Create non-root user for security
RUN addgroup --system cityfetch && adduser --system --ingroup cityfetch cityfetch

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Create data directory and set permissions
RUN mkdir -p /data && chown cityfetch:cityfetch /data

# Switch to non-root user
USER cityfetch

# Environment variables with defaults
ENV LANGUAGES=en \
    OUTPUT_FORMAT=sql \
    OUTPUT_DIR=/data \
    SCHEDULE=7DAYS \
    WEBHOOK_URL="" \
    WEBHOOK_SECRET="" \
    BATCH_SIZE=1000 \
    MAX_PAGES=40 \
    PAGE_SIZE=500 \
    VERBOSE=false

# Set the entrypoint
ENTRYPOINT ["docker-entrypoint.sh"]

# Default command (shows help if no schedule set)
CMD ["--help"]
