# CDS-CityFetch MVP
# Minimal Docker image for fetching Wikidata city information
# Runs once and exits - no scheduling, no CLI framework

FROM python:3.12-slim

WORKDIR /app

# Install only runtime dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY cityfetch/ ./cityfetch/

# Create data directory
RUN mkdir -p /data

# Environment configuration
ENV OUTPUT_DIR=/data
ENV PYTHONUNBUFFERED=1

# Set entrypoint to pass all args to main.py
ENTRYPOINT ["python", "main.py"]
CMD []  # Default: no args, will show help if no -v provided
