# ── Build stage ────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Runtime stage ───────────────────────────────────────────────────────────────
FROM python:3.12-slim

# Non-root user for least-privilege operation
RUN addgroup --system gatherer && adduser --system --ingroup gatherer gatherer

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY src/ ./src/

# Data directory (SQL output); override via DATA_DIR env var if needed
RUN mkdir -p /app/data && chown gatherer:gatherer /app/data

USER gatherer

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

CMD ["python", "/app/src/scheduler.py"]
