# Multi-stage production Dockerfile for Temporal Product Knowledge System
FROM python:3.12-slim as base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/backend

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# Copy application source code
COPY backend /app/backend
COPY sample_data /app/sample_data

# Ensure data directory exists
RUN mkdir -p /app/backend/data /app/backend/data/backups

# Expose FastAPI port
EXPOSE 8000

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Production startup: apply migrations and start uvicorn
CMD ["sh", "-c", "python /app/backend/migrations/runner.py && uvicorn app.main:app --app-dir /app/backend --host 0.0.0.0 --port 8000"]
