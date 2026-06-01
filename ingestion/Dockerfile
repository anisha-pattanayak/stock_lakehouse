FROM python:3.11-slim

LABEL maintainer="Stock Lakehouse Platform"
LABEL service="stock-ingestion"

WORKDIR /app

# Install system dependencies (librdkafka for confluent-kafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Health check: ensure producer process is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD pgrep -f producer.py || exit 1

CMD ["python", "producer.py"]
