FROM bitnami/spark:3.4.1

LABEL maintainer="Stock Lakehouse Platform"
LABEL service="spark-processing"

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    delta-spark==2.4.0 \
    confluent-kafka==2.3.0 \
    pandas==2.1.0 \
    pyarrow==13.0.0 \
    python-dotenv==1.0.0

WORKDIR /app
COPY . .

USER 1001
