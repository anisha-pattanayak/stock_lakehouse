# Architecture Overview

## System Design

```
Stock Market APIs (Alpha Vantage / Finnhub / Yahoo Finance / Polygon.io)
        │
        ▼
┌─────────────────┐
│ Kafka Producer  │  ingestion/producer.py
│ (Python)        │  Fetches quotes every 10s, retries on failure
└────────┬────────┘
         │ raw-stock-events (topic)
         ▼
┌─────────────────┐
│  Apache Kafka   │  Distributed event streaming
│  + Zookeeper    │  3 partitions per topic
└────────┬────────┘
         │
         ▼
┌──────────────────────────────────────┐
│   PySpark Structured Streaming       │  spark-processing/streaming_pipeline.py
│                                      │
│  ┌──────────┐  ┌──────────┐  ┌────────────┐
│  │  Bronze  │→ │  Silver  │→ │    Gold    │
│  │  (Raw)   │  │ (Cleaned)│  │ (Aggregated│
│  └──────────┘  └──────────┘  └────────────┘
└──────────────────────┬───────────────┘
                       │ Delta Lake
                       ▼
┌─────────────────┐  ┌─────────────────┐
│    FastAPI      │  │   PostgreSQL    │
│  REST Layer     │  │  Metadata Store │
└────────┬────────┘  └─────────────────┘
         │
         ▼
┌─────────────────┐  ┌─────────────────┐
│   Streamlit     │  │     Grafana     │
│   Dashboard     │  │   Dashboards    │
└─────────────────┘  └─────────────────┘

┌─────────────────────────────────────┐
│         Apache Airflow              │
│  DAG1: Ingestion health check       │
│  DAG2: dbt transformations          │
│  DAG3: Data quality checks          │
│  DAG4: Warehouse load               │
└─────────────────────────────────────┘
```

## Data Flow

1. **Ingestion** — `producer.py` polls stock APIs and pushes JSON to `raw-stock-events` Kafka topic
2. **Bronze** — Spark writes raw Kafka records as-is to Delta Lake (immutable, partitioned by date)
3. **Silver** — Spark parses JSON, validates, deduplicates, enriches (adds change%, price range, event_time)
4. **Gold** — Spark computes 1-min OHLCV windows, 5-min moving averages, and 10-min volatility metrics
5. **API** — FastAPI reads from Silver/Gold layers and serves REST endpoints
6. **Dashboard** — Streamlit polls the API every 10s and renders charts

## Medallion Layers

| Layer | Format | Partitioning | Retention |
|---|---|---|---|
| Bronze | Delta Lake | load_date | 90 days |
| Silver | Delta Lake | load_date, symbol | 30 days |
| Gold/OHLCV | Delta Lake | window_start | 30 days |
| Gold/MA | Delta Lake | window_start | 30 days |
| Gold/Volatility | Delta Lake | window_start | 30 days |
