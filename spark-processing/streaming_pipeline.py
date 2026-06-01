"""
PySpark Structured Streaming Pipeline
Kafka → Bronze → Silver → Gold (Delta Lake)

Architecture:
  raw-stock-events (Kafka)
       ↓
  Bronze Layer  – raw immutable JSON
       ↓
  Silver Layer  – cleaned, validated, typed
       ↓
  Gold Layer    – aggregated windows, moving averages, volatility
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
)

# ─────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("spark-streaming")

# ─────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "raw-stock-events")
TOPIC_ANALYTICS = os.getenv("KAFKA_TOPIC_ANALYTICS", "analytics-events")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/lakehouse/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "/data/lakehouse/silver")
GOLD_PATH = os.getenv("GOLD_PATH", "/data/lakehouse/gold")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")

# ─────────────────────────────────────────────────────────
# Schema — must match ingestion/stock_client.py StockQuote
# ─────────────────────────────────────────────────────────
STOCK_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("prev_close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("change", DoubleType(), True),
    StructField("change_pct", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("ingested_at", StringType(), True),
])


# ─────────────────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StockLakehouseStreaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────
# LAYER 1 — Bronze (Raw immutable landing)
# ─────────────────────────────────────────────────────────
def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read raw Kafka events as a streaming DataFrame."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )


def write_bronze(raw_df: DataFrame):  # returns StreamingQuery
    """Write raw messages to Bronze Delta table (append-only)."""
    bronze_df = raw_df.select(
        F.col("key").cast(StringType()).alias("kafka_key"),
        F.col("value").cast(StringType()).alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("bronze_loaded_at"),
        # Partition by date for efficient pruning
        F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("load_date"),
    )

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .partitionBy("load_date")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/bronze")
        .option("path", f"{BRONZE_PATH}/stock_events")
        .trigger(processingTime="10 seconds")
        .start()
    )
    logger.info("Bronze streaming query started: %s", query.id)
    return query


# ─────────────────────────────────────────────────────────
# LAYER 2 — Silver (Parsed, validated, enriched)
# ─────────────────────────────────────────────────────────
def parse_and_validate(raw_df: DataFrame) -> DataFrame:
    """Parse JSON payload and apply data quality rules."""
    parsed = raw_df.select(
        F.from_json(
            F.col("value").cast(StringType()),
            STOCK_SCHEMA,
        ).alias("data"),
        F.col("timestamp").alias("kafka_ts"),
    ).select("data.*", "kafka_ts")

    # --- Data Quality Filters ---
    silver_df = (
        parsed
        # Drop nulls in critical fields
        .filter(F.col("symbol").isNotNull())
        .filter(F.col("price").isNotNull())
        # Validate ranges
        .filter(F.col("price") > 0)
        .filter(F.col("volume") >= 0)
        .filter(F.col("high") >= F.col("low"))
        # Bug fix: parse event_time BEFORE dropDuplicates so watermark column exists
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("ingested_at")),
        )
        # Watermark must be declared before dropDuplicates in streaming to bound state
        .withWatermark("event_time", "5 minutes")
        # Deduplicate within watermark window by symbol+ingested_at
        .dropDuplicates(["symbol", "ingested_at"])
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn(
            "is_positive_day",
            F.when(F.col("change_pct") > 0, True).otherwise(False),
        )
        .withColumn(
            "price_range",
            F.round(F.col("high") - F.col("low"), 4),
        )
        .withColumn(
            "price_range_pct",
            F.round(
                (F.col("high") - F.col("low")) / F.col("prev_close") * 100,
                4,
            ),
        )
        .withColumn(
            "load_date",
            F.date_format(F.col("event_time"), "yyyy-MM-dd"),
        )
    )
    return silver_df


def write_silver(raw_df: DataFrame):  # returns StreamingQuery
    """Write Silver layer."""
    silver_df = parse_and_validate(raw_df)

    query = (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .partitionBy("load_date", "symbol")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/silver")
        .option("path", f"{SILVER_PATH}/stock_quotes")
        .trigger(processingTime="15 seconds")
        .start()
    )
    logger.info("Silver streaming query started: %s", query.id)
    return query


# ─────────────────────────────────────────────────────────
# LAYER 3 — Gold (Aggregated analytics)
# ─────────────────────────────────────────────────────────
def build_gold_aggregations(raw_df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Three Gold tables:
      1. 1-minute OHLCV windows
      2. Moving averages (5-min, 10-min)
      3. Volatility metrics
    """
    silver_df = parse_and_validate(raw_df)

    # ── Gold 1: 1-minute OHLCV windows ──
    ohlcv_1m = (
        silver_df
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "1 minute").alias("window"),
            F.col("symbol"),
        )
        .agg(
            F.first("open").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.last("price").alias("close"),
            F.sum("volume").alias("total_volume"),
            F.avg("price").alias("avg_price"),
            F.count("*").alias("tick_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol", "open", "high", "low", "close",
            "total_volume", "avg_price", "tick_count",
            F.current_timestamp().alias("gold_processed_at"),
        )
    )

    # ── Gold 2: 5-minute moving average windows ──
    moving_avg_5m = (
        silver_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window("event_time", "5 minutes", "1 minute").alias("window"),
            F.col("symbol"),
        )
        .agg(
            F.avg("price").alias("ma_5min"),
            F.stddev("price").alias("stddev_5min"),
            F.min("price").alias("min_5min"),
            F.max("price").alias("max_5min"),
            F.count("*").alias("sample_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            F.round("ma_5min", 4).alias("ma_5min"),
            F.round("stddev_5min", 6).alias("stddev_5min"),
            F.round("min_5min", 4).alias("min_5min"),
            F.round("max_5min", 4).alias("max_5min"),
            "sample_count",
        )
    )

    # ── Gold 3: Volatility index per symbol ──
    volatility = (
        silver_df
        .withWatermark("event_time", "15 minutes")
        .groupBy(
            F.window("event_time", "10 minutes", "1 minute").alias("window"),
            F.col("symbol"),
        )
        .agg(
            F.stddev("price").alias("price_volatility"),
            F.avg("price_range_pct").alias("avg_range_pct"),
            F.max("change_pct").alias("max_gain_pct"),
            F.min("change_pct").alias("max_loss_pct"),
            F.sum(F.when(F.col("is_positive_day"), 1).otherwise(0)).alias("up_ticks"),
            F.count("*").alias("total_ticks"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            F.round("price_volatility", 6).alias("price_volatility"),
            F.round("avg_range_pct", 4).alias("avg_range_pct"),
            F.round("max_gain_pct", 4).alias("max_gain_pct"),
            F.round("max_loss_pct", 4).alias("max_loss_pct"),
            "up_ticks",
            "total_ticks",
            F.round(F.col("up_ticks") / F.col("total_ticks") * 100, 2)
            .alias("bullish_pct"),
        )
    )

    return ohlcv_1m, moving_avg_5m, volatility


def write_gold(raw_df: DataFrame) -> list:
    """Write all three Gold tables."""
    ohlcv, ma, vol = build_gold_aggregations(raw_df)
    queries = []

    for name, df in [("ohlcv_1m", ohlcv), ("moving_avg_5m", ma), ("volatility", vol)]:
        q = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/gold/{name}")
            .option("path", f"{GOLD_PATH}/{name}")
            .trigger(processingTime="30 seconds")
            .start()
        )
        logger.info("Gold/%s streaming query started: %s", name, q.id)
        queries.append(q)

    return queries


# ─────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────
def main() -> None:
    logger.info("Initializing Spark Structured Streaming pipeline...")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading from Kafka topic: %s", TOPIC_RAW)
    raw_df = read_kafka_stream(spark)

    logger.info("Starting Bronze writer...")
    bronze_q = write_bronze(raw_df)

    logger.info("Starting Silver writer...")
    silver_q = write_silver(raw_df)

    logger.info("Starting Gold writers...")
    gold_qs = write_gold(raw_df)

    all_queries = [bronze_q, silver_q] + gold_qs
    logger.info("All %d streaming queries running. Awaiting termination...", len(all_queries))

    # Block until all queries finish (or error)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
