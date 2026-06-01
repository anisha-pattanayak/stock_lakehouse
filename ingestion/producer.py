"""
Kafka Producer
Continuously fetches stock quotes and publishes to Kafka topics.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from stock_client import get_stock_client, StockQuote

# ─────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("stock-producer")


# ─────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "raw-stock-events")
SYMBOLS = os.getenv("STOCK_SYMBOLS", "AAPL,MSFT,GOOGL,TSLA,AMZN").split(",")
INTERVAL = int(os.getenv("INGESTION_INTERVAL_SECONDS", "10"))


# ─────────────────────────────────────────────────────────
# Producer helpers
# ─────────────────────────────────────────────────────────
def delivery_report(err, msg) -> None:
    """Callback fired after each message is delivered (or fails)."""
    if err is not None:
        logger.error("Delivery failed for %s: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered %s → topic=%s partition=%d offset=%d",
            msg.key(),
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def build_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "compression.type": "snappy",
        "enable.idempotence": True,
    }
    return Producer(conf)


def ensure_topics(bootstrap: str, topics: list[str]) -> None:
    """Create Kafka topics if they don't already exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics.keys()
    new_topics = [
        NewTopic(t, num_partitions=3, replication_factor=1)
        for t in topics if t not in existing
    ]
    if new_topics:
        fs = admin.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                logger.info("Created topic: %s", topic)
            except KafkaException as exc:
                logger.warning("Topic %s already exists or error: %s", topic, exc)


# ─────────────────────────────────────────────────────────
# Main producer loop
# ─────────────────────────────────────────────────────────
class StockProducer:
    def __init__(self) -> None:
        self.client = get_stock_client()
        self.producer = build_producer()
        self._running = True

        # Graceful shutdown on SIGINT / SIGTERM
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_) -> None:
        logger.info("Shutdown signal received. Flushing and exiting...")
        self._running = False

    def publish(self, quote: StockQuote) -> None:
        payload = json.dumps(quote.to_dict()).encode("utf-8")
        self.producer.produce(
            topic=TOPIC_RAW,
            key=quote.symbol.encode("utf-8"),
            value=payload,
            on_delivery=delivery_report,
        )
        self.producer.poll(0)  # Trigger delivery callbacks

    def run(self) -> None:
        logger.info(
            "Starting ingestion | provider=%s | symbols=%s | interval=%ds",
            os.getenv("API_PROVIDER", "yahoo_finance"),
            SYMBOLS,
            INTERVAL,
        )
        ensure_topics(KAFKA_BOOTSTRAP, [TOPIC_RAW])

        cycle = 0
        while self._running:
            cycle += 1
            logger.info("── Ingestion cycle #%d ──", cycle)
            start = time.monotonic()

            for symbol in SYMBOLS:
                if not self._running:
                    break
                quote = self.client.get_quote(symbol)
                if quote:
                    self.publish(quote)
                    logger.info(
                        "Published %s | price=%.2f | chg=%.2f%%",
                        quote.symbol,
                        quote.price,
                        quote.change_pct,
                    )
                else:
                    logger.warning("No quote returned for %s", symbol)

            # Flush all pending messages
            self.producer.flush(timeout=10)

            elapsed = time.monotonic() - start
            sleep_time = max(0, INTERVAL - elapsed)
            logger.info(
                "Cycle #%d done in %.1fs. Sleeping %.1fs...",
                cycle, elapsed, sleep_time,
            )
            time.sleep(sleep_time)

        self.producer.flush(timeout=30)
        logger.info("Producer shut down cleanly.")


# ─────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    StockProducer().run()
