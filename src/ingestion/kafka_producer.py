"""Financial Transaction Stream Kafka Producer.

Streams continuous transactions (QRIS, BI-FAST, M-Banking) into Apache Kafka.
"""

import json
import logging
import time
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.ingestion.transaction_generator import generate_transaction

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("KafkaProducer")

DEFAULT_TOPIC = "financial.transactions.raw"
KAFKA_BOOTSTRAP = "localhost:9094"


def create_producer(bootstrap_servers: str = KAFKA_BOOTSTRAP, retries: int = 10) -> KafkaProducer:
    """Connect to Kafka broker with resilient retry loop."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10
            )
            logger.info(f"Connected to Kafka broker at {bootstrap_servers}")
            return producer
        except (KafkaError, Exception) as e:
            logger.warning(f"Kafka broker not ready at {bootstrap_servers} (attempt {attempt}/{retries}): {e}. Retrying in 2s...")
            time.sleep(2)
    raise RuntimeError(f"Could not connect to Kafka at {bootstrap_servers} after {retries} attempts.")


def stream_transactions(rate_per_sec: float = 2.0, max_events: Optional[int] = None):
    """Continuously generate and produce transactions to Kafka."""
    producer = create_producer()
    interval = 1.0 / max(rate_per_sec, 0.1)
    produced = 0

    logger.info(f"Starting transaction streaming to topic '{DEFAULT_TOPIC}' at {rate_per_sec} tx/s...")
    try:
        while True:
            tx = generate_transaction()
            producer.send(DEFAULT_TOPIC, key=tx["account_from"].encode("utf-8"), value=tx)
            produced += 1

            if produced % 20 == 0:
                logger.info(
                    f"📤 Streamed {produced} transactions | Latest: {tx['transaction_id']} "
                    f"({tx['bank_from']}->{tx['bank_to']} Rp {tx['amount']:,.0f} [{tx['intended_pattern']}])"
                )

            if max_events and produced >= max_events:
                logger.info(f"Reached max events limit ({max_events}). Stopping stream.")
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Kafka streaming stopped by user.")
    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed gracefully.")


if __name__ == "__main__":
    stream_transactions(rate_per_sec=2.0)
