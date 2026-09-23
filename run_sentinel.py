"""Unified Orchestrator: Enterprise Real-Time Financial Fraud & AML Sentinel.

Launches the streaming ingestion gateway, Redis feature store sync,
Laya AI System 1 decision engine, and automated enforcement in a single command.
"""

import logging
import os
import signal
import sys
import threading
import time

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Ensure project root is in python path
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.ingestion.kafka_producer import stream_transactions
from src.processing.fraud_stream_processor import FraudStreamProcessor
from src.lakehouse.parquet_archiver import archive_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("SentinelOrchestrator")


def run_producer_thread(stop_event):
    """Background worker continuously feeding live transactions to Kafka."""
    logger.info("Initializing Financial Transaction Stream Generator (2 tx/s)...")
    try:
        from src.ingestion.transaction_generator import generate_transaction
        from src.ingestion.kafka_producer import create_producer, DEFAULT_TOPIC
        producer = create_producer()
        while not stop_event.is_set():
            tx = generate_transaction()
            producer.send(DEFAULT_TOPIC, key=tx["account_from"].encode("utf-8"), value=tx)
            time.sleep(0.5)
    except Exception as e:
        logger.error(f"Producer thread encountered an error: {e}")


def run_processor_thread(stop_event):
    """Background worker consuming from Kafka, querying Redis & Laya, enforcing freezes."""
    logger.info("Initializing Fraud Stream Processor & Laya AI Engine...")
    try:
        processor = FraudStreamProcessor()
        processor.run()
    except Exception as e:
        logger.error(f"Stream processor thread encountered an error: {e}")


def run_archiver_loop(stop_event):
    """Periodic cold-storage archiver running every 60 seconds."""
    logger.info("Cold Lakehouse Archiver active (period: 60s)...")
    while not stop_event.is_set():
        time.sleep(60)
        try:
            stats = archive_data()
            if stats["transactions_archived"] > 0:
                logger.info(f"🏛️ [LAKEHOUSE] Periodic archive cycle complete: {stats}")
        except Exception as e:
            logger.error(f"Archiver loop error: {e}")


def main():
    print("""
============================================================================
🛡️  ENTERPRISE REAL-TIME FINANCIAL FRAUD & AML SENTINEL (LAYA AI) 🛡️
============================================================================
Infrastructure:
  • Apache Kafka (localhost:9094)
  • Redis Feature Store (localhost:6379)
  • PostgreSQL Timescale (localhost:5433 / financial_fraud_db)
  • Laya System 1 Decision Engine (~30ms Non-Autoregressive Inference)
  • Apache Parquet Lakehouse (data/lakehouse/transactions/)
============================================================================
    """)

    stop_event = threading.Event()

    # 1. Start Stream Processor (Consumer)
    proc_thread = threading.Thread(target=run_processor_thread, args=(stop_event,), daemon=True)
    proc_thread.start()

    time.sleep(3)  # Let consumer establish partition assignments

    # 2. Start Transaction Producer
    prod_thread = threading.Thread(target=run_producer_thread, args=(stop_event,), daemon=True)
    prod_thread.start()

    # 3. Start Lakehouse Archiver
    arch_thread = threading.Thread(target=run_archiver_loop, args=(stop_event,), daemon=True)
    arch_thread.start()

    logger.info("🚀 All Sentinel sub-systems are operating live. Press Ctrl+C to terminate.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\n🛑 Shutdown signal received. Stopping Sentinel pipeline...")
        stop_event.set()
        time.sleep(2)
        logger.info("Sentinel pipeline stopped gracefully.")


if __name__ == "__main__":
    main()
