"""Enterprise Real-Time Financial Fraud & AML Stream Processor.

Kafka consumer pipeline integrating Redis dynamic feature store, Laya System 1
AI engine, Core Banking autonomous enforcement, and PostgreSQL hot storage.
"""

import json
import logging
import os
import sys
import time
from typing import Dict, Any, List
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
from psycopg2.extras import execute_values

from src.feature_store.redis_store import RedisFeatureStore
from src.engine.laya_sentinel import LayaSentinelEngine
from src.simulation.core_banking_api import CoreBankingService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FraudStreamProcessor")

KAFKA_BOOTSTRAP = "localhost:9094"
KAFKA_TOPIC = "financial.transactions.raw"
PG_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "user": "sentinel_user",
    "password": "sentinel_secret",
    "dbname": "financial_fraud_db"
}


class FraudStreamProcessor:
    """Enterprise streaming processor executing in-line fraud telematics."""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP, pg_config: Dict = PG_CONFIG):
        self.bootstrap = bootstrap_servers
        self.pg_config = pg_config
        self.pg_conn = None
        self.redis_store = None
        self.ai_engine = None
        self.cbs_api = None
        self._init_components()

    def _init_components(self):
        """Initialize database, Redis, AI model, and Core Banking services."""
        # 1. Connect PostgreSQL
        for attempt in range(10):
            try:
                self.pg_conn = psycopg2.connect(**self.pg_config)
                self.pg_conn.autocommit = False
                logger.info(f"Connected to PostgreSQL at {self.pg_config['host']}:{self.pg_config['port']}")
                break
            except Exception as e:
                logger.warning(f"PostgreSQL not ready (attempt {attempt+1}/10): {e}. Retrying in 2s...")
                time.sleep(2)
        if not self.pg_conn:
            raise RuntimeError("Could not connect to PostgreSQL.")

        # 2. Connect Redis Feature Store
        self.redis_store = RedisFeatureStore(host="localhost", port=6379)
        self.redis_store.seed_initial_baselines()

        # 3. Core Banking Service
        self.cbs_api = CoreBankingService(pg_conn_pool=self.pg_conn, redis_client=self.redis_store.r)

        # 4. Laya AI Decision Engine
        self.ai_engine = LayaSentinelEngine(use_gpu=True)
        logger.info("All Sentinel Processor components successfully initialized.")

    def run(self):
        """Consume transactions from Kafka and process them in-line."""
        consumer = None
        group_id = f"sentinel-processor-{int(time.time())}"

        for attempt in range(10):
            try:
                consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=self.bootstrap,
                    group_id=group_id,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=1000
                )
                logger.info(f"Kafka consumer subscribed to '{KAFKA_TOPIC}' with group '{group_id}'")
                break
            except (KafkaError, Exception) as e:
                logger.warning(f"Waiting for Kafka broker at {self.bootstrap} (attempt {attempt+1}/10): {e}...")
                time.sleep(2)

        if not consumer:
            raise RuntimeError("Failed to connect Kafka consumer.")

        processed_count = 0
        fraud_count = 0
        total_latency = 0.0

        logger.info("🚀 Fraud Stream Processor is actively processing live transactions...")
        try:
            while True:
                records = consumer.poll(timeout_ms=1000)
                if not records:
                    continue

                for tp, messages in records.items():
                    for message in messages:
                        tx = message.value
                        t0 = time.time()

                        # 1. Fast-path check: Is account already frozen?
                        if self.redis_store.is_account_frozen(tx["account_from"]):
                            logger.warning(f"🚫 [BLOCKED] Transaction from frozen account {tx['account_from']} dropped.")
                            self._save_transaction(tx, status="BLOCKED")
                            continue

                        # 2. Redis Feature Store Lookup (< 2ms)
                        context = self.redis_store.get_customer_context(
                            tx["account_from"], tx["device_fingerprint"], tx["amount"]
                        )

                        # 3. Laya System 1 Inference (~30ms)
                        decision = self.ai_engine.evaluate_transaction(tx, context)
                        proc_latency = (time.time() - t0) * 1000

                        processed_count += 1
                        total_latency += proc_latency

                        # 4. Autonomous Enforcement
                        if decision["action_status"] == "AUTO_FROZEN":
                            fraud_count += 1
                            reason_msg = (
                                f"{decision['fraud_pattern'].upper()} detected with "
                                f"{decision['confidence']*100:.1f}% confidence. "
                                f"Amount Rp {tx['amount']:,.0f} (Ratio {context['amount_ratio']}x daily avg)"
                            )
                            self.cbs_api.freeze_account(
                                account_id=tx["account_from"],
                                transaction_id=tx["transaction_id"],
                                reason=reason_msg,
                                confidence=decision["confidence"],
                                amount=tx["amount"]
                            )
                            self._save_transaction(tx, status="FLAGGED")
                            self._save_alert(tx, decision)

                        elif decision["action_status"] == "FLAGGED_INVESTIGATION":
                            fraud_count += 1
                            self.cbs_api.issue_biometric_challenge(tx["account_from"], tx["transaction_id"])
                            self._save_transaction(tx, status="FLAGGED")
                            self._save_alert(tx, decision)

                        else:
                            # Legitimate transaction
                            self._save_transaction(tx, status="PROCESSED")

                        # 5. Record velocity in Redis
                        self.redis_store.record_transaction(tx["account_from"], tx["amount"])

                        # 6. Periodic console status
                        if processed_count % 10 == 0:
                            avg_lat = total_latency / processed_count
                            logger.info(
                                f"⚡ [TELEMETRY] Processed: {processed_count} txs | Fraud Blocked: {fraud_count} | "
                                f"Avg Latency: {avg_lat:.2f}ms | Latest: {tx['transaction_id']} "
                                f"({decision['fraud_pattern']} - {decision['action_status']})"
                            )

        except KeyboardInterrupt:
            logger.info("Stream processor stopped by user.")
        finally:
            if consumer:
                consumer.close()
            if self.pg_conn:
                self.pg_conn.close()
            logger.info("Stream processor shut down cleanly.")

    def _save_transaction(self, tx: Dict, status: str):
        """Save transaction to PostgreSQL."""
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO transactions 
                    (transaction_id, timestamp, account_from, account_to, bank_from, bank_to, 
                     amount, channel, device_fingerprint, ip_address, location_city, latitude, 
                     longitude, transfer_note, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING;
                    """,
                    (
                        tx["transaction_id"], tx["timestamp"], tx["account_from"], tx["account_to"],
                        tx["bank_from"], tx["bank_to"], tx["amount"], tx["channel"],
                        tx.get("device_fingerprint"), tx.get("ip_address"), tx.get("location_city"),
                        tx.get("latitude"), tx.get("longitude"), tx.get("transfer_note"), status
                    )
                )
                self.pg_conn.commit()
        except Exception as e:
            logger.error(f"Failed to save transaction: {e}")
            self.pg_conn.rollback()

    def _save_alert(self, tx: Dict, decision: Dict):
        """Save fraud alert to PostgreSQL."""
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fraud_alerts 
                    (transaction_id, timestamp, fraud_pattern, risk_score, confidence, 
                     immediate_block_trigger, decision_latency_ms, account_from, account_to, 
                     amount, channel, location_city, action_status)
                    VALUES (%s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        tx["transaction_id"], decision["fraud_pattern"], decision["risk_score"],
                        decision["confidence"], decision["immediate_block_trigger"],
                        decision["decision_latency_ms"], tx["account_from"], tx["account_to"],
                        tx["amount"], tx["channel"], tx.get("location_city"), decision["action_status"]
                    )
                )
                self.pg_conn.commit()
        except Exception as e:
            logger.error(f"Failed to save alert: {e}")
            self.pg_conn.rollback()


if __name__ == "__main__":
    processor = FraudStreamProcessor()
    processor.run()
