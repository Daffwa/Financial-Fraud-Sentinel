"""Cold Data Lakehouse Archiver (Snappy Apache Parquet).

Archives transactions and fraud audit trails into partitioned Apache Parquet files
(year=YYYY/month=MM/day=DD/) for regulatory compliance (OJK/BI audit trail).
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ParquetArchiver")

BASE_LAKEHOUSE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "lakehouse")
PG_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "user": "sentinel_user",
    "password": "sentinel_secret",
    "dbname": "financial_fraud_db"
}


def archive_data(base_dir: str = BASE_LAKEHOUSE_DIR, pg_config: Dict = PG_CONFIG) -> Dict[str, int]:
    """Query recent transactions and alerts and export partitioned Snappy Parquet files."""
    now = datetime.now(timezone.utc)
    year_str = now.strftime("%Y")
    month_str = now.strftime("%m")
    day_str = now.strftime("%d")
    timestamp_suffix = now.strftime("%Y%m%d_%H%M%S")

    conn = psycopg2.connect(**pg_config)
    stats = {"transactions_archived": 0, "alerts_archived": 0}

    try:
        # 1. Archive Transactions
        tx_query = "SELECT * FROM transactions ORDER BY timestamp DESC LIMIT 5000;"
        tx_df = pd.read_sql(tx_query, conn)

        if not tx_df.empty:
            tx_dir = os.path.join(base_dir, "transactions", f"year={year_str}", f"month={month_str}", f"day={day_str}")
            os.makedirs(tx_dir, exist_ok=True)
            tx_file = os.path.join(tx_dir, f"transactions_{timestamp_suffix}.parquet")

            # Convert to PyArrow Table and write Snappy Parquet
            table = pa.Table.from_pandas(tx_df)
            pq.write_table(table, tx_file, compression="snappy")
            stats["transactions_archived"] = len(tx_df)
            logger.info(f"📦 Archived {len(tx_df)} transactions to: {tx_file}")

        # 2. Archive Fraud Alerts
        alert_query = "SELECT * FROM fraud_alerts ORDER BY timestamp DESC LIMIT 5000;"
        alert_df = pd.read_sql(alert_query, conn)

        if not alert_df.empty:
            alert_dir = os.path.join(base_dir, "fraud_alerts", f"year={year_str}", f"month={month_str}", f"day={day_str}")
            os.makedirs(alert_dir, exist_ok=True)
            alert_file = os.path.join(alert_dir, f"alerts_{timestamp_suffix}.parquet")

            table_alert = pa.Table.from_pandas(alert_df)
            pq.write_table(table_alert, alert_file, compression="snappy")
            stats["alerts_archived"] = len(alert_df)
            logger.info(f"🚨 Archived {len(alert_df)} fraud alerts to: {alert_file}")

        # 3. Archive Automated Actions
        action_query = "SELECT * FROM automated_actions ORDER BY timestamp DESC LIMIT 5000;"
        action_df = pd.read_sql(action_query, conn)

        if not action_df.empty:
            action_dir = os.path.join(base_dir, "automated_actions", f"year={year_str}", f"month={month_str}", f"day={day_str}")
            os.makedirs(action_dir, exist_ok=True)
            action_file = os.path.join(action_dir, f"actions_{timestamp_suffix}.parquet")

            table_action = pa.Table.from_pandas(action_df)
            pq.write_table(table_action, action_file, compression="snappy")
            logger.info(f"🛡️ Archived {len(action_df)} automated actions to: {action_file}")

    except Exception as e:
        logger.error(f"Error during parquet archival: {e}")
    finally:
        conn.close()

    return stats


if __name__ == "__main__":
    archive_data()
