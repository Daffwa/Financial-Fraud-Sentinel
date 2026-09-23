"""Core Banking Mock API & Autonomous Enforcement Service.

Simulates enterprise Core Banking System (CBS) integration for automated
account freezes, biometric verification challenges, and fund holds.
"""

import logging
import time
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CoreBankingAPI")


class CoreBankingService:
    """Mock Core Banking System API providing real-time operational enforcement."""

    def __init__(self, pg_conn_pool=None, redis_client=None):
        self.pg_conn = pg_conn_pool
        self.redis = redis_client
        logger.info("CoreBankingService initialized. Ready for autonomous enforcement actions.")

    def freeze_account(
        self,
        account_id: str,
        transaction_id: str,
        reason: str,
        confidence: float,
        amount: float
    ) -> Dict[str, Any]:
        """Execute autonomous emergency account freeze when confidence > 95%."""
        start_time = time.time()
        logger.warning(
            f"🚨 [CORE BANKING FREEZE] Account {account_id} FROZEN | Reason: {reason} | Conf: {confidence:.3f} | Tx: {transaction_id}"
        )

        execution_latency_ms = (time.time() - start_time) * 1000

        # Update Redis dynamic blacklist flag (< 1ms)
        if self.redis:
            try:
                self.redis.sadd("blacklist:frozen_accounts", account_id)
                self.redis.set(f"freeze_reason:{account_id}", reason, ex=86400)
            except Exception as e:
                logger.error(f"Failed to update Redis freeze flag: {e}")

        # Update PostgreSQL account status and record audit log
        if self.pg_conn:
            try:
                with self.pg_conn.cursor() as cur:
                    # Update account status
                    cur.execute(
                        """
                        UPDATE accounts
                        SET is_frozen = TRUE, risk_level = 'SUSPENDED', frozen_reason = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE account_id = %s;
                        """,
                        (reason, account_id)
                    )
                    # Record autonomous action audit
                    cur.execute(
                        """
                        INSERT INTO automated_actions 
                        (transaction_id, target_account, action_type, reason, confidence, execution_time_ms, status)
                        VALUES (%s, %s, 'ACCOUNT_FREEZE', %s, %s, %s, 'EXECUTED');
                        """,
                        (transaction_id, account_id, reason, confidence, execution_latency_ms)
                    )
                    self.pg_conn.commit()
            except Exception as e:
                logger.error(f"Failed to update PostgreSQL freeze status: {e}")
                if self.pg_conn:
                    self.pg_conn.rollback()

        return {
            "status": "SUCCESS",
            "account_id": account_id,
            "action": "ACCOUNT_FREEZE",
            "reason": reason,
            "confidence": confidence,
            "latency_ms": round(execution_latency_ms, 2)
        }

    def issue_biometric_challenge(self, account_id: str, transaction_id: str) -> Dict[str, Any]:
        """Trigger simulated Step-Up 2FA / Biometric verification prompt."""
        logger.info(f"📱 [STEP-UP 2FA] Dispatched Biometric Push Notification to user {account_id} for tx {transaction_id}")
        return {
            "status": "DISPATCHED",
            "account_id": account_id,
            "challenge_type": "BIOMETRIC_FACE_ID_OR_OTP",
            "expires_in_seconds": 120
        }
