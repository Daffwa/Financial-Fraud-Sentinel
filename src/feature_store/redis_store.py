"""Redis In-Memory Dynamic Feature Store for Sub-2ms Customer Context Lookup."""

import json
import logging
import time
from typing import Dict, Any, Optional
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RedisFeatureStore")


class RedisFeatureStore:
    """Enterprise dynamic feature store managing transaction velocity and customer profiles."""

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        try:
            self.r.ping()
            logger.info(f"Connected to Redis Feature Store at {host}:{port}")
        except Exception as e:
            logger.error(f"Could not connect to Redis: {e}")
            raise

    def seed_initial_baselines(self):
        """Seed initial customer baselines and trusted devices for simulation."""
        baselines = {
            "ACC-ID-8821901": {
                "owner": "Budi Santoso",
                "bank": "BCA",
                "daily_avg": 2500000.0,
                "trusted_device": "DEV-SAMSUNG-S23-01",
                "home_city": "Jakarta",
                "mule_score": 0.05
            },
            "ACC-ID-7712034": {
                "owner": "Siti Rahmawati",
                "bank": "Mandiri",
                "daily_avg": 750000.0,
                "trusted_device": "DEV-IPHONE15-02",
                "home_city": "Bandung",
                "mule_score": 0.02
            },
            "ACC-ID-6634129": {
                "owner": "Ahmad Hidayat",
                "bank": "BRI",
                "daily_avg": 500000.0,
                "trusted_device": "DEV-XIAOMI-13-03",
                "home_city": "Surabaya",
                "mule_score": 0.08
            },
            "ACC-ID-5541982": {
                "owner": "Dewi Lestari",
                "bank": "BNI",
                "daily_avg": 5000000.0,
                "trusted_device": "DEV-IPHONE14-04",
                "home_city": "Medan",
                "mule_score": 0.03
            },
            "ACC-ID-4429810": {
                "owner": "Eko Prasetyo",
                "bank": "BCA",
                "daily_avg": 350000.0,
                "trusted_device": "DEV-OPPO-RENO-05",
                "home_city": "Yogyakarta",
                "mule_score": 0.10
            },
            "ACC-ID-3318721": {
                "owner": "Rian Kurniawan",
                "bank": "GoPay",
                "daily_avg": 100000.0,
                "trusted_device": "DEV-UNKNOWN-06",
                "home_city": "Semarang",
                "mule_score": 0.85  # Known mule profile!
            }
        }

        for acc_id, profile in baselines.items():
            self.r.set(f"profile:{acc_id}", json.dumps(profile))
            self.r.sadd("watchlist:known_accounts", acc_id)

        # Seed high-risk mule target accounts
        self.r.sadd("watchlist:mule_accounts", "ACC-ID-3318721")
        logger.info(f"Seeded {len(baselines)} customer baselines into Redis Feature Store.")

    def get_customer_context(self, account_id: str, device_fingerprint: str, amount: float) -> Dict[str, Any]:
        """Fetch real-time profile and velocity metrics in < 2ms."""
        # 1. Fetch static profile
        raw_profile = self.r.get(f"profile:{account_id}")
        profile = json.loads(raw_profile) if raw_profile else {
            "owner": "Unknown", "bank": "Unknown", "daily_avg": 500000.0,
            "trusted_device": "NONE", "home_city": "Unknown", "mule_score": 0.1
        }

        # 2. Velocity metrics (sliding 1 hour)
        vel_count_key = f"vel:1h:count:{account_id}"
        vel_sum_key = f"vel:1h:sum:{account_id}"

        hourly_count = int(self.r.get(vel_count_key) or 0)
        hourly_sum = float(self.r.get(vel_sum_key) or 0.0)

        # 3. Anomaly flags
        is_device_mismatch = (device_fingerprint != profile.get("trusted_device", ""))
        is_frozen = bool(self.r.sismember("blacklist:frozen_accounts", account_id))
        is_mule_candidate = bool(self.r.sismember("watchlist:mule_accounts", account_id))
        amount_ratio = amount / max(profile.get("daily_avg", 500000.0), 1.0)

        return {
            "account_id": account_id,
            "owner": profile.get("owner"),
            "bank": profile.get("bank"),
            "daily_avg": profile.get("daily_avg"),
            "trusted_device": profile.get("trusted_device"),
            "home_city": profile.get("home_city"),
            "mule_score": profile.get("mule_score", 0.0),
            "hourly_count": hourly_count,
            "hourly_sum": hourly_sum,
            "is_device_mismatch": is_device_mismatch,
            "is_frozen": is_frozen,
            "is_mule_candidate": is_mule_candidate,
            "amount_ratio": round(amount_ratio, 2)
        }

    def record_transaction(self, account_id: str, amount: float):
        """Update sliding window transaction velocity with 3600s TTL."""
        pipe = self.r.pipeline()
        vel_count_key = f"vel:1h:count:{account_id}"
        vel_sum_key = f"vel:1h:sum:{account_id}"

        pipe.incr(vel_count_key)
        pipe.expire(vel_count_key, 3600)
        pipe.incrbyfloat(vel_sum_key, amount)
        pipe.expire(vel_sum_key, 3600)
        pipe.execute()

    def is_account_frozen(self, account_id: str) -> bool:
        return bool(self.r.sismember("blacklist:frozen_accounts", account_id))
