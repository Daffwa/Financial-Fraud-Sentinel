"""Realistic Indonesian Financial Transaction Stream Generator.

Generates authentic stream of financial transactions across BCA, Mandiri, BRI,
BNI, GoPay, OVO, ShopeePay, and DANA. Mixes legitimate retail transactions
with sophisticated fraud patterns (Account Takeover, Money Mule Layering, Phishing).
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List

INDONESIAN_CITIES = [
    {"city": "Jakarta Pusat", "lat": -6.1818, "lon": 106.8223},
    {"city": "Jakarta Selatan", "lat": -6.2615, "lon": 106.8106},
    {"city": "Bandung", "lat": -6.9175, "lon": 107.6191},
    {"city": "Surabaya", "lat": -7.2575, "lon": 112.7521},
    {"city": "Semarang", "lat": -6.9932, "lon": 110.4203},
    {"city": "Medan", "lat": 3.5952, "lon": 98.6722},
    {"city": "Yogyakarta", "lat": -7.7956, "lon": 110.3695},
    {"city": "Makassar", "lat": -5.1477, "lon": 119.4327},
    {"city": "Denpasar", "lat": -8.6705, "lon": 115.2126},
    {"city": "Palembang", "lat": -2.9761, "lon": 104.7754}
]

ACCOUNTS = [
    {"id": "ACC-ID-8821901", "name": "Budi Santoso", "bank": "BCA", "device": "DEV-SAMSUNG-S23-01", "city": "Jakarta Selatan"},
    {"id": "ACC-ID-7712034", "name": "Siti Rahmawati", "bank": "Mandiri", "device": "DEV-IPHONE15-02", "city": "Bandung"},
    {"id": "ACC-ID-6634129", "name": "Ahmad Hidayat", "bank": "BRI", "device": "DEV-XIAOMI-13-03", "city": "Surabaya"},
    {"id": "ACC-ID-5541982", "name": "Dewi Lestari", "bank": "BNI", "device": "DEV-IPHONE14-04", "city": "Medan"},
    {"id": "ACC-ID-4429810", "name": "Eko Prasetyo", "bank": "BCA", "device": "DEV-OPPO-RENO-05", "city": "Yogyakarta"}
]

MULE_ACCOUNTS = [
    {"id": "ACC-ID-3318721", "name": "Rian Kurniawan (Mule Candidate)", "bank": "GoPay"},
    {"id": "ACC-ID-2209183", "name": "Doni Iskandar (Suspicious)", "bank": "OVO"},
    {"id": "ACC-ID-1104829", "name": "Samsul Bahri (Unregistered VA)", "bank": "DANA"}
]

NORMAL_NOTES = [
    "Kopi Kenangan Senopati", "Makan siang warteg", "Beli token listrik PLN", "Transfer uang jajan anak",
    "Belanja sayur FreshBox", "Tagihan internet Indihome", "Bensin Pertamina Shell", "Bayar kosan bulanan",
    "Gojek ride", "Shopee checkout kemeja", "Reimburse kantor meeting", "Arisan keluarga"
]

FRAUD_NOTES = [
    "Pajak tebusan undian berhadiah resmi", "Biaya admin verifikasi OTP Bank", "Deposit kilat judi slot gacor",
    "Titipan dana cepat kilat amanah", "Pencairan dana pinjol ilegal", "Tebusan pengembalian akun IG teretas"
]


def generate_transaction(force_pattern: str = None) -> Dict[str, Any]:
    """Generate a realistic financial transaction, optionally injecting specific fraud anomalies."""
    now = datetime.now(timezone.utc).isoformat()
    tx_id = f"TX-{uuid.uuid4().hex[:12].upper()}"

    # Determine transaction scenario (85% normal, 15% fraud/anomaly)
    pattern = force_pattern
    if not pattern:
        rand_val = random.random()
        if rand_val < 0.85:
            pattern = "LEGITIMATE"
        elif rand_val < 0.90:
            pattern = "ACCOUNT_TAKEOVER"
        elif rand_val < 0.95:
            pattern = "MULE_LAYERING"
        else:
            pattern = "PHISHING_SCAM"

    sender = random.choice(ACCOUNTS)
    recipient = random.choice(ACCOUNTS)
    while recipient["id"] == sender["id"]:
        recipient = random.choice(ACCOUNTS)

    channel = random.choice(["QRIS", "BI-FAST", "M-Banking", "VA"])
    city_data = next((c for c in INDONESIAN_CITIES if c["city"] == sender["city"]), INDONESIAN_CITIES[0])

    if pattern == "LEGITIMATE":
        amount = round(random.uniform(25000, 1200000), -3)
        device = sender["device"]
        ip = f"182.253.{random.randint(10, 250)}.{random.randint(1, 254)}"
        note = random.choice(NORMAL_NOTES)
        dest_account = recipient["id"]
        dest_bank = recipient["bank"]

    elif pattern == "ACCOUNT_TAKEOVER":
        # Sudden spike: 15x - 30x normal volume from unknown device in midnight hours
        amount = round(random.uniform(25000000, 48000000), -5)
        device = f"DEV-ROG-ROGUE-{random.randint(80, 99)}"
        ip = f"103.145.{random.randint(1, 254)}.{random.randint(1, 254)}"  # Datacenter/Proxy IP
        note = "Transfer cepat urgent keperluan mendesak"
        mule = random.choice(MULE_ACCOUNTS)
        dest_account = mule["id"]
        dest_bank = mule["bank"]
        channel = "M-Banking"
        # Suspicious distant city
        city_data = {"city": "Batam (Free Trade Zone)", "lat": 1.1301, "lon": 104.0529}

    elif pattern == "MULE_LAYERING":
        # Structuring/smurfing: Rapid transfers just below reporting thresholds
        amount = round(random.uniform(4500000, 4999000), -3)
        device = sender["device"]
        ip = f"114.125.{random.randint(1, 254)}.{random.randint(1, 254)}"
        note = random.choice(["Titipan dana cepat kilat amanah", "Pencairan dana pinjol", "Komisi referral"])
        mule = random.choice(MULE_ACCOUNTS)
        dest_account = mule["id"]
        dest_bank = mule["bank"]
        channel = "BI-FAST"

    elif pattern == "PHISHING_SCAM":
        amount = round(random.uniform(8000000, 25000000), -4)
        device = sender["device"]
        ip = f"36.85.{random.randint(1, 254)}.{random.randint(1, 254)}"
        note = random.choice(FRAUD_NOTES)
        mule = random.choice(MULE_ACCOUNTS)
        dest_account = mule["id"]
        dest_bank = mule["bank"]
        channel = "VA"

    else:
        amount = 500000.0
        device = sender["device"]
        ip = "127.0.0.1"
        note = "Standard test transaction"
        dest_account = recipient["id"]
        dest_bank = recipient["bank"]

    return {
        "transaction_id": tx_id,
        "timestamp": now,
        "account_from": sender["id"],
        "account_to": dest_account,
        "bank_from": sender["bank"],
        "bank_to": dest_bank,
        "amount": amount,
        "channel": channel,
        "device_fingerprint": device,
        "ip_address": ip,
        "location_city": city_data["city"],
        "latitude": city_data["lat"],
        "longitude": city_data["lon"],
        "transfer_note": note,
        "intended_pattern": pattern
    }
