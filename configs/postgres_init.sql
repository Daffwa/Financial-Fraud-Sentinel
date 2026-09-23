-- ============================================================================
-- Enterprise Real-Time Financial Fraud & AML Sentinel DDL Schema
-- Database: financial_fraud_db
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Accounts Master Table (Customer Profiles)
CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(32) PRIMARY KEY,
    owner_name VARCHAR(100) NOT NULL,
    bank_name VARCHAR(50) NOT NULL,
    account_type VARCHAR(20) DEFAULT 'Checking',
    balance NUMERIC(15, 2) NOT NULL DEFAULT 10000000.00,
    daily_limit NUMERIC(15, 2) NOT NULL DEFAULT 50000000.00,
    risk_level VARCHAR(20) DEFAULT 'LOW', -- LOW, MEDIUM, HIGH, SUSPENDED
    is_frozen BOOLEAN DEFAULT FALSE,
    frozen_reason TEXT,
    device_trusted VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Financial Transactions Stream Log (All incoming events)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    account_from VARCHAR(32) NOT NULL,
    account_to VARCHAR(32) NOT NULL,
    bank_from VARCHAR(50) NOT NULL,
    bank_to VARCHAR(50) NOT NULL,
    amount NUMERIC(15, 2) NOT NULL,
    channel VARCHAR(20) NOT NULL, -- QRIS, BI-FAST, M-Banking, VA, Card
    device_fingerprint VARCHAR(64),
    ip_address VARCHAR(45),
    location_city VARCHAR(100),
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    transfer_note TEXT,
    status VARCHAR(20) DEFAULT 'PROCESSED' -- PROCESSED, FLAGGED, BLOCKED
);

-- Indexes for lightning fast queries
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_account_from ON transactions(account_from);
CREATE INDEX IF NOT EXISTS idx_transactions_account_to ON transactions(account_to);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);

-- 3. Fraud & AML Anomaly Alerts (Populated by Laya AI Engine)
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fraud_pattern VARCHAR(50) NOT NULL, -- Account Takeover, Mule Account Layering, Phishing Scam, Carding, Legitimate
    risk_score NUMERIC(4, 2) NOT NULL,  -- Scale 0.00 - 5.00
    confidence NUMERIC(5, 4) NOT NULL,  -- Calibrated confidence 0.0 - 1.0
    immediate_block_trigger BOOLEAN NOT NULL DEFAULT FALSE,
    decision_latency_ms NUMERIC(6, 2) NOT NULL,
    account_from VARCHAR(32) NOT NULL,
    account_to VARCHAR(32) NOT NULL,
    amount NUMERIC(15, 2) NOT NULL,
    channel VARCHAR(20) NOT NULL,
    location_city VARCHAR(100),
    action_status VARCHAR(50) NOT NULL -- AUTO_FROZEN, FLAGGED_INVESTIGATION, AUTO_APPROVED
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_timestamp ON fraud_alerts(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_pattern ON fraud_alerts(fraud_pattern);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_action ON fraud_alerts(action_status);

-- 4. Automated Autonomous Actions Log (Proof of Automation)
CREATE TABLE IF NOT EXISTS automated_actions (
    action_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transaction_id VARCHAR(64) NOT NULL,
    target_account VARCHAR(32) NOT NULL,
    action_type VARCHAR(50) NOT NULL, -- ACCOUNT_FREEZE, 2FA_CHALLENGE, MULE_BLACKLISTED
    reason TEXT NOT NULL,
    confidence NUMERIC(5, 4) NOT NULL,
    execution_time_ms NUMERIC(6, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'EXECUTED'
);

CREATE INDEX IF NOT EXISTS idx_automated_actions_target ON automated_actions(target_account);

-- 5. Real-Time Telemetry & System Performance Metrics (For Grafana)
CREATE TABLE IF NOT EXISTS sentinel_metrics (
    metric_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tps NUMERIC(8, 2) NOT NULL DEFAULT 0.0,
    avg_decision_latency_ms NUMERIC(6, 2) NOT NULL DEFAULT 0.0,
    p99_latency_ms NUMERIC(6, 2) NOT NULL DEFAULT 0.0,
    fraud_rate_pct NUMERIC(5, 2) NOT NULL DEFAULT 0.0,
    total_transactions_processed BIGINT NOT NULL DEFAULT 0,
    total_fraud_blocked BIGINT NOT NULL DEFAULT 0
);

-- Seed realistic Indonesian accounts
INSERT INTO accounts (account_id, owner_name, bank_name, account_type, balance, risk_level, is_frozen, device_trusted)
VALUES 
    ('ACC-ID-8821901', 'Budi Santoso', 'BCA', 'Checking', 45000000.00, 'LOW', false, 'DEV-SAMSUNG-S23-01'),
    ('ACC-ID-7712034', 'Siti Rahmawati', 'Mandiri', 'Checking', 12500000.00, 'LOW', false, 'DEV-IPHONE15-02'),
    ('ACC-ID-6634129', 'Ahmad Hidayat', 'BRI', 'Savings', 8400000.00, 'LOW', false, 'DEV-XIAOMI-13-03'),
    ('ACC-ID-5541982', 'Dewi Lestari', 'BNI', 'Checking', 95000000.00, 'LOW', false, 'DEV-IPHONE14-04'),
    ('ACC-ID-4429810', 'Eko Prasetyo', 'BCA', 'Savings', 2300000.00, 'LOW', false, 'DEV-OPPO-RENO-05'),
    ('ACC-ID-3318721', 'Rian Kurniawan (Mule Candidate)', 'GoPay', 'E-Wallet', 150000.00, 'HIGH', false, 'DEV-UNKNOWN-06'),
    ('ACC-ID-2209183', 'Doni Iskandar (Suspicious)', 'OVO', 'E-Wallet', 50000.00, 'HIGH', false, 'DEV-UNKNOWN-07')
ON CONFLICT (account_id) DO NOTHING;
