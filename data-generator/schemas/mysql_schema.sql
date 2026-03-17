-- ============================================================
-- MySQL Transaction System Schema
-- Timezone: SET time_zone = '+07:00' (WIB, UTC+7) applied at session level
-- Pre-configured for CDC (binlog ROW format)
-- ============================================================
-- MySQL config (see infrastructure/configs/mysql.cnf):
--   binlog_format = ROW
--   binlog_row_image = FULL
--   expire_logs_days = 7
--   server_id = 1  (unique per replica in CDC setup)
-- ============================================================

-- Use UTF8MB4 for full Unicode support (emojis, etc.)
CREATE DATABASE IF NOT EXISTS transaction_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE transaction_db;

-- Set session timezone ke WIB untuk semua DEFAULT CURRENT_TIMESTAMP
SET time_zone = '+07:00';

-- ─────────────────────────────────────────────
-- Reference Data
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS transaction_types (
    type_id         INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    type_code       VARCHAR(20)  NOT NULL UNIQUE,
    type_name       VARCHAR(100) NOT NULL,
    type_category   VARCHAR(30)  NOT NULL COMMENT 'debit | credit | transfer | fee | reversal',
    is_active       TINYINT(1)   NOT NULL DEFAULT 1,
    created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS payment_methods (
    method_id       INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    method_code     VARCHAR(20)  NOT NULL UNIQUE,
    method_name     VARCHAR(100) NOT NULL,
    is_active       TINYINT(1)   NOT NULL DEFAULT 1,
    created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS merchant_categories (
    mcc_code        VARCHAR(10)  NOT NULL PRIMARY KEY  COMMENT 'Merchant Category Code (ISO 18245)',
    category_name   VARCHAR(100) NOT NULL,
    is_high_risk    TINYINT(1)   NOT NULL DEFAULT 0,
    created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────
-- Merchants
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id         INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    merchant_uuid       VARCHAR(36)  NOT NULL UNIQUE DEFAULT (UUID()),
    merchant_name       VARCHAR(200) NOT NULL,
    merchant_legal_name VARCHAR(200),
    mcc_code            VARCHAR(10)  REFERENCES merchant_categories(mcc_code),
    city                VARCHAR(50),
    province            VARCHAR(50),
    country             CHAR(2)      NOT NULL DEFAULT 'ID',
    is_online           TINYINT(1)   NOT NULL DEFAULT 0,
    risk_level          VARCHAR(10)  NOT NULL DEFAULT 'normal' COMMENT 'normal | elevated | high',
    is_active           TINYINT(1)   NOT NULL DEFAULT 1,
    created_at          DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    deleted_at          DATETIME(6)  DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────
-- Transactions
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      BIGINT       NOT NULL AUTO_INCREMENT,
    -- UNIQUE tidak bisa inline di partitioned table kecuali include partition key.
    -- Dipindah ke INDEX di bawah dengan menyertakan transaction_date.
    transaction_uuid    VARCHAR(36)  NOT NULL DEFAULT (UUID()),
    -- References
    account_id          INT          NOT NULL COMMENT 'FK to PostgreSQL accounts.account_id',
    customer_id         INT          NOT NULL COMMENT 'FK to PostgreSQL customers.customer_id',
    merchant_id         INT          REFERENCES merchants(merchant_id),
    type_id             INT          NOT NULL REFERENCES transaction_types(type_id),
    method_id           INT          REFERENCES payment_methods(method_id),
    -- Financial
    amount              DECIMAL(20,2) NOT NULL,
    currency            CHAR(3)      NOT NULL DEFAULT 'IDR',
    exchange_rate       DECIMAL(15,6) NOT NULL DEFAULT 1.000000,
    amount_idr          DECIMAL(20,2) NOT NULL COMMENT 'Amount normalized to IDR',
    fee_amount          DECIMAL(20,2) NOT NULL DEFAULT 0.00,
    -- Balance snapshot
    balance_before      DECIMAL(20,2),
    balance_after       DECIMAL(20,2),
    -- Metadata
    description         VARCHAR(500),
    -- UNIQUE reference_number juga harus include transaction_date di partitioned table.
    -- Dipindah ke UNIQUE INDEX di bawah.
    reference_number    VARCHAR(50),
    channel             VARCHAR(30)  COMMENT 'atm | mobile | web | branch | pos | qris',
    transaction_status  VARCHAR(20)  NOT NULL DEFAULT 'completed' COMMENT 'pending | completed | failed | reversed',
    status_reason       VARCHAR(200),
    -- Timestamps
    transaction_date    DATE         NOT NULL COMMENT 'Partition key — wajib ada di semua UNIQUE index',
    transaction_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    processed_at        DATETIME(6),
    -- Audit
    created_at          DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    -- Aturan MySQL: semua UNIQUE index di partitioned table HARUS include partition key.
    -- ERROR 1503 jika ada UNIQUE index yang tidak menyertakan transaction_date.
    PRIMARY KEY (transaction_id, transaction_date),
    UNIQUE KEY uq_transaction_uuid (transaction_uuid, transaction_date),
    UNIQUE KEY uq_reference_number (reference_number, transaction_date),
    INDEX idx_account_date      (account_id, transaction_date),
    INDEX idx_customer_date     (customer_id, transaction_date),
    INDEX idx_transaction_date  (transaction_date),
    INDEX idx_updated_at        (updated_at),
    INDEX idx_merchant_id       (merchant_id),
    INDEX idx_status            (transaction_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (YEAR(transaction_date) * 100 + MONTH(transaction_date)) (
    PARTITION p202401 VALUES LESS THAN (202402),
    PARTITION p202402 VALUES LESS THAN (202403),
    PARTITION p202403 VALUES LESS THAN (202404),
    PARTITION p202404 VALUES LESS THAN (202405),
    PARTITION p202405 VALUES LESS THAN (202406),
    PARTITION p202406 VALUES LESS THAN (202407),
    PARTITION p202407 VALUES LESS THAN (202408),
    PARTITION p202408 VALUES LESS THAN (202409),
    PARTITION p202409 VALUES LESS THAN (202410),
    PARTITION p202410 VALUES LESS THAN (202411),
    PARTITION p202411 VALUES LESS THAN (202412),
    PARTITION p202412 VALUES LESS THAN (202501),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- ─────────────────────────────────────────────
-- Fraud Flags
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fraud_flags (
    flag_id             BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    transaction_id      BIGINT       NOT NULL REFERENCES transactions(transaction_id),
    customer_id         INT          NOT NULL,
    flag_type           VARCHAR(50)  NOT NULL COMMENT 'velocity | geo_anomaly | amount_anomaly | device_fingerprint | blacklist_merchant',
    severity            VARCHAR(10)  NOT NULL DEFAULT 'medium' COMMENT 'low | medium | high | critical',
    confidence_score    DECIMAL(5,4) NOT NULL COMMENT '0.0000 to 1.0000',
    model_version       VARCHAR(20)  NOT NULL DEFAULT 'v1.0',
    is_confirmed_fraud  TINYINT(1)   DEFAULT NULL COMMENT 'NULL=pending review, 1=confirmed, 0=false positive',
    reviewed_by         VARCHAR(100),
    reviewed_at         DATETIME(6),
    notes               TEXT,
    created_at          DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    INDEX idx_transaction_id (transaction_id),
    INDEX idx_customer_id (customer_id),
    INDEX idx_severity_date (severity, created_at),
    INDEX idx_unreviewed (is_confirmed_fraud, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────
-- Seed Reference Data
-- ─────────────────────────────────────────────

INSERT INTO transaction_types (type_code, type_name, type_category) VALUES
    ('DEBIT_PURCHASE',    'Debit Purchase',           'debit'),
    ('CREDIT_PURCHASE',   'Credit Card Purchase',     'debit'),
    ('ATM_WITHDRAWAL',    'ATM Cash Withdrawal',      'debit'),
    ('TRANSFER_OUT',      'Transfer Out',             'debit'),
    ('TRANSFER_IN',       'Transfer Incoming',        'credit'),
    ('SALARY_CREDIT',     'Salary Credit',            'credit'),
    ('LOAN_DISBURSEMENT', 'Loan Disbursement',        'credit'),
    ('LOAN_PAYMENT',      'Loan Monthly Payment',     'debit'),
    ('BILL_PAYMENT',      'Bill Payment',             'debit'),
    ('QRIS_PAYMENT',      'QRIS Digital Payment',     'debit'),
    ('FEE_ADMIN',         'Administration Fee',       'fee'),
    ('FEE_TRANSFER',      'Transfer Fee',             'fee'),
    ('REVERSAL',          'Transaction Reversal',     'reversal'),
    ('INTEREST_INCOME',   'Interest Income',          'credit'),
    ('INTEREST_EXPENSE',  'Interest Expense',         'debit')
ON DUPLICATE KEY UPDATE type_name = VALUES(type_name);

INSERT INTO payment_methods (method_code, method_name) VALUES
    ('DEBIT_CARD',   'Debit Card'),
    ('CREDIT_CARD',  'Credit Card'),
    ('BANK_TRANSFER','Bank Transfer'),
    ('QRIS',         'QRIS'),
    ('VIRTUAL_ACC',  'Virtual Account'),
    ('CASH',         'Cash'),
    ('MOBILE_BANKING','Mobile Banking'),
    ('INTERNET_BANKING','Internet Banking')
ON DUPLICATE KEY UPDATE method_name = VALUES(method_name);

INSERT INTO merchant_categories (mcc_code, category_name, is_high_risk) VALUES
    ('5411', 'Grocery Stores & Supermarkets', 0),
    ('5812', 'Eating Places & Restaurants', 0),
    ('5541', 'Service Stations & Fuel', 0),
    ('5912', 'Drug Stores & Pharmacies', 0),
    ('5732', 'Electronics Stores', 0),
    ('5311', 'Department Stores', 0),
    ('7011', 'Hotels & Lodging', 0),
    ('4511', 'Airlines & Air Carriers', 0),
    ('7995', 'Gambling & Betting', 1),
    ('6051', 'Crypto & Currency Exchange', 1),
    ('5999', 'Misc Retail Stores', 0),
    ('7372', 'Computer Services & Software', 0),
    ('8099', 'Health Services', 0),
    ('5661', 'Shoe Stores', 0),
    ('5691', 'Clothing & Apparel', 0)
ON DUPLICATE KEY UPDATE category_name = VALUES(category_name);