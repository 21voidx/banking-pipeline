-- ============================================================
-- PostgreSQL Core Banking Schema
-- Timezone: All DEFAULT timestamps use Asia/Jakarta (WIB, UTC+7)
-- Pre-configured for CDC (WAL logical replication)
-- ============================================================
-- All tables include:
--   - created_at / updated_at timestamps (for incremental load)
--   - deleted_at (soft delete pattern)
--   - Primary keys (required for CDC)
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";   -- For PII encryption

-- ─────────────────────────────────────────────
-- Reference Data
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS branches (
    branch_id       SERIAL PRIMARY KEY,
    branch_code     VARCHAR(10)  NOT NULL UNIQUE,
    branch_name     VARCHAR(100) NOT NULL,
    branch_type     VARCHAR(20)  NOT NULL CHECK (branch_type IN ('main', 'regional', 'sub', 'atm_center')),
    city            VARCHAR(50)  NOT NULL,
    province        VARCHAR(50)  NOT NULL,
    address         TEXT,
    phone           VARCHAR(20),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    opened_date     DATE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    deleted_at      TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS product_types (
    product_type_id SERIAL PRIMARY KEY,
    product_code    VARCHAR(20)  NOT NULL UNIQUE,
    product_name    VARCHAR(100) NOT NULL,
    product_category VARCHAR(50) NOT NULL CHECK (product_category IN ('savings', 'checking', 'loan', 'credit_card', 'deposit', 'investment')),
    interest_rate   NUMERIC(5,4),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta')
);

-- ─────────────────────────────────────────────
-- Customers
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customers (
    customer_id         SERIAL PRIMARY KEY,
    customer_uuid       UUID         NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    -- PII fields — encrypted at rest via pgcrypto
    -- In BigQuery: masked using column-level security
    full_name           VARCHAR(200) NOT NULL,
    national_id         VARCHAR(20)  NOT NULL UNIQUE,  -- NIK (encrypted in prod)
    date_of_birth       DATE         NOT NULL,
    gender              CHAR(1)      NOT NULL CHECK (gender IN ('M', 'F')),
    marital_status      VARCHAR(20)  CHECK (marital_status IN ('single', 'married', 'divorced', 'widowed')),
    occupation          VARCHAR(100),
    income_range        VARCHAR(30)  CHECK (income_range IN ('0-3jt', '3-5jt', '5-10jt', '10-25jt', '25-50jt', '>50jt')),
    -- Contact
    email               VARCHAR(200),
    phone_primary       VARCHAR(20),
    phone_secondary     VARCHAR(20),
    -- Address
    address_street      TEXT,
    address_city        VARCHAR(50),
    address_province    VARCHAR(50),
    address_postal_code VARCHAR(10),
    -- Banking
    customer_segment    VARCHAR(30)  NOT NULL DEFAULT 'retail' CHECK (customer_segment IN ('retail', 'priority', 'premier', 'private', 'sme', 'corporate')),
    kyc_status          VARCHAR(20)  NOT NULL DEFAULT 'pending' CHECK (kyc_status IN ('pending', 'verified', 'rejected', 'expired')),
    kyc_verified_at     TIMESTAMPTZ,
    onboarding_branch_id INT         REFERENCES branches(branch_id),
    acquisition_channel VARCHAR(30)  CHECK (acquisition_channel IN ('branch', 'mobile_app', 'web', 'agent', 'referral', 'telemarketing')),
    risk_rating         VARCHAR(10)  DEFAULT 'low' CHECK (risk_rating IN ('low', 'medium', 'high', 'blacklist')),
    is_politically_exposed BOOLEAN   NOT NULL DEFAULT FALSE,
    -- Audit
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    deleted_at          TIMESTAMPTZ
);

-- ─────────────────────────────────────────────
-- Employees
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS employees (
    employee_id     SERIAL PRIMARY KEY,
    employee_code   VARCHAR(20)  NOT NULL UNIQUE,
    full_name       VARCHAR(200) NOT NULL,
    email           VARCHAR(200) NOT NULL UNIQUE,
    role            VARCHAR(50)  NOT NULL CHECK (role IN ('teller', 'cs', 'relationship_manager', 'branch_manager', 'back_office', 'analyst', 'it')),
    branch_id       INT          REFERENCES branches(branch_id),
    hire_date       DATE         NOT NULL,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    deleted_at      TIMESTAMPTZ
);

-- ─────────────────────────────────────────────
-- Accounts
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS accounts (
    account_id          SERIAL PRIMARY KEY,
    account_number      VARCHAR(20)  NOT NULL UNIQUE,
    customer_id         INT          NOT NULL REFERENCES customers(customer_id),
    product_type_id     INT          NOT NULL REFERENCES product_types(product_type_id),
    branch_id           INT          NOT NULL REFERENCES branches(branch_id),
    currency            CHAR(3)      NOT NULL DEFAULT 'IDR',
    balance             NUMERIC(20,2) NOT NULL DEFAULT 0.00,
    available_balance   NUMERIC(20,2) NOT NULL DEFAULT 0.00,
    account_status      VARCHAR(20)  NOT NULL DEFAULT 'active' CHECK (account_status IN ('active', 'dormant', 'blocked', 'closed', 'pending')),
    opened_date         DATE         NOT NULL DEFAULT CURRENT_DATE,
    closed_date         DATE,
    last_transaction_at TIMESTAMPTZ,
    -- Loan-specific fields (nullable for non-loan accounts)
    credit_limit        NUMERIC(20,2),
    outstanding_balance NUMERIC(20,2),
    interest_rate       NUMERIC(5,4),
    tenor_months        INT,
    due_date            DATE,
    dpd                 INT          DEFAULT 0,  -- Days Past Due
    -- Audit
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    deleted_at          TIMESTAMPTZ
);

-- ─────────────────────────────────────────────
-- Credit Scores
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS credit_scores (
    score_id        SERIAL PRIMARY KEY,
    customer_id     INT          NOT NULL REFERENCES customers(customer_id),
    score_value     INT          NOT NULL CHECK (score_value BETWEEN 300 AND 850),
    score_band      VARCHAR(20)  NOT NULL CHECK (score_band IN ('poor', 'fair', 'good', 'very_good', 'exceptional')),
    score_date      DATE         NOT NULL DEFAULT CURRENT_DATE,
    model_version   VARCHAR(20)  NOT NULL DEFAULT 'v2.0',
    data_source     VARCHAR(50)  NOT NULL DEFAULT 'internal',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta')
);

-- ─────────────────────────────────────────────
-- Loan Applications
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS loan_applications (
    application_id      SERIAL PRIMARY KEY,
    application_number  VARCHAR(30)  NOT NULL UNIQUE,
    customer_id         INT          NOT NULL REFERENCES customers(customer_id),
    branch_id           INT          NOT NULL REFERENCES branches(branch_id),
    handled_by          INT          REFERENCES employees(employee_id),
    product_type_id     INT          NOT NULL REFERENCES product_types(product_type_id),
    requested_amount    NUMERIC(20,2) NOT NULL,
    approved_amount     NUMERIC(20,2),
    tenor_months        INT          NOT NULL,
    interest_rate       NUMERIC(5,4),
    purpose             VARCHAR(100),
    collateral_type     VARCHAR(50)  CHECK (collateral_type IN ('none', 'property', 'vehicle', 'deposit', 'guarantee', 'other')),
    collateral_value    NUMERIC(20,2),
    application_status  VARCHAR(30)  NOT NULL DEFAULT 'submitted' CHECK (application_status IN ('submitted', 'under_review', 'approved', 'rejected', 'disbursed', 'cancelled', 'withdrawn')),
    submitted_at        TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    reviewed_at         TIMESTAMPTZ,
    decided_at          TIMESTAMPTZ,
    disbursed_at        TIMESTAMPTZ,
    rejection_reason    TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Jakarta'),
    deleted_at          TIMESTAMPTZ
);

-- ─────────────────────────────────────────────
-- Indexes (for query performance)
-- ─────────────────────────────────────────────

-- Customers
CREATE INDEX idx_customers_updated_at     ON customers(updated_at);
CREATE INDEX idx_customers_segment        ON customers(customer_segment);
CREATE INDEX idx_customers_kyc_status     ON customers(kyc_status);

-- Accounts
CREATE INDEX idx_accounts_customer_id     ON accounts(customer_id);
CREATE INDEX idx_accounts_updated_at      ON accounts(updated_at);
CREATE INDEX idx_accounts_status          ON accounts(account_status);
CREATE INDEX idx_accounts_product_type    ON accounts(product_type_id);

-- Loan Applications
CREATE INDEX idx_loan_apps_customer_id    ON loan_applications(customer_id);
CREATE INDEX idx_loan_apps_updated_at     ON loan_applications(updated_at);
CREATE INDEX idx_loan_apps_status         ON loan_applications(application_status);

-- Credit Scores
CREATE INDEX idx_credit_scores_customer   ON credit_scores(customer_id, score_date DESC);

-- ─────────────────────────────────────────────
-- Triggers: Auto-update updated_at
-- ─────────────────────────────────────────────

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = (NOW() AT TIME ZONE 'Asia/Jakarta');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    t TEXT;
BEGIN
    FOR t IN SELECT unnest(ARRAY['customers', 'accounts', 'branches', 'employees', 'loan_applications', 'credit_scores', 'product_types'])
    LOOP
        EXECUTE format(
            'CREATE TRIGGER trg_%s_updated_at
             BEFORE UPDATE ON %s
             FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();',
            t, t
        );
    END LOOP;
END;
$$;

-- ─────────────────────────────────────────────
-- CDC: Create replication publication
-- (Activated in Phase 2 with Debezium)
-- ─────────────────────────────────────────────

-- Publication covers all CDC-relevant tables
-- Run this when enabling CDC:
-- CREATE PUBLICATION banking_cdc_publication
--     FOR TABLE customers, accounts, loan_applications, credit_scores
--     WITH (publish = 'insert, update, delete');

-- Verify WAL settings (should be 'logical' for CDC):
-- SELECT name, setting FROM pg_settings WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders');

COMMENT ON TABLE customers         IS 'Core customer master data. PII fields require column-level security in BigQuery.';
COMMENT ON TABLE accounts          IS 'Bank accounts (savings, checking, loans, credit cards).';
COMMENT ON TABLE branches          IS 'Bank branch reference data.';
COMMENT ON TABLE employees         IS 'Bank employee reference data.';
COMMENT ON TABLE loan_applications IS 'Loan application lifecycle tracking.';
COMMENT ON TABLE credit_scores     IS 'Customer credit scoring history (SCD Type 2 via dbt snapshot).';
COMMENT ON TABLE product_types     IS 'Banking product catalog.';
