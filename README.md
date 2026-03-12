# 🏦 Banking Data Platform — End-to-End Data Engineering Portfolio

> **Production-grade batch analytics pipeline for banking domain**  
> Multi-source ingestion → Trino federation → BigQuery → dbt → Looker

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SOURCE SYSTEMS                                     │
│                                                                             │
│  ┌────────────────────── ┐        ┌──────────────────────────────────────┐  │
│  │  PostgreSQL 15        │        │  MySQL 8.0                           │  │
│  │  (Core Banking)       │        │  (Transaction System)                │  │
│  │                       │        │                                      │  │
│  │  • customers          │        │  • transactions                      │  │
│  │  • accounts           │        │  • merchants                         │  │
│  │  • branches           │        │  • fraud_flags                       │  │
│  │  • employees          │        │  • payment_methods                   │  │
│  │  • loan_applications  │        │  • transaction_types                 │  │
│  │  • credit_scores      │        │                                      │  │
│  │                       │        │  [WAL/binlog pre-configured          │  │
│  │  [wal_level=logical   │        │   for future CDC via Debezium]       │  │
│  │   pre-configured]     │        │                                      │  │
│  └──────────┬─────────── ┘        └───────────────┬──────────────────────┘  │
│             │                                     │                         │
└─────────────┼─────────────────────────────────────┼─────────────────────────┘
              │                                     │
              ▼                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         QUERY FEDERATION LAYER                              │
│                                                                             │
│                    ┌──────────────────────────┐                             │
│                    │   Trino (Query Engine)    │                            │
│                    │                           │                            │
│                    │  Catalogs:                │                            │
│                    │  • postgresql (connector) │                            │
│                    │  • mysql     (connector)  │                            │
│                    │  • bigquery  (connector)  │                            │
│                    └──────────────┬────────────┘                            │
│                                   │                                         │
└───────────────────────────────────┼─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION (Apache Airflow 2.8)                      │
│                                                                             │
│  DAGs:                                                                      │
│  ├── dag_core_banking       (Postgres → BigQuery raw)                       │
│  ├── dag_transactions       (MySQL → BigQuery raw)                          │
│  └── dbt_transformation     (BigQuery raw → staging → marts)                │  │                                                                             │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE (Google BigQuery)                          │
│                                                                             │
│  Dataset: raw_core_banking      ← Airflow (Postgres extract)                │
│  Dataset: raw_transactions      ← Airflow (MySQL extract)                   │
│  Dataset: staging               ← dbt (cleaned, typed, documented)          │
│  Dataset: intermediate          ← dbt (business logic joins)                │
│  Dataset: marts                 ← dbt (analytics-ready aggregations)        │
│           ├── customer/                                                     │
│           ├── risk/                                                         │
│           └── finance/                                                      │
│                                                                             │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BI & ANALYTICS (Looker / LookML)                         │
│                                                                             │
│  • Customer 360 Dashboard                                                   │
│  • Fraud & Risk Analytics                                                   │
│  • Transaction Volume & Revenue                                             │
│  • Loan Portfolio Performance                                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│              🔮 FUTURE: CDC Real-Time Streaming (Phase 2)                  │
│                                                                             │
│  PostgreSQL (WAL) ──► Debezium ──► Kafka ──► ClickHouse ──► Grafana         │
│  MySQL (binlog)   ──►                                                       │
│                                                                             │
│  [Infrastructure pre-configured in this project — not yet activated]        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🗂️ Project Structure

```
banking-data-platform/
│
├── README.md                          # This file
├── SETUP_GCP.md                       # Step-by-step GCP setup guide
├── docker-compose.yml                 # All local services
├── docker-compose.override.yml        # Dev-only overrides
├── .env.example                       # Environment variables template
├── .gitignore
├── Makefile                           # Common dev commands
│
├── docs/
│   ├── architecture.md                # Detailed architecture decisions
│   ├── data-dictionary.md             # All table definitions
│   └── cdc-roadmap.md                 # Phase 2 CDC planning
│
├── data-generator/                    # Synthetic banking data
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config/settings.py
│   ├── generators/
│   │   ├── core_banking/              # Postgres generators
│   │   │   ├── customers.py
│   │   │   ├── accounts.py
│   │   │   ├── branches.py
│   │   │   ├── employees.py
│   │   │   └── loan_applications.py
│   │   └── transaction/               # MySQL generators
│   │       ├── transactions.py
│   │       ├── merchants.py
│   │       └── fraud_flags.py
│   ├── schemas/
│   │   ├── postgres_schema.sql        # DDL with WAL config
│   │   └── mysql_schema.sql           # DDL with binlog config
│   └── main.py
│
├── ingestion/
│   ├── trino/
│   │   ├── config/config.properties
│   │   └── catalogs/
│   │       ├── postgresql.properties
│   │       ├── mysql.properties
│   │       └── bigquery.properties
│   └── scripts/
│       ├── extract_postgres.py        # Incremental extract via Trino
│       └── extract_mysql.py           # Incremental extract via Trino
│
├── orchestration/
│   └── airflow/
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── dags/
│       │   ├── dag_core_banking.py
│       │   ├── dag_transactions.py
│       │   └── dbt_transformation_dag.py
│       └── plugins/
│
├── transformation/
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── packages.yml
│       ├── models/
│       │   ├── staging/
│       │   │   ├── _sources.yml
│       │   │   ├── core_banking/      # stg_customers, stg_accounts, ...
│       │   │   └── transactions/      # stg_transactions, stg_merchants, ...
│       │   ├── intermediate/          # Business logic joins
│       │   └── marts/
│       │       ├── customer/          # mart_customer_360, mart_clv
│       │       ├── risk/              # mart_fraud_analytics, mart_credit_risk
│       │       └── finance/           # mart_transaction_analytics
│       ├── tests/
│       ├── macros/
│       │   ├── generate_schema_name.sql
│       │   ├── audit_columns.sql
│       │   └── banking_utils.sql
│       └── snapshots/                 # SCD Type 2 for customers & accounts
│
├── monitoring/
│   ├── great_expectations/            # Data quality suite
│   └── alerts/alert_config.yml        # Alerting rules
│
└── scripts/
    ├── setup.sh                       # Local dev bootstrap
    ├── setup_gcp.sh                   # GCP project bootstrap
    └── init_cdc.sh                    # CDC pre-configuration validator
```

---

## ⚡ Quick Start (Local Development)

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Docker | ≥ 24.0 | Container runtime |
| Docker Compose | ≥ 2.20 | Service orchestration |
| Python | ≥ 3.11 | Scripts & generators |
| `gcloud` CLI | latest | GCP interaction |
| `dbt-bigquery` | ≥ 1.7 | Transformations |
| Make | any | Dev shortcuts |

### 1. Clone & Configure

```bash
git clone https://github.com/yourname/banking-data-platform.git
cd banking-data-platform

# Copy and edit environment variables
cp .env.example .env
# Edit .env with your actual values (GCP project ID, credentials, etc.)
```

### 2. Bootstrap Local Services

```bash
# Start all local services (Postgres, MySQL, Trino, Airflow)
make up

# Verify all services are healthy
make health

# Generate synthetic banking data
make generate-data

# Verify data was loaded
make verify-data
```

### 3. Run Ingestion Pipeline

```bash
# Trigger ingestion DAGs via Airflow UI
open http://localhost:8080   # admin / admin

# Or trigger manually via CLI
make trigger-ingest
```

### 4. Run dbt Transformations

```bash
cd transformation/dbt

# Install dependencies
dbt deps

# Run staging + intermediate + marts
dbt run --profiles-dir . --target dev

# Run data quality tests
dbt test --profiles-dir . --target dev

# Generate documentation
dbt docs generate && dbt docs serve
```

---

## 🏛️ Data Models

### Source: Core Banking (PostgreSQL)

```sql
-- Key tables
customers          → 500K rows  (PII encrypted at rest)
accounts           → 1.2M rows  (savings, checking, loans, credit)
branches           → 150 rows
employees          → 2K rows
loan_applications  → 200K rows
credit_scores      → 500K rows
```

### Source: Transaction System (MySQL)

```sql
transactions       → 10M+ rows (incremental daily load)
merchants          → 50K rows
fraud_flags        → 100K rows
payment_methods    → 8 types
transaction_types  → 15 types
```

### BigQuery Marts

| Mart | Description | Refresh |
|------|-------------|---------|
| `mart_customer_360` | Unified customer profile with all products | Daily |
| `mart_customer_lifetime_value` | CLV segmentation & scoring | Weekly |
| `mart_transaction_analytics` | Daily transaction volume, revenue | Daily |
| `mart_fraud_analytics` | Fraud patterns, risk scoring | Daily |
| `mart_credit_risk` | Loan performance, NPL ratios | Daily |
| `mart_product_performance` | Product adoption, cross-sell metrics | Weekly |

---

## 🔒 Banking-Grade Best Practices

### Security
- **PII Masking**: Customer PII (name, NIK, phone) masked in `staging` layer using BigQuery column-level security
- **Encryption**: All GCS buckets use CMEK (Customer-Managed Encryption Keys)
- **IAM**: Least-privilege service accounts per component
- **Secret Management**: All credentials via GCP Secret Manager (never in `.env` in production)
- **Network**: VPC-native setup, Private Google Access enabled
- **Audit Log**: BigQuery Data Access audit logs enabled

### Data Quality
- **Source freshness** checks in dbt (`source freshness`)
- **Not-null, unique, accepted-values** tests on all primary & foreign keys
- **Great Expectations** suites for raw data validation before ingestion
- **Referential integrity** tests across source joins

### Reliability
- **Idempotent** DAGs: safe to re-run without duplicating data
- **Incremental loads** with `updated_at` watermark tracking
- **Dead-letter queue**: Failed records written to `raw_errors` dataset
- **Alerting**: Airflow email + Slack alerts on SLA miss

### Observability
- **dbt artifacts** (manifest, run_results) stored to GCS for lineage
- **Airflow metrics** via StatsD → Prometheus → Grafana (optional)
- **BigQuery slot usage** dashboards in Looker

### Compliance (Banking)
- **Data lineage** tracked end-to-end via dbt lineage graph
- **Row-level security** in BigQuery for multi-branch access
- **Data retention** policies on raw datasets (90 days)
- **GDPR/right-to-be-forgotten** workflow via dbt macro

---

## 🔮 Phase 2: CDC Real-Time Streaming (Roadmap)

The infrastructure is **pre-configured** for CDC but not yet active:

```
PostgreSQL  ──► (WAL logical replication configured)
MySQL       ──► (binlog ROW format configured)
                        │
                        ▼
                   Debezium (Kafka Connect)
                        │
                        ▼
                   Apache Kafka
                        │
                        ▼
                   ClickHouse (OLAP)
                        │
                        ▼
                   Grafana (Real-time dashboards)
```

**What's already done for CDC readiness:**
- PostgreSQL: `wal_level=logical`, `max_replication_slots=5`, `max_wal_senders=5`
- MySQL: `binlog_format=ROW`, `binlog_row_image=FULL`, `expire_logs_days=7`
- Tables have `created_at`, `updated_at`, and `deleted_at` (soft-delete) columns
- Primary keys defined on all tables
- `scripts/init_cdc.sh` validates CDC readiness

---

## 🌐 GCP Services Used

| Service | Purpose |
|---------|---------|
| BigQuery | Data warehouse, SQL transformations |
| Cloud Storage (GCS) | Raw data lake landing zone |
| Cloud Composer (optional) | Managed Airflow |
| Artifact Registry | Docker image storage |
| Secret Manager | Credentials management |
| Cloud Run / GCE | Airflow worker (self-managed) |
| IAM & VPC | Security & networking |

> **Full GCP setup guide → [`SETUP_GCP.md`](./SETUP_GCP.md)**

---

## 🛠️ Make Commands Reference

```bash
make up              # Start all Docker services
make down            # Stop all services
make health          # Check service health
make generate-data   # Run data generator (Postgres + MySQL)
make verify-data     # Count rows in source tables
make trigger-ingest  # Trigger Airflow ingestion DAGs
make dbt-run         # Run all dbt models
make dbt-test        # Run all dbt tests
make dbt-docs        # Generate & serve dbt docs
make lint            # Run SQLFluff linter on dbt models
make clean           # Remove volumes and containers
make logs            # Tail logs for all services
```

---

