"""
dbt_transformation_dag.py
═════════════════════════════════════════════════════════════════════════════
DAG     : dbt_transformation
Schedule: Daily 00:00 UTC (07:00 WIB) — setelah ingestion DAGs selesai

Pipeline (BashOperator, dbt via uv venv di Airflow image):

  start
    │
  dbt_deps                      ← install / update dbt packages
    │
  dbt_source_freshness          ← validasi raw data sudah ter-load (BLOCKING)
    │
  ┌──────────┬──────────────┐
  run_stg_   run_stg_       │   ← staging core_banking & transactions paralel
  core       txn            │
  └────┬─────┴──────────────┘
       │
  test_staging               ← dbt test seluruh staging layer (BLOCKING)
       │
  ┌──────────────────────────────────────┐
  run_mart_   run_mart_   run_mart_      │   ← 3 domain mart paralel
  customer    risk        finance        │
  └─────┬──────────┴──────────┴──────────┘
        │
  test_marts                 ← dbt test seluruh marts (non-blocking, ALL_DONE)
        │
  run_snapshots              ← SCD Type 2 (ALL_DONE)
        │
  dbt_docs_generate          ← generate manifest & catalog (ALL_DONE)
        │
  end

Failure handling:
  - source_freshness FAILED  → pipeline stop, alert (data belum siap)
  - test_staging FAILED      → pipeline stop, alert (data source corrupt)
  - test_marts FAILED        → alert saja, tidak stop (marts tetap publish)
  - Semua task: retry=1, delay=5 menit
  - Slack alert via SlackWebhookOperator pada setiap task failure

Env vars yang dibutuhkan (set di Airflow Variables atau docker-compose .env):
  DBT_PROJECT_DIR               path ke dbt project di dalam container
  DBT_PROFILES_DIR              path ke profiles.yml
  DBT_VENV_PATH                 path ke venv dbt (/opt/airflow/dbt_venv)
  DBT_TARGET                    prod / dev / ci
  GCP_PROJECT_ID                GCP project ID untuk BigQuery
  BQ_LOCATION                   BigQuery location (default: asia-southeast2)
  GOOGLE_APPLICATION_CREDENTIALS path ke service account keyfile
  SLACK_WEBHOOK_URL             Slack incoming webhook (opsional)

Catatan arsitektur:
  - dbt berjalan via /opt/airflow/dbt_venv/bin/dbt (uv venv terpisah)
  - Tidak ada konflik dependency dengan Airflow karena isolated venv
  - CeleryExecutor: setiap BashOperator task di-dispatch ke worker terpisah
  - Parallelisme antar task dikontrol Airflow (bukan dbt --threads)
═════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.timetables.interval import CronDataIntervalTimetable

# ─── Paths & constants ────────────────────────────────────────────────────────

# dbt binary dari uv venv — sesuai Dockerfile
DBT_BIN          = os.getenv("DBT_VENV_PATH", "/opt/airflow/dbt_venv") + "/bin/dbt"

# dbt project & profiles — di-mount ke Airflow worker via volume
DBT_PROJECT_DIR  = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")

# Target profile: prod untuk scheduled run, override saat manual trigger
DBT_TARGET       = os.getenv("DBT_TARGET", "prod")

# Slack alert — opsional, kosongkan untuk disable
SLACK_WEBHOOK    = os.getenv("SLACK_WEBHOOK_URL", "")

# ─── Base dbt command (reused di setiap task) ─────────────────────────────────
# --no-write-json: skip manifest write di source freshness agar lebih cepat
# --profiles-dir & --project-dir selalu eksplisit agar tidak bergantung CWD

DBT_BASE = (
    f"{DBT_BIN}"
    f" --project-dir {DBT_PROJECT_DIR}"
    f" --profiles-dir {DBT_PROFILES_DIR}"
    f" --target {DBT_TARGET}"
)

# ─── Env vars yang di-inject ke setiap BashOperator ──────────────────────────
# Airflow worker sudah punya env ini via docker-compose / Cloud Composer,
# tapi kita pass eksplisit agar tidak bergantung pada env worker yang berbeda.

TASK_ENV = {
    "GCP_PROJECT_ID":                 os.getenv("GCP_PROJECT_ID", "banking-modernstack"),
    "BQ_LOCATION":                    os.getenv("BQ_LOCATION", "US"),
    "GOOGLE_APPLICATION_CREDENTIALS": os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "/opt/gcp/service-account.json",
    ),
    # dbt execution_date — digunakan oleh macro get_execution_date() di dbt
    "EXECUTION_DATE": "{{ ds }}",
}

# ─── Slack alert helper ───────────────────────────────────────────────────────

# def slack_alert(context: dict) -> None:
#     """Kirim notifikasi ke Slack saat task gagal. Skip jika webhook tidak dikonfigurasi."""
#     if not SLACK_WEBHOOK:
#         return

#     dag_id   = context["dag"].dag_id
#     task_id  = context["task_instance"].task_id
#     exec_dt  = context["ds"]
#     log_url  = context["task_instance"].log_url

#     message = (
#         f":x: *dbt task failed*\n"
#         f"*DAG*: `{dag_id}`\n"
#         f"*Task*: `{task_id}`\n"
#         f"*Execution date*: `{exec_dt}`\n"
#         f"<{log_url}|View logs>"
#     )

#     SlackWebhookOperator(
#         task_id="slack_alert_inline",
#         slack_webhook_conn_id="slack_webhook_default",
#         message=message,
#     ).execute(context)


# ─── Default args ─────────────────────────────────────────────────────────────

default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "email":             [os.getenv("ALERT_EMAIL", "data-engineering@bank.co.id")],
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    # "on_failure_callback": slack_alert,
}

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dbt_transformation",
    description=(
        "dbt BashOperator pipeline: source freshness → staging → marts → snapshots. "
        "Dijalankan setelah dag_core_banking dan dag_transactions selesai."
    ),
    schedule=CronDataIntervalTimetable("0 9 * * *", timezone="Asia/Jakarta"),
    start_date=pendulum.datetime(2026, 3, 11, tz="Asia/Jakarta"),
    catchup=True,                    # backfill historical runs
    max_active_runs=1,               # cegah concurrent mart rebuild
    tags=["dbt", "bigquery", "transformation", "daily"],
    default_args=default_args,
    doc_md=__doc__,
    params={
        "dbt_target":     DBT_TARGET,
        "full_refresh":   False,     # set True via Airflow UI untuk force rebuild
    },
) as dag:

    # ── Start ─────────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── 1. dbt deps ───────────────────────────────────────────────────────────
    # Install / update packages dari packages.yml.
    # Berjalan setiap DAG run agar packages selalu up-to-date.
    # Cepat jika packages sudah ter-install (dbt cek hash).
    task_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"{DBT_BIN}"
            f" deps"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
        env=TASK_ENV,
        doc_md=(
            "Install dbt packages dari `packages.yml` (dbt_utils, dbt_expectations, dll). "
            "Idempoten — aman dijalankan ulang."
        ),
    )

    # ── 2. Source freshness ───────────────────────────────────────────────────
    # Validasi bahwa raw_core_banking dan raw_transactions sudah ter-load
    # dalam window yang ditentukan di _sources.yml (warn: 25h, error: 49h).
    # Task ini BLOCKING — jika gagal, pipeline stop di sini.
    task_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"{DBT_BASE}"
            " source freshness"
            # Output ke file untuk debugging; Airflow juga capture stdout
            f" --output {DBT_PROJECT_DIR}/target/sources.json"
        ),
        env=TASK_ENV,
        doc_md=(
            "Validasi freshness sumber data di BigQuery. "
            "Gagal jika `updated_at` melebihi threshold di `_sources.yml`. "
            "**BLOCKING**: pipeline tidak lanjut jika raw data belum siap."
        ),
    )

    # ── 3a. Run staging — core_banking ────────────────────────────────────────
    # Materialize semua staging views untuk source core_banking (PostgreSQL).
    # Berjalan paralel dengan task_run_stg_txn.
    task_run_stg_core = BashOperator(
        task_id="dbt_run_staging_core_banking",
        bash_command=(
            f"{DBT_BASE}"
            " run"
            " --select staging.core_banking"
            " --vars '{{\"execution_date\": \"{{{{ ds }}}}\"}}'  "
            # --threads sudah dikonfigurasi di profiles.yml (prod: 16)
        ),
        env=TASK_ENV,
        doc_md=(
            "Run staging layer untuk source core_banking: "
            "`stg_customers`, `stg_accounts`, `stg_branches`, "
            "`stg_employees`, `stg_loan_applications`, `stg_credit_scores`."
        ),
    )

    # ── 3b. Run staging — transactions ────────────────────────────────────────
    # Materialize semua staging views untuk source transactions (MySQL).
    # Berjalan paralel dengan task_run_stg_core.
    task_run_stg_txn = BashOperator(
        task_id="dbt_run_staging_transactions",
        bash_command=(
            f"{DBT_BASE}"
            " run"
            " --select staging.transactions"
            " --vars '{{\"execution_date\": \"{{{{ ds }}}}\"}}'  "
        ),
        env=TASK_ENV,
        doc_md=(
            "Run staging layer untuk source transactions: "
            "`stg_transactions`, `stg_merchants`, `stg_fraud_flags`."
        ),
    )

    # ── 4. Test staging ───────────────────────────────────────────────────────
    # Jalankan semua dbt tests untuk staging layer.
    # --store-failures: simpan failed records ke BigQuery untuk investigasi.
    # Task ini BLOCKING — mart tidak boleh dibangun di atas data staging yang rusak.
    task_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"{DBT_BASE}"
            " test"
            " --select staging"
            " --store-failures"
            # Lanjut walaupun ada test warning (hanya error yang fail task)
        ),
        env=TASK_ENV,
        doc_md=(
            "Test seluruh staging layer: not_null, unique, accepted_values, "
            "relationships, dbt_utils.accepted_range. "
            "**BLOCKING**: mart tidak dijalankan jika staging test gagal. "
            "Failed records disimpan ke dataset `dbt_test_failures` di BigQuery."
        ),
    )

    # ── 5a. Run mart — customer domain ────────────────────────────────────────
    # mart_customer_360 dan mart_customer_lifetime_value.
    # Berjalan paralel dengan mart risk dan finance.
    task_run_mart_customer = BashOperator(
        task_id="dbt_run_mart_customer",
        bash_command=(
            f"{DBT_BASE}"
            " run"
            " --select marts.customer"
            " --vars '{{\"execution_date\": \"{{{{ ds }}}}\"}}'  "
        ),
        env=TASK_ENV,
        doc_md=(
            "Build customer domain marts: "
            "`mart_customer_360` (partisi harian, cluster segment+kyc), "
            "`mart_customer_lifetime_value` (RFM scoring)."
        ),
    )

    # ── 5b. Run mart — risk domain ────────────────────────────────────────────
    # mart_fraud_analytics dan mart_credit_risk.
    task_run_mart_risk = BashOperator(
        task_id="dbt_run_mart_risk",
        bash_command=(
            f"{DBT_BASE}"
            " run"
            " --select marts.risk"
            " --vars '{{\"execution_date\": \"{{{{ ds }}}}\"}}'  "
        ),
        env=TASK_ENV,
        doc_md=(
            "Build risk domain marts: "
            "`mart_fraud_analytics` (partisi harian, cluster severity+flag_type), "
            "`mart_credit_risk` (daily NPL snapshot, DPD buckets)."
        ),
    )

    # ── 5c. Run mart — finance domain ─────────────────────────────────────────
    # mart_transaction_analytics.
    task_run_mart_finance = BashOperator(
        task_id="dbt_run_mart_finance",
        bash_command=(
            f"{DBT_BASE}"
            " run"
            " --select marts.finance"
            " --vars '{{\"execution_date\": \"{{{{ ds }}}}\"}}'  "
        ),
        env=TASK_ENV,
        doc_md=(
            "Build finance domain mart: "
            "`mart_transaction_analytics` (daily aggregasi volume, revenue, channel). "
            "Partisi per `transaction_date`, cluster `channel` + `transaction_status`."
        ),
    )

    # ── 6. Test marts ─────────────────────────────────────────────────────────
    # Test semua mart models. TriggerRule.ALL_DONE = berjalan walaupun
    # salah satu mart gagal, agar kita tahu scope kegagalannya.
    # Non-blocking: gagalnya test mart tidak stop snapshot/docs.
    task_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"{DBT_BASE}"
            " test"
            " --select marts"
            " --store-failures"
        ),
        env=TASK_ENV,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md=(
            "Test seluruh mart layer. "
            "**Non-blocking** (ALL_DONE): berjalan meski ada mart yang gagal build, "
            "sehingga scope kegagalan terlihat jelas. "
            "Failed records disimpan ke `dbt_test_failures`."
        ),
    )

    # ── 7. Snapshots (SCD Type 2) ─────────────────────────────────────────────
    # snap_customers: track perubahan segment, kyc_status, risk_rating.
    # TriggerRule.ALL_DONE: snapshot tetap jalan meski mart test ada yang gagal.
    task_snapshots = BashOperator(
        task_id="dbt_run_snapshots",
        bash_command=(
            f"{DBT_BASE}"
            " snapshot"
        ),
        env=TASK_ENV,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md=(
            "SCD Type 2 snapshot untuk `snap_customers`. "
            "Track perubahan `customer_segment`, `kyc_status`, `risk_rating`. "
            "Berjalan setelah mart test selesai (ALL_DONE)."
        ),
    )

    # ── 8. Generate docs ──────────────────────────────────────────────────────
    # Generate manifest.json + catalog.json untuk dbt docs.
    # TriggerRule.ALL_DONE: docs tetap di-generate walaupun ada kegagalan
    # di upstream, agar pipeline metadata tetap terupdate.
    task_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=(
            f"{DBT_BASE}"
            " docs generate"
            # --no-compile: skip re-compile, gunakan manifest yang sudah ada
            " --no-compile"
        ),
        env=TASK_ENV,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md=(
            "Generate `manifest.json` dan `catalog.json` untuk dbt documentation. "
            "Output di `{DBT_PROJECT_DIR}/target/`. "
            "Berjalan selalu (ALL_DONE) agar metadata pipeline selalu fresh."
        ),
    )

    # ── End ───────────────────────────────────────────────────────────────────
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    #
    # start
    #   │
    # dbt_deps
    #   │
    # dbt_source_freshness          ← BLOCKING
    #   │
    #   ├── dbt_run_staging_core_banking ─┐
    #   └── dbt_run_staging_transactions  ┘  (paralel)
    #                   │
    #             dbt_test_staging          ← BLOCKING
    #                   │
    #   ┌───────────────┼───────────────┐
    #   dbt_run_mart_   dbt_run_mart_   dbt_run_mart_   (paralel)
    #   customer        risk            finance
    #   └───────────────┼───────────────┘
    #                   │
    #             dbt_test_marts            (ALL_DONE, non-blocking)
    #                   │
    #             dbt_run_snapshots         (ALL_DONE)
    #                   │
    #             dbt_generate_docs         (ALL_DONE)
    #                   │
    #                  end

    start >> task_deps >> task_source_freshness

    task_source_freshness >> [task_run_stg_core, task_run_stg_txn]

    [task_run_stg_core, task_run_stg_txn] >> task_test_staging

    task_test_staging >> [task_run_mart_customer, task_run_mart_risk, task_run_mart_finance]

    [task_run_mart_customer, task_run_mart_risk, task_run_mart_finance] >> task_test_marts

    task_test_marts >> task_snapshots >> task_docs >> end