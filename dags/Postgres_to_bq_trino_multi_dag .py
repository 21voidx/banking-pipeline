"""
DAG : postgres_to_bq_trino_multi_table_dag
Airflow : 3.x
Engine  : Trino + BigQuery EXTERNAL_QUERY (no GCS)

Generates one isolated TaskGroup per table from TABLE_CONFIGS.
Each group is fully independent — a failure in one table does not block others.

Best practices
──────────────
  time_partitioning    — DAY partition on each table's event column
  cluster_fields       — per-table cluster spec
  write_disposition    — WRITE_TRUNCATE on temp, WRITE_EMPTY on final (MERGE controls writes)
  create_disposition   — CREATE_NEVER (table must exist before INSERT)
  schema_update_options— ALLOW_FIELD_ADDITION + ALLOW_FIELD_RELAXATION
  outlets              — per-table Dataset assets for lineage
  idempotency_token    — job_id = dag_id__task__ds_nodash (deterministic, retry-safe)
  deduplicate          — ROW_NUMBER() in INSERT + QUALIFY guard in MERGE source
  deferrable=True      — async operators; workers freed while BQ job runs
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.utils.task_group import TaskGroup

# ══════════════════════════════════════════════════════════════════════════════
#  GLOBAL CONFIG
# ══════════════════════════════════════════════════════════════════════════════

TRINO_CONN_ID    = "trino_default"
GCP_CONN_ID      = "google_cloud_default"

TRINO_BQ_CATALOG = "bigquery"
TRINO_PG_CATALOG = "postgresql"

BQ_PROJECT       = "your-gcp-project"           # ← change
BQ_DATASET       = "your_dataset"               # ← change
BQ_LOCATION      = "US"

BQ_EXTERNAL_CONN = "projects/your-project/locations/us/connections/pg-conn"  # ← change

# ══════════════════════════════════════════════════════════════════════════════
#  TABLE CONFIGS  — add one entry per source table
# ══════════════════════════════════════════════════════════════════════════════

TABLE_CONFIGS: list[dict[str, Any]] = [
    {
        "table_id":       "users",
        "pg_schema":      "public",
        "pg_table":       "users",
        "merge_key":      "id",
        "partition_field": "updated_at",
        "cluster_fields": ["status", "country"],
        "schema": [
            {"name": "id",         "type": "INTEGER",   "mode": "REQUIRED"},
            {"name": "name",       "type": "STRING",    "mode": "NULLABLE"},
            {"name": "email",      "type": "STRING",    "mode": "NULLABLE"},
            {"name": "status",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "country",    "type": "STRING",    "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
    },
    {
        "table_id":       "orders",
        "pg_schema":      "public",
        "pg_table":       "orders",
        "merge_key":      "order_id",
        "partition_field": "order_date",
        "cluster_fields": ["status", "user_id"],
        "schema": [
            {"name": "order_id",   "type": "INTEGER",   "mode": "REQUIRED"},
            {"name": "user_id",    "type": "INTEGER",   "mode": "NULLABLE"},
            {"name": "amount",     "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "currency",   "type": "STRING",    "mode": "NULLABLE"},
            {"name": "status",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "order_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
    },
    {
        "table_id":       "products",
        "pg_schema":      "public",
        "pg_table":       "products",
        "merge_key":      "product_id",
        "partition_field": "updated_at",
        "cluster_fields": ["category"],
        "schema": [
            {"name": "product_id",  "type": "INTEGER",  "mode": "REQUIRED"},
            {"name": "name",        "type": "STRING",   "mode": "NULLABLE"},
            {"name": "category",    "type": "STRING",   "mode": "NULLABLE"},
            {"name": "price",       "type": "NUMERIC",  "mode": "NULLABLE"},
            {"name": "is_active",   "type": "BOOLEAN",  "mode": "NULLABLE"},
            {"name": "created_at",  "type": "TIMESTAMP","mode": "NULLABLE"},
            {"name": "updated_at",  "type": "TIMESTAMP","mode": "NULLABLE"},
        ],
    },
]

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _bq_type_to_trino(bq_type: str) -> str:
    return {
        "INTEGER":   "BIGINT",
        "STRING":    "VARCHAR",
        "FLOAT":     "DOUBLE",
        "NUMERIC":   "DECIMAL(38,9)",
        "BOOLEAN":   "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP(6) WITH TIME ZONE",
        "DATE":      "DATE",
        "BYTES":     "VARBINARY",
        "JSON":      "JSON",
    }.get(bq_type, "VARCHAR")


def _trino_create_temp_ddl(cfg: dict) -> str:
    """
    Trino DDL — drop-and-recreate the BQ temp table with partitioning + clustering.
    Fully idempotent: safe to rerun at any time.
    """
    tbl             = cfg["table_id"]
    partition_field = cfg["partition_field"]
    cluster_fields  = cfg["cluster_fields"]
    col_defs        = ",\n    ".join(
        f"{f['name']}  {_bq_type_to_trino(f['type'])}" for f in cfg["schema"]
    )
    partition_spec  = f"ARRAY['DAY({partition_field})']"
    clustering_spec = f"ARRAY[{', '.join(repr(f) for f in cluster_fields)}]"

    return f"""
DROP TABLE IF EXISTS {TRINO_BQ_CATALOG}.{BQ_DATASET}.{tbl}_temp;

CREATE TABLE {TRINO_BQ_CATALOG}.{BQ_DATASET}.{tbl}_temp (
    {col_defs}
)
WITH (
    partitioning    = {partition_spec},
    clustering_key  = {clustering_spec}
)
"""


def _bq_insert_job_config(cfg: dict) -> dict:
    """
    BigQuery INSERT job config.
    Uses EXTERNAL_QUERY to federate into Postgres — no GCS hop.
    ROW_NUMBER() deduplicates at read time before inserting into temp.
    """
    tbl             = cfg["table_id"]
    pg_schema       = cfg["pg_schema"]
    pg_table        = cfg["pg_table"]
    merge_key       = cfg["merge_key"]
    partition_field = cfg["partition_field"]
    cluster_fields  = cfg["cluster_fields"]
    columns         = [f["name"] for f in cfg["schema"]]
    cols_str        = ", ".join(columns)
    src_cols_str    = ", ".join(f"src.{c}" for c in columns)

    # The SQL string passed to EXTERNAL_QUERY must be a Postgres SQL literal.
    # Airflow {{ ds }} / {{ next_ds }} macros are resolved at runtime.
    external_sql = (
        f"SELECT {cols_str} "
        f"FROM {pg_schema}.{pg_table} "
        f"WHERE {partition_field} >= ''{{{{ ds }}}}'' "
        f"  AND {partition_field} <  ''{{{{ next_ds }}}}''"
    ).replace("''", "'")

    return {
        "query": {
            "query": f"""
                INSERT INTO `{BQ_PROJECT}.{BQ_DATASET}.{tbl}_temp`
                ({cols_str})

                WITH ranked AS (
                    SELECT
                        {src_cols_str},
                        ROW_NUMBER() OVER (
                            PARTITION BY src.{merge_key}
                            ORDER BY src.{partition_field} DESC
                        ) AS _rn
                    FROM EXTERNAL_QUERY(
                        '{BQ_EXTERNAL_CONN}',
                        '{external_sql}'
                    ) AS src
                )
                SELECT {cols_str}
                FROM   ranked
                WHERE  _rn = 1
            """,
            "useLegacySql": False,
            "writeDisposition":    "WRITE_TRUNCATE",
            "createDisposition":   "CREATE_NEVER",
            "schemaUpdateOptions": [
                "ALLOW_FIELD_ADDITION",
                "ALLOW_FIELD_RELAXATION",
            ],
            "timePartitioning": {
                "type":  "DAY",
                "field": partition_field,
            },
            "clustering": {
                "fields": cluster_fields,
            },
            "defaultDataset": {
                "projectId": BQ_PROJECT,
                "datasetId": BQ_DATASET,
            },
        }
    }


def _bq_merge_job_config(cfg: dict) -> dict:
    """
    BigQuery MERGE job config.
    Updates only when incoming row is newer (prevents out-of-order clobber).
    Second ROW_NUMBER inside MERGE source guards against any remaining dupes.
    """
    tbl             = cfg["table_id"]
    merge_key       = cfg["merge_key"]
    partition_field = cfg["partition_field"]
    columns         = [f["name"] for f in cfg["schema"]]
    non_key_cols    = [c for c in columns if c != merge_key]
    cols_str        = ", ".join(columns)

    set_clause  = ",\n            ".join(f"T.{c} = S.{c}" for c in non_key_cols)
    ins_vals    = ", ".join(f"S.{c}" for c in columns)

    return {
        "query": {
            "query": f"""
                MERGE `{BQ_PROJECT}.{BQ_DATASET}.{tbl}` AS T
                USING (
                    SELECT * EXCEPT(_rn)
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY {merge_key}
                                ORDER BY {partition_field} DESC
                            ) AS _rn
                        FROM `{BQ_PROJECT}.{BQ_DATASET}.{tbl}_temp`
                    )
                    WHERE _rn = 1
                ) AS S
                ON T.{merge_key} = S.{merge_key}

                WHEN MATCHED
                    AND S.{partition_field} > T.{partition_field}
                THEN UPDATE SET
                    {set_clause}

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({cols_str})
                    VALUES ({ins_vals})
            """,
            "useLegacySql": False,
            "writeDisposition":    "WRITE_EMPTY",       # MERGE manages writes
            "createDisposition":   "CREATE_NEVER",
            "schemaUpdateOptions": [
                "ALLOW_FIELD_ADDITION",
                "ALLOW_FIELD_RELAXATION",
            ],
            "defaultDataset": {
                "projectId": BQ_PROJECT,
                "datasetId": BQ_DATASET,
            },
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
#  DAG
# ══════════════════════════════════════════════════════════════════════════════

default_args = {
    "owner":                     "data-engineering",
    "depends_on_past":           False,
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(hours=2),
}

with DAG(
    dag_id="postgres_to_bq_trino_multi",
    description="Multi-table Postgres → BigQuery via Trino + EXTERNAL_QUERY",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["postgres", "bigquery", "trino", "ingestion", "multi-table"],
    doc_md=__doc__,
) as dag:

    for cfg in TABLE_CONFIGS:
        tbl = cfg["table_id"]

        # Per-table Dataset assets for Airflow 3.x lineage tracking
        temp_asset  = Dataset(f"bigquery://{BQ_PROJECT}/{BQ_DATASET}/{tbl}_temp")
        final_asset = Dataset(f"bigquery://{BQ_PROJECT}/{BQ_DATASET}/{tbl}")

        with TaskGroup(group_id=tbl) as tg:

            # ── 1. CREATE temp table via Trino DDL ─────────────────────────
            create_temp = SQLExecuteQueryOperator(
                task_id="create_bq_temp",
                conn_id=TRINO_CONN_ID,
                sql=_trino_create_temp_ddl(cfg),
                autocommit=True,
                outlets=[temp_asset],
                doc_md=f"Creates BQ temp table for {tbl} with partition + cluster via Trino DDL.",
            )

            # ── 2. INSERT Postgres → BQ temp via EXTERNAL_QUERY ───────────
            insert_temp = BigQueryInsertJobOperator(
                task_id="insert_to_bq_temp",
                gcp_conn_id=GCP_CONN_ID,
                project_id=BQ_PROJECT,
                location=BQ_LOCATION,
                configuration=_bq_insert_job_config(cfg),
                # Idempotency: same DAG run always generates same job_id
                job_id=f"{{{{ dag.dag_id }}}}__{tbl}__insert__{{{{ ds_nodash }}}}",
                force_rerun=False,
                deferrable=True,               # async — frees Airflow worker slot
                outlets=[temp_asset],
                doc_md=(
                    f"Federated INSERT from Postgres into BQ temp for {tbl}. "
                    "Deduplicates with ROW_NUMBER() before insert."
                ),
            )

            # ── 3. MERGE temp → final table ───────────────────────────────
            merge_final = BigQueryInsertJobOperator(
                task_id="merge_to_final",
                gcp_conn_id=GCP_CONN_ID,
                project_id=BQ_PROJECT,
                location=BQ_LOCATION,
                configuration=_bq_merge_job_config(cfg),
                job_id=f"{{{{ dag.dag_id }}}}__{tbl}__merge__{{{{ ds_nodash }}}}",
                force_rerun=False,
                deferrable=True,
                outlets=[final_asset],
                doc_md=(
                    f"MERGE (UPSERT) from temp into {tbl}. "
                    "Only updates rows where incoming updated_at is newer."
                ),
            )

            # ── 4. DROP temp table ────────────────────────────────────────
            drop_temp = BigQueryDeleteTableOperator(
                task_id="drop_bq_temp",
                gcp_conn_id=GCP_CONN_ID,
                deletion_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{tbl}_temp",
                ignore_if_missing=True,
                trigger_rule="all_done",       # clean up even on upstream failure
                doc_md=f"Drops {tbl}_temp. Runs regardless of upstream success/failure.",
            )

            create_temp >> insert_temp >> merge_final >> drop_temp