"""
Configuration settings for the data generator.
Values are loaded from environment variables.

Timezone: semua timestamp default menggunakan WIB (Asia/Jakarta, UTC+7).
"""

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

WIB = ZoneInfo("Asia/Jakarta")


def _parse_date_env(env_key: str) -> date | None:
    """Parse YYYY-MM-DD dari env var. Return None jika tidak di-set."""
    raw = os.getenv(env_key, "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(
            f"Format tanggal tidak valid untuk env {env_key}='{raw}'. "
            f"Gunakan format YYYY-MM-DD (contoh: 2026-03-01)."
        )


def _now_wib() -> datetime:
    return datetime.now(tz=WIB)


class Settings:
    def __init__(self):
        # PostgreSQL Core Banking
        self.postgres_host: str     = os.getenv("POSTGRES_HOST", "localhost")
        self.postgres_port: int     = int(os.getenv("POSTGRES_PORT", "5432"))
        self.postgres_db: str       = os.getenv("POSTGRES_DB", "core_banking")
        self.postgres_user: str     = os.getenv("POSTGRES_USER", "banking_core")
        self.postgres_password: str = os.getenv("POSTGRES_PASSWORD", "")

        # MySQL Transaction System
        self.mysql_host: str        = os.getenv("MYSQL_HOST", "localhost")
        self.mysql_port: int        = int(os.getenv("MYSQL_PORT", "3306"))
        self.mysql_db: str          = os.getenv("MYSQL_DB", "transaction_db")
        self.mysql_user: str        = os.getenv("MYSQL_USER", "banking_txn")
        self.mysql_password: str    = os.getenv("MYSQL_PASSWORD", "")

        # Generation parameters
        self.seed: int              = int(os.getenv("SEED", "42"))
        self.num_customers: int     = int(os.getenv("NUM_CUSTOMERS", "10000"))
        self.num_accounts: int      = int(os.getenv("NUM_ACCOUNTS", "15000"))
        self.num_transactions: int  = int(os.getenv("NUM_TRANSACTIONS", "100000"))
        self.num_merchants: int     = int(os.getenv("NUM_MERCHANTS", "500"))
        self.num_branches: int      = int(os.getenv("NUM_BRANCHES", "50"))
        self.num_employees: int     = int(os.getenv("NUM_EMPLOYEES", "200"))
        self.batch_size: int        = int(os.getenv("BATCH_SIZE", "500"))

        # ── Custom date range ───────────────────────────────────────────────
        # FULL_START_DATE   : anchor end-date untuk full load.
        #                     Default: 2026-03-01
        # INCREMENTAL_DATE  : tanggal simulasi untuk incremental load.
        #                     Default: FULL_START_DATE + 1 hari
        # FULL_START_DATE = tanggal AKHIR range full load (default: 2025-12-31)
        _full = _parse_date_env("FULL_START_DATE")
        self.full_start_date: date = _full or date(2025, 12, 31)

        # FULL_RANGE_START = tanggal AWAL range full load (default: 2025-01-01)
        _full_range = _parse_date_env("FULL_RANGE_START")
        self.full_range_start: date = _full_range or date(2025, 1, 1)

        # INCREMENTAL_DATE = tanggal simulasi incremental (default: 2026-01-02)
        _incr = _parse_date_env("INCREMENTAL_DATE")
        self.incremental_date: date = _incr or date(2026, 1, 2)
