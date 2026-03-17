"""
Banking Data Platform — Synthetic Data Generator
================================================
Generates realistic banking data for:
  - PostgreSQL: Core Banking (customers, accounts, branches, employees, loans)
  - MySQL: Transaction System (transactions, merchants, fraud_flags)

Usage:
  python main.py --mode full
      # Full initial load, data historis berakhir di FULL_START_DATE (default 2026-03-01)

  python main.py --mode full --start-date 2026-03-01
      # Full load dengan anchor date custom

  python main.py --mode incremental
      # Incremental load simulasi 1 hari, menggunakan INCREMENTAL_DATE (default = start_date + 1)

  python main.py --mode incremental --incremental-date 2026-03-05
      # Incremental load di tanggal custom tanpa harus menunggu hari berikutnya

  python main.py --mode truncate      # Truncate and reload

Semua timestamp menggunakan zona waktu WIB (UTC+7 / Asia/Jakarta).
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

from config.settings import Settings

WIB = ZoneInfo("Asia/Jakarta")
from generators.core_banking.branches import BranchGenerator
from generators.core_banking.employees import EmployeeGenerator
from generators.core_banking.customers import CustomerGenerator
from generators.core_banking.accounts import AccountGenerator
from generators.core_banking.loan_applications import LoanApplicationGenerator
from generators.transaction.merchants import MerchantGenerator
from generators.transaction.transactions import TransactionGenerator
from generators.transaction.fraud_flags import FraudFlagGenerator
from generators.db_connections import get_postgres_conn, get_mysql_conn

# ─── Logging Setup (WIB timezone) ─────────────────────────
class _WIBFormatter(logging.Formatter):
    """Format log timestamps in WIB (Asia/Jakarta, UTC+7)."""
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=WIB)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S %Z")


_handler_stdout = logging.StreamHandler(sys.stdout)
_handler_file   = logging.FileHandler("/tmp/data-generator.log")
_fmt = _WIBFormatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
_handler_stdout.setFormatter(_fmt)
_handler_file.setFormatter(_fmt)

logging.basicConfig(level=logging.INFO, handlers=[_handler_stdout, _handler_file])
logger = logging.getLogger("banking-generator")


def run_full_load(settings: Settings, anchor_date: date, range_start: date):
    """Initial full data generation for all tables.
    
    anchor_date  : tanggal AKHIR range data (misal 2025-12-31)
    range_start  : tanggal AWAL range data (misal 2025-01-01)
    Semua data transaksional akan tersebar merata di [range_start, anchor_date].
    """
    start = time.time()
    logger.info("=" * 60)
    logger.info("Starting FULL LOAD data generation")
    logger.info(f"  Range:        {range_start.isoformat()} s/d {anchor_date.isoformat()} (WIB)")
    logger.info(f"  Customers:    {settings.num_customers:,}")
    logger.info(f"  Transactions: {settings.num_transactions:,}")
    logger.info(f"  Seed:         {settings.seed}")
    logger.info("=" * 60)

    pg_conn = get_postgres_conn(settings)
    mysql_conn = get_mysql_conn(settings)

    try:
        logger.info("📦 [Postgres] Generating reference data...")
        branch_gen = BranchGenerator(pg_conn, settings)
        branch_ids = branch_gen.generate(count=settings.num_branches, range_end=anchor_date)
        logger.info(f"  ✅ Branches: {len(branch_ids)}")

        employee_gen = EmployeeGenerator(pg_conn, settings)
        employee_ids = employee_gen.generate(branch_ids=branch_ids, count=settings.num_employees, range_end=anchor_date)
        logger.info(f"  ✅ Employees: {len(employee_ids)}")

        logger.info("👥 [Postgres] Generating customers...")
        customer_gen = CustomerGenerator(pg_conn, settings)
        customer_ids = customer_gen.generate(
            count=settings.num_customers,
            branch_ids=branch_ids,
            range_start=range_start,
            range_end=anchor_date,
        )
        logger.info(f"  ✅ Customers: {len(customer_ids)}")

        logger.info("🏦 [Postgres] Generating accounts...")
        account_gen = AccountGenerator(pg_conn, settings)
        account_ids, account_customer_map = account_gen.generate(
            customer_ids=customer_ids,
            branch_ids=branch_ids,
            range_start=range_start,
            range_end=anchor_date,
        )
        logger.info(f"  ✅ Accounts: {len(account_ids)}")

        logger.info("💳 [Postgres] Generating loan applications...")
        loan_gen = LoanApplicationGenerator(pg_conn, settings)
        loan_ids = loan_gen.generate(
            customer_ids=customer_ids,
            branch_ids=branch_ids,
            employee_ids=employee_ids,
            range_start=range_start,
            range_end=anchor_date,
        )
        logger.info(f"  ✅ Loan Applications: {len(loan_ids)}")

        # ── MySQL Transaction System ─────────────────────
        logger.info("🏪 [MySQL] Generating merchants...")
        merchant_gen = MerchantGenerator(mysql_conn, settings)
        merchant_ids = merchant_gen.generate(count=settings.num_merchants, range_end=anchor_date)
        logger.info(f"  ✅ Merchants: {len(merchant_ids)}")

        logger.info("💸 [MySQL] Generating transactions...")
        txn_gen = TransactionGenerator(mysql_conn, settings)
        transaction_ids = txn_gen.generate(
            account_ids=account_ids,
            account_customer_map=account_customer_map,
            merchant_ids=merchant_ids,
            count=settings.num_transactions,
            range_start=range_start,
            range_end=anchor_date,
        )
        logger.info(f"  ✅ Transactions: {len(transaction_ids)}")

        logger.info("🚨 [MySQL] Generating fraud flags...")
        fraud_gen = FraudFlagGenerator(mysql_conn, settings)
        fraud_ids = fraud_gen.generate(
            transaction_ids=transaction_ids,
            account_customer_map=account_customer_map,
            fraud_rate=0.008,
            range_end=anchor_date,
        )
        logger.info(f"  ✅ Fraud Flags: {len(fraud_ids)}")

    finally:
        pg_conn.close()
        mysql_conn.close()

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(f"✅ Full load complete in {elapsed:.1f}s")
    logger.info("=" * 60)


def run_incremental_load(settings: Settings, incremental_date: date):
    """Simulate a daily incremental data load.
    
    incremental_date: tanggal yang disimulasikan sebagai 'hari ini'.
    Semua data baru (customers, transactions, loans) akan ber-timestamp
    di tanggal ini. Tidak perlu menunggu hari berikutnya untuk test.
    """
    logger.info(f"📈 Starting INCREMENTAL LOAD — simulating date: {incremental_date.isoformat()} (WIB)")

    pg_conn = get_postgres_conn(settings)
    mysql_conn = get_mysql_conn(settings)

    try:
        # Fetch existing IDs
        with pg_conn.cursor() as cur:
            cur.execute("SELECT customer_id FROM customers WHERE deleted_at IS NULL ORDER BY RANDOM() LIMIT 1000")
            customer_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT branch_id FROM branches WHERE is_active = true")
            branch_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT employee_id FROM employees WHERE is_active = true")
            employee_ids = [r[0] for r in cur.fetchall()]

        with mysql_conn.cursor() as cur:
            cur.execute("SELECT merchant_id FROM merchants WHERE is_active = 1")
            merchant_ids = [r[0] for r in cur.fetchall()]
            cur.execute("""
                SELECT account_id, customer_id
                FROM (
                    SELECT DISTINCT account_id, customer_id
                    FROM transactions
                    ORDER BY RAND() LIMIT 5000
                ) t
            """)
            rows = cur.fetchall()
            account_ids = [r[0] for r in rows]
            account_customer_map = {r[0]: r[1] for r in rows}

        # New customers (small batch)
        new_customer_count = max(10, settings.num_customers // 100)
        customer_gen = CustomerGenerator(pg_conn, settings)
        new_customer_ids = customer_gen.generate(
            count=new_customer_count,
            branch_ids=branch_ids,
            range_start=incremental_date,
            range_end=incremental_date,
        )
        logger.info(f"  New customers: {len(new_customer_ids)}")

        # New loan applications
        loan_gen = LoanApplicationGenerator(pg_conn, settings)
        new_loans = loan_gen.generate(
            customer_ids=customer_ids[:200],
            branch_ids=branch_ids,
            employee_ids=employee_ids,
            count_override=50,
            range_start=incremental_date,
            range_end=incremental_date,
        )
        logger.info(f"  New loan applications: {len(new_loans)}")

        # Daily transactions (10% of total)
        daily_txn_count = max(1000, settings.num_transactions // 30)
        txn_gen = TransactionGenerator(mysql_conn, settings)
        new_txn_ids = txn_gen.generate(
            account_ids=account_ids,
            account_customer_map=account_customer_map,
            merchant_ids=merchant_ids,
            count=daily_txn_count,
            range_start=incremental_date,
            range_end=incremental_date,
        )
        logger.info(f"  New transactions: {len(new_txn_ids)}")

        # Fraud flags
        fraud_gen = FraudFlagGenerator(mysql_conn, settings)
        fraud_gen.generate(
            transaction_ids=new_txn_ids,
            account_customer_map=account_customer_map,
            fraud_rate=0.008,
            range_end=incremental_date,
        )

    finally:
        pg_conn.close()
        mysql_conn.close()

    logger.info("✅ Incremental load complete")


def main():
    parser = argparse.ArgumentParser(
        description="Banking Data Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Contoh penggunaan:
  # Full load dengan anchor date default (2026-03-01)
  python main.py --mode full

  # Full load dengan anchor date custom
  python main.py --mode full --start-date 2026-03-01

  # Incremental load simulasi tanggal custom (tidak perlu tunggu besok)
  python main.py --mode incremental --incremental-date 2026-03-05

  # Bisa juga pakai env var
  FULL_START_DATE=2026-03-01 INCREMENTAL_DATE=2026-03-05 python main.py --mode incremental
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental", "truncate"],
        default="full",
        help="Generation mode",
    )
    parser.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        help="Tanggal AKHIR range full load (override FULL_START_DATE env). "
             "Default: 2025-12-31",
    )
    parser.add_argument(
        "--range-start",
        metavar="YYYY-MM-DD",
        help="Tanggal AWAL range full load (override FULL_RANGE_START env). "
             "Default: 2025-01-01",
    )
    parser.add_argument(
        "--incremental-date",
        metavar="YYYY-MM-DD",
        help="Tanggal yang disimulasikan untuk incremental load (override INCREMENTAL_DATE env). "
             "Default: start-date + 1 hari",
    )
    args = parser.parse_args()

    settings = Settings()

    # ── Resolve dates ────────────────────────────────────────────────────
    from datetime import timedelta

    if args.start_date:
        try:
            from datetime import datetime as _dt
            anchor_date = _dt.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError:
            parser.error(f"--start-date format tidak valid: '{args.start_date}'. Gunakan YYYY-MM-DD.")
    else:
        anchor_date = settings.full_start_date  # default: 2025-12-31

    if args.range_start:
        try:
            from datetime import datetime as _dt
            range_start = _dt.strptime(args.range_start, "%Y-%m-%d").date()
        except ValueError:
            parser.error(f"--range-start format tidak valid: '{args.range_start}'. Gunakan YYYY-MM-DD.")
    else:
        range_start = settings.full_range_start  # default: 2025-01-01

    if args.incremental_date:
        try:
            from datetime import datetime as _dt
            incremental_date = _dt.strptime(args.incremental_date, "%Y-%m-%d").date()
        except ValueError:
            parser.error(f"--incremental-date format tidak valid: '{args.incremental_date}'. Gunakan YYYY-MM-DD.")
    else:
        incremental_date = settings.incremental_date or (anchor_date + timedelta(days=1))

    # ── Run ──────────────────────────────────────────────────────────────
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Postgres: {settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}")
    logger.info(f"MySQL:    {settings.mysql_host}:{settings.mysql_port}/{settings.mysql_db}")
    logger.info(f"Timezone: WIB (Asia/Jakarta, UTC+7)")

    if args.mode == "full":
        run_full_load(settings, anchor_date=anchor_date, range_start=range_start)
    elif args.mode == "incremental":
        run_incremental_load(settings, incremental_date=incremental_date)
    elif args.mode == "truncate":
        logger.warning("⚠️  Truncate mode: dropping all generated data...")
        raise NotImplementedError("Truncate mode not yet implemented")


if __name__ == "__main__":
    main()
