"""
Banking Data Platform — Synthetic Data Generator
================================================
Generates realistic banking data for:
  - PostgreSQL: Core Banking (customers, accounts, branches, employees, loans)
  - MySQL: Transaction System (transactions, merchants, fraud_flags)

Usage:
  python main.py --mode full          # Full initial load
  python main.py --mode incremental   # Daily incremental simulation
  python main.py --mode truncate      # Truncate and reload
"""

import argparse
import logging
import sys
import time
from datetime import datetime

from config.settings import Settings
from generators.core_banking.branches import BranchGenerator
from generators.core_banking.employees import EmployeeGenerator
from generators.core_banking.customers import CustomerGenerator
from generators.core_banking.accounts import AccountGenerator
from generators.core_banking.loan_applications import LoanApplicationGenerator
from generators.transaction.merchants import MerchantGenerator
from generators.transaction.transactions import TransactionGenerator
from generators.transaction.fraud_flags import FraudFlagGenerator
from generators.db_connections import get_postgres_conn, get_mysql_conn

# ─── Logging Setup ────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/data-generator.log"),
    ],
)
logger = logging.getLogger("banking-generator")


def run_full_load(settings: Settings):
    """Initial full data generation for all tables."""
    start = time.time()
    logger.info("=" * 60)
    logger.info("Starting FULL LOAD data generation")
    logger.info(f"  Customers:    {settings.num_customers:,}")
    logger.info(f"  Transactions: {settings.num_transactions:,}")
    logger.info(f"  Seed:         {settings.seed}")
    logger.info("=" * 60)

    pg_conn = get_postgres_conn(settings)
    mysql_conn = get_mysql_conn(settings)

    try:
        # ── PostgreSQL Core Banking ──────────────────────
        logger.info("📦 [Postgres] Generating reference data...")
        branch_gen = BranchGenerator(pg_conn, settings)
        branch_ids = branch_gen.generate(count=settings.num_branches)
        logger.info(f"  ✅ Branches: {len(branch_ids)}")

        employee_gen = EmployeeGenerator(pg_conn, settings)
        employee_ids = employee_gen.generate(branch_ids=branch_ids, count=settings.num_employees)
        logger.info(f"  ✅ Employees: {len(employee_ids)}")

        logger.info("👥 [Postgres] Generating customers...")
        customer_gen = CustomerGenerator(pg_conn, settings)
        customer_ids = customer_gen.generate(
            count=settings.num_customers,
            branch_ids=branch_ids,
        )
        logger.info(f"  ✅ Customers: {len(customer_ids)}")

        logger.info("🏦 [Postgres] Generating accounts...")
        account_gen = AccountGenerator(pg_conn, settings)
        account_ids, account_customer_map = account_gen.generate(
            customer_ids=customer_ids,
            branch_ids=branch_ids,
        )
        logger.info(f"  ✅ Accounts: {len(account_ids)}")

        logger.info("💳 [Postgres] Generating loan applications...")
        loan_gen = LoanApplicationGenerator(pg_conn, settings)
        loan_ids = loan_gen.generate(
            customer_ids=customer_ids,
            branch_ids=branch_ids,
            employee_ids=employee_ids,
        )
        logger.info(f"  ✅ Loan Applications: {len(loan_ids)}")

        # ── MySQL Transaction System ─────────────────────
        logger.info("🏪 [MySQL] Generating merchants...")
        merchant_gen = MerchantGenerator(mysql_conn, settings)
        merchant_ids = merchant_gen.generate(count=settings.num_merchants)
        logger.info(f"  ✅ Merchants: {len(merchant_ids)}")

        logger.info("💸 [MySQL] Generating transactions...")
        txn_gen = TransactionGenerator(mysql_conn, settings)
        transaction_ids = txn_gen.generate(
            account_ids=account_ids,
            account_customer_map=account_customer_map,
            merchant_ids=merchant_ids,
            count=settings.num_transactions,
        )
        logger.info(f"  ✅ Transactions: {len(transaction_ids)}")

        logger.info("🚨 [MySQL] Generating fraud flags...")
        fraud_gen = FraudFlagGenerator(mysql_conn, settings)
        fraud_ids = fraud_gen.generate(
            transaction_ids=transaction_ids,
            account_customer_map=account_customer_map,
            fraud_rate=0.008,  # 0.8% fraud rate — realistic for banking
        )
        logger.info(f"  ✅ Fraud Flags: {len(fraud_ids)}")

    finally:
        pg_conn.close()
        mysql_conn.close()

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(f"✅ Full load complete in {elapsed:.1f}s")
    logger.info("=" * 60)


def run_incremental_load(settings: Settings):
    """Simulate a daily incremental data load."""
    logger.info("📈 Starting INCREMENTAL LOAD (simulating 1 business day)")

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
        new_customer_ids = customer_gen.generate(count=new_customer_count, branch_ids=branch_ids)
        logger.info(f"  New customers: {len(new_customer_ids)}")

        # New loan applications
        loan_gen = LoanApplicationGenerator(pg_conn, settings)
        new_loans = loan_gen.generate(
            customer_ids=customer_ids[:200],
            branch_ids=branch_ids,
            employee_ids=employee_ids,
            count_override=50,
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
            date_range_days=1,
        )
        logger.info(f"  New transactions: {len(new_txn_ids)}")

        # Fraud flags
        fraud_gen = FraudFlagGenerator(mysql_conn, settings)
        fraud_gen.generate(
            transaction_ids=new_txn_ids,
            account_customer_map=account_customer_map,
            fraud_rate=0.008,
        )

    finally:
        pg_conn.close()
        mysql_conn.close()

    logger.info("✅ Incremental load complete")


def main():
    parser = argparse.ArgumentParser(description="Banking Data Generator")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental", "truncate"],
        default="full",
        help="Generation mode",
    )
    args = parser.parse_args()

    settings = Settings()
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Postgres: {settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}")
    logger.info(f"MySQL:    {settings.mysql_host}:{settings.mysql_port}/{settings.mysql_db}")

    if args.mode == "full":
        run_full_load(settings)
    elif args.mode == "incremental":
        run_incremental_load(settings)
    elif args.mode == "truncate":
        logger.warning("⚠️  Truncate mode: dropping all generated data...")
        # Implement truncate if needed
        raise NotImplementedError("Truncate mode not yet implemented")


if __name__ == "__main__":
    main()
