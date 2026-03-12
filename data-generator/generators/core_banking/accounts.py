"""
Account data generator for PostgreSQL core_banking.
Generates realistic banking accounts linked to customers.
Returns (account_ids, account_customer_map) as expected by main.py.
"""
import logging
import random
from datetime import date, timedelta
from typing import Dict, List, Tuple

import psycopg2.extras
from faker import Faker

from config.settings import Settings

logger = logging.getLogger(__name__)

fake = Faker("id_ID")

# Must match CHECK constraint in postgres_schema.sql
ACCOUNT_STATUSES = [
    ("active",   0.88),
    ("dormant",  0.06),
    ("blocked",  0.03),
    ("closed",   0.02),
    ("pending",  0.01),
]


def _weighted_choice(choices: list) -> str:
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


class AccountGenerator:
    def __init__(self, conn, settings: Settings):
        self.conn = conn
        self.settings = settings
        random.seed(settings.seed + 2)
        Faker.seed(settings.seed + 2)

    def _get_product_type_ids(self, cur) -> List[int]:
        cur.execute("SELECT product_type_id FROM product_types WHERE is_active = TRUE")
        rows = cur.fetchall()
        if not rows:
            # Seed minimal product types if not yet present
            cur.execute("""
                INSERT INTO product_types (product_code, product_name, product_category, interest_rate)
                VALUES
                    ('SAV001', 'Tabungan Regular',    'savings',  0.0250),
                    ('SAV002', 'Tabungan Premium',    'savings',  0.0350),
                    ('CHK001', 'Giro Bisnis',         'checking', 0.0000),
                    ('DEP001', 'Deposito 3 Bulan',    'deposit',  0.0550),
                    ('LON001', 'KPR Properti',        'loan',     0.0975),
                    ('LON002', 'KTA Multiguna',       'loan',     0.1200),
                    ('CC001',  'Kartu Kredit Silver', 'credit_card', 0.0200)
                ON CONFLICT (product_code) DO NOTHING
            """)
            self.conn.commit()
            cur.execute("SELECT product_type_id FROM product_types WHERE is_active = TRUE")
            rows = cur.fetchall()
        return [r[0] for r in rows]

    def _build_record(
        self,
        customer_id: int,
        branch_id: int,
        product_type_id: int,
        seq: int,
    ) -> dict:
        status = _weighted_choice(ACCOUNT_STATUSES)
        opened = date.today() - timedelta(days=random.randint(30, 365 * 10))
        balance = round(random.uniform(0, 500_000_000), 2)   # 0 – 500 juta IDR
        return {
            "account_number": f"8{str(seq).zfill(12)}",      # 13-digit, starts with 8
            "customer_id": customer_id,
            "product_type_id": product_type_id,
            "branch_id": branch_id,
            "currency": "IDR",
            "balance": balance,
            "available_balance": round(balance * random.uniform(0.8, 1.0), 2),
            "account_status": status,
            "opened_date": opened,
            "closed_date": date.today() - timedelta(days=random.randint(1, 365)) if status == "closed" else None,
        }

    def generate(
        self,
        customer_ids: List[int],
        branch_ids: List[int],
    ) -> Tuple[List[int], Dict[int, int]]:
        """
        Generate accounts and return (account_ids, account_customer_map).
        Each customer gets 1–3 accounts. Total count ~= settings.num_accounts.
        """
        batch_size = self.settings.batch_size
        all_ids: List[int] = []
        account_customer_map: Dict[int, int] = {}
        seq = 1

        with self.conn.cursor() as cur:
            product_type_ids = self._get_product_type_ids(cur)
            batch = []
            records_meta = []  # track (customer_id) per batch item

            for customer_id in customer_ids:
                num_accounts = random.choices([1, 2, 3], weights=[0.55, 0.35, 0.10])[0]
                branch_id = random.choice(branch_ids)

                for _ in range(num_accounts):
                    product_type_id = random.choice(product_type_ids)
                    record = self._build_record(customer_id, branch_id, product_type_id, seq)
                    batch.append(record)
                    records_meta.append(customer_id)
                    seq += 1

                if len(batch) >= batch_size:
                    self._flush(cur, batch)
                    batch = []
                    records_meta = []

            if batch:
                self._flush(cur, batch)

            cur.execute("SELECT account_id, customer_id FROM accounts ORDER BY account_id")
            for acc_id, cust_id in cur.fetchall():
                all_ids.append(acc_id)
                account_customer_map[acc_id] = cust_id

        logger.info(f"Generated {len(all_ids)} accounts")
        return all_ids, account_customer_map

    def _flush(self, cur, batch: list):
        try:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO accounts (
                    account_number, customer_id, product_type_id, branch_id,
                    currency, balance, available_balance, account_status,
                    opened_date, closed_date
                ) VALUES (
                    %(account_number)s, %(customer_id)s, %(product_type_id)s, %(branch_id)s,
                    %(currency)s, %(balance)s, %(available_balance)s, %(account_status)s,
                    %(opened_date)s, %(closed_date)s
                )
                ON CONFLICT (account_number) DO NOTHING
                """,
                batch,
                page_size=len(batch),
            )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Account batch insert failed: {e}")
            raise