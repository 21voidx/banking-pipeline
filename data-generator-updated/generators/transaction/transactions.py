"""
Transaction data generator for MySQL transaction_db.
Generates realistic banking transaction patterns including
daily spending, salary credits, and transfers.

Semua timestamp menggunakan WIB (Asia/Jakarta, UTC+7).
"""

import logging
import random
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import mysql.connector

from config.settings import Settings

logger = logging.getLogger(__name__)

WIB = ZoneInfo("Asia/Jakarta")

# Transaction amount distributions by type (IDR)
TRANSACTION_PATTERNS = {
    "DEBIT_PURCHASE":    {"min": 10_000,    "max": 5_000_000,  "weight": 0.30},
    "CREDIT_PURCHASE":   {"min": 50_000,    "max": 25_000_000, "weight": 0.10},
    "ATM_WITHDRAWAL":    {"min": 100_000,   "max": 3_000_000,  "weight": 0.15},
    "TRANSFER_OUT":      {"min": 50_000,    "max": 50_000_000, "weight": 0.15},
    "TRANSFER_IN":       {"min": 50_000,    "max": 50_000_000, "weight": 0.12},
    "SALARY_CREDIT":     {"min": 3_000_000, "max": 50_000_000, "weight": 0.05},
    "BILL_PAYMENT":      {"min": 50_000,    "max": 5_000_000,  "weight": 0.08},
    "QRIS_PAYMENT":      {"min": 5_000,     "max": 500_000,    "weight": 0.05},
}

CHANNELS = [
    ("mobile", 0.40),
    ("atm", 0.20),
    ("web", 0.15),
    ("pos", 0.15),
    ("branch", 0.05),
    ("qris", 0.05),
]

STATUS_DISTRIBUTION = [
    ("completed", 0.95),
    ("failed", 0.03),
    ("pending", 0.01),
    ("reversed", 0.01),
]


def _weighted_choice(choices: list) -> str:
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


class TransactionGenerator:
    def __init__(self, conn, settings: Settings):
        self.conn = conn
        self.settings = settings
        random.seed(settings.seed + 100)  # Different seed from customers

        # Load type and method IDs from DB
        self._load_reference_ids()

    def _load_reference_ids(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT type_id, type_code FROM transaction_types WHERE is_active = 1")
        self.type_map = {row[1]: row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT method_id, method_code FROM payment_methods WHERE is_active = 1")
        self.method_map = {row[1]: row[0] for row in cursor.fetchall()}

        cursor.close()

    def _random_amount(self, txn_type_code: str) -> Tuple[Decimal, Decimal]:
        """Return (amount, fee_amount) based on transaction type."""
        pattern = TRANSACTION_PATTERNS.get(
            txn_type_code,
            {"min": 10_000, "max": 1_000_000}
        )
        amount = Decimal(str(round(random.uniform(pattern["min"], pattern["max"]), -3)))
        fee_amount = Decimal("0.00")
        if txn_type_code in ("TRANSFER_OUT", "TRANSFER_IN"):
            fee_amount = Decimal("6500.00")
        elif txn_type_code == "ATM_WITHDRAWAL":
            fee_amount = Decimal("7500.00")
        return amount, fee_amount

    def _generate_reference_number(self) -> str:
        ts = datetime.now(tz=WIB).strftime("%Y%m%d%H%M%S")
        rand = str(random.randint(100000, 999999))
        return f"TXN{ts}{rand}"

    def _build_transaction(
        self,
        account_id: int,
        customer_id: int,
        merchant_id: int,
        txn_date: date,
        type_codes: List[str],
    ) -> dict:
        txn_type_code = random.choices(
            list(TRANSACTION_PATTERNS.keys()),
            weights=[p["weight"] for p in TRANSACTION_PATTERNS.values()],
        )[0]

        amount, fee_amount = self._random_amount(txn_type_code)
        channel = _weighted_choice(CHANNELS)

        # Determine payment method based on channel
        channel_method_map = {
            "mobile": "MOBILE_BANKING",
            "web": "INTERNET_BANKING",
            "atm": "DEBIT_CARD",
            "pos": "DEBIT_CARD",
            "branch": "CASH",
            "qris": "QRIS",
        }
        method_code = channel_method_map.get(channel, "BANK_TRANSFER")

        txn_hour = random.choices(
            range(24),
            weights=[
                1, 1, 1, 1, 1, 2,   # 00-05: low activity
                5, 8, 10, 9, 8, 9,   # 06-11: morning peak
                10, 8, 7, 7, 8, 9,   # 12-17: afternoon
                10, 9, 7, 5, 3, 2,   # 18-23: evening peak
            ],
        )[0]
        txn_minute = random.randint(0, 59)
        txn_second = random.randint(0, 59)

        # Build naive datetime then attach WIB tzinfo
        txn_at_naive = datetime(
            txn_date.year, txn_date.month, txn_date.day,
            txn_hour, txn_minute, txn_second,
        )
        txn_at = txn_at_naive.replace(tzinfo=WIB)

        status = _weighted_choice(STATUS_DISTRIBUTION)

        # Assign merchant only for purchase types
        use_merchant = txn_type_code in ("DEBIT_PURCHASE", "CREDIT_PURCHASE", "QRIS_PAYMENT")

        # MySQL DATETIME(6) tidak aware timezone — store as WIB naive string
        txn_at_str = txn_at_naive.strftime("%Y-%m-%d %H:%M:%S.%f")
        processed_at_str = txn_at_naive.strftime("%Y-%m-%d %H:%M:%S.%f") if status == "completed" else None

        return {
            "transaction_uuid": str(uuid.uuid4()),
            "account_id": account_id,
            "customer_id": customer_id,
            "merchant_id": merchant_id if use_merchant else None,
            "type_id": self.type_map.get(txn_type_code, 1),
            "method_id": self.method_map.get(method_code),
            "amount": float(amount),
            "currency": "IDR",
            "exchange_rate": 1.000000,
            "amount_idr": float(amount),
            "fee_amount": float(fee_amount),
            "balance_before": float(random.uniform(100_000, 100_000_000)),
            "balance_after": None,
            "description": f"{txn_type_code.replace('_', ' ').title()} via {channel.upper()}",
            "reference_number": self._generate_reference_number(),
            "channel": channel,
            "transaction_status": status,
            "status_reason": "Insufficient funds" if status == "failed" else None,
            "transaction_date": txn_date.strftime("%Y-%m-%d"),
            "transaction_at": txn_at_str,
            "processed_at": processed_at_str,
            "created_at": txn_at_str,
            "updated_at": txn_at_str,
        }

    def generate(
        self,
        account_ids: List[int],
        account_customer_map: Dict[int, int],
        merchant_ids: List[int],
        count: int,
        range_start: date = None,
        range_end: date = None,
    ) -> List[int]:
        """Generate transactions and return list of transaction_ids.

        range_start : tanggal AWAL distribusi transaksi (inklusif)
                      Full load      : 2025-01-01
                      Incremental    : tanggal hari itu sendiri (mis. 2026-01-02)
        range_end   : tanggal AKHIR distribusi transaksi (inklusif)
                      Full load      : 2025-12-31
                      Incremental    : sama dengan range_start
        """
        all_ids = []
        batch_size = self.settings.batch_size

        today = datetime.now(tz=WIB).date()
        if range_end is None:
            range_end = today - timedelta(days=1)
        if range_start is None:
            range_start = range_end - timedelta(days=364)

        start_date = range_start
        end_date = range_end
        date_range_days = (end_date - start_date).days

        insert_sql = """
            INSERT INTO transactions (
                transaction_uuid, account_id, customer_id, merchant_id,
                type_id, method_id,
                amount, currency, exchange_rate, amount_idr, fee_amount,
                balance_before, balance_after,
                description, reference_number, channel,
                transaction_status, status_reason,
                transaction_date, transaction_at, processed_at,
                created_at, updated_at
            ) VALUES (
                %(transaction_uuid)s, %(account_id)s, %(customer_id)s, %(merchant_id)s,
                %(type_id)s, %(method_id)s,
                %(amount)s, %(currency)s, %(exchange_rate)s, %(amount_idr)s, %(fee_amount)s,
                %(balance_before)s, %(balance_after)s,
                %(description)s, %(reference_number)s, %(channel)s,
                %(transaction_status)s, %(status_reason)s,
                %(transaction_date)s, %(transaction_at)s, %(processed_at)s,
                %(created_at)s, %(updated_at)s
            )
        """

        cursor = self.conn.cursor()
        batch = []

        for i in range(count):
            account_id = random.choice(account_ids)
            customer_id = account_customer_map.get(account_id, random.choice(list(account_customer_map.values())))
            merchant_id = random.choice(merchant_ids)
            txn_date = start_date + timedelta(days=random.randint(0, date_range_days))

            record = self._build_transaction(
                account_id=account_id,
                customer_id=customer_id,
                merchant_id=merchant_id,
                txn_date=txn_date,
                type_codes=list(TRANSACTION_PATTERNS.keys()),
            )
            batch.append(record)

            if len(batch) >= batch_size or i == count - 1:
                try:
                    cursor.executemany(insert_sql, batch)
                    self.conn.commit()

                    # Fetch the IDs just inserted
                    cursor.execute(
                        "SELECT transaction_id FROM transactions ORDER BY transaction_id DESC LIMIT %s",
                        (len(batch),),
                    )
                    all_ids.extend([r[0] for r in cursor.fetchall()])
                    batch = []

                except mysql.connector.Error as e:
                    self.conn.rollback()
                    logger.error(f"Transaction batch failed at record {i}: {e}")
                    batch = []

        cursor.close()
        logger.info(f"Generated {len(all_ids)} transactions")
        return all_ids
