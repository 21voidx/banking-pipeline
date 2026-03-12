"""
Loan application data generator for PostgreSQL core_banking.
Generates realistic loan application lifecycle records.
"""
import logging
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Optional

import psycopg2.extras
from faker import Faker

from config.settings import Settings

logger = logging.getLogger(__name__)

fake = Faker("id_ID")

# Must match CHECK constraints in postgres_schema.sql
APPLICATION_STATUSES = [
    ("submitted",    0.08),
    ("under_review", 0.10),
    ("approved",     0.35),
    ("rejected",     0.20),
    ("disbursed",    0.20),
    ("cancelled",    0.04),
    ("withdrawn",    0.03),
]

COLLATERAL_TYPES = [
    ("none",      0.45),
    ("property",  0.25),
    ("vehicle",   0.15),
    ("deposit",   0.08),
    ("guarantee", 0.05),
    ("other",     0.02),
]

PURPOSES = [
    "Modal Usaha", "Renovasi Rumah", "Pendidikan", "Kendaraan Bermotor",
    "Pembelian Properti", "Kebutuhan Konsumtif", "Refinancing", "Investasi",
    "Biaya Kesehatan", "Pernikahan",
]

LOAN_PRODUCT_CODES = ["LON001", "LON002"]   # KPR, KTA


def _weighted_choice(choices: list) -> str:
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


class LoanApplicationGenerator:
    def __init__(self, conn, settings: Settings):
        self.conn = conn
        self.settings = settings
        random.seed(settings.seed + 3)
        Faker.seed(settings.seed + 3)

    def _get_loan_product_type_ids(self, cur) -> List[int]:
        cur.execute(
            "SELECT product_type_id FROM product_types WHERE product_category = 'loan' AND is_active = TRUE"
        )
        rows = cur.fetchall()
        return [r[0] for r in rows] if rows else [1]

    def _build_record(
        self,
        customer_id: int,
        branch_id: int,
        employee_id: Optional[int],
        product_type_id: int,
        seq: int,
    ) -> dict:
        status = _weighted_choice(APPLICATION_STATUSES)
        collateral = _weighted_choice(COLLATERAL_TYPES)
        requested = round(random.choice([
            random.uniform(5_000_000, 50_000_000),    # KTA range
            random.uniform(100_000_000, 2_000_000_000),  # KPR range
        ]), -3)
        approved = round(requested * random.uniform(0.7, 1.0), -3) if status in ("approved", "disbursed") else None
        tenor = random.choice([12, 24, 36, 48, 60, 84, 120, 180, 240])
        submitted_at = datetime.now() - timedelta(days=random.randint(1, 365 * 3))

        reviewed_at = submitted_at + timedelta(days=random.randint(1, 7)) \
            if status not in ("submitted",) else None
        decided_at = reviewed_at + timedelta(days=random.randint(1, 5)) \
            if reviewed_at and status in ("approved", "rejected", "disbursed") else None
        disbursed_at = decided_at + timedelta(days=random.randint(1, 14)) \
            if decided_at and status == "disbursed" else None

        return {
            "application_number": f"LA{submitted_at.strftime('%Y%m')}{str(seq).zfill(6)}",
            "customer_id": customer_id,
            "branch_id": branch_id,
            "handled_by": employee_id,
            "product_type_id": product_type_id,
            "requested_amount": requested,
            "approved_amount": approved,
            "tenor_months": tenor,
            "interest_rate": round(random.uniform(0.07, 0.15), 4),
            "purpose": random.choice(PURPOSES),
            "collateral_type": collateral,
            "collateral_value": round(requested * random.uniform(1.0, 1.5), -3) if collateral != "none" else None,
            "application_status": status,
            "submitted_at": submitted_at,
            "reviewed_at": reviewed_at,
            "decided_at": decided_at,
            "disbursed_at": disbursed_at,
            "rejection_reason": fake.sentence() if status == "rejected" else None,
        }

    def generate(
        self,
        customer_ids: List[int],
        branch_ids: List[int],
        employee_ids: List[int],
        count_override: Optional[int] = None,
    ) -> List[int]:
        """Generate loan applications and return list of application_ids."""
        count = count_override or max(len(customer_ids) // 5, 1)
        batch_size = self.settings.batch_size
        all_ids: List[int] = []

        with self.conn.cursor() as cur:
            product_type_ids = self._get_loan_product_type_ids(cur)
            batch = []

            for seq in range(1, count + 1):
                record = self._build_record(
                    customer_id=random.choice(customer_ids),
                    branch_id=random.choice(branch_ids),
                    employee_id=random.choice(employee_ids) if employee_ids else None,
                    product_type_id=random.choice(product_type_ids),
                    seq=seq,
                )
                batch.append(record)

                if len(batch) >= batch_size or seq == count:
                    try:
                        psycopg2.extras.execute_batch(
                            cur,
                            """
                            INSERT INTO loan_applications (
                                application_number, customer_id, branch_id, handled_by,
                                product_type_id, requested_amount, approved_amount,
                                tenor_months, interest_rate, purpose,
                                collateral_type, collateral_value, application_status,
                                submitted_at, reviewed_at, decided_at, disbursed_at,
                                rejection_reason
                            ) VALUES (
                                %(application_number)s, %(customer_id)s, %(branch_id)s, %(handled_by)s,
                                %(product_type_id)s, %(requested_amount)s, %(approved_amount)s,
                                %(tenor_months)s, %(interest_rate)s, %(purpose)s,
                                %(collateral_type)s, %(collateral_value)s, %(application_status)s,
                                %(submitted_at)s, %(reviewed_at)s, %(decided_at)s, %(disbursed_at)s,
                                %(rejection_reason)s
                            )
                            ON CONFLICT (application_number) DO NOTHING
                            """,
                            batch,
                            page_size=batch_size,
                        )
                        self.conn.commit()
                        batch = []
                    except Exception as e:
                        self.conn.rollback()
                        logger.error(f"Loan batch insert failed: {e}")
                        raise

            cur.execute("SELECT application_id FROM loan_applications ORDER BY application_id")
            all_ids = [r[0] for r in cur.fetchall()]

        logger.info(f"Generated {len(all_ids)} loan applications")
        return all_ids