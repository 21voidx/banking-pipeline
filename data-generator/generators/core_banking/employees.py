"""
Employee data generator for PostgreSQL core_banking.
Generates realistic Indonesian bank employee profiles.
"""
import logging
import random
from datetime import date, timedelta
from typing import List

import psycopg2.extras
from faker import Faker

from config.settings import Settings

logger = logging.getLogger(__name__)

fake = Faker("id_ID")

# Must match CHECK constraint in postgres_schema.sql
ROLES = [
    ("teller",                0.28),
    ("cs",                    0.22),
    ("relationship_manager",  0.18),
    ("back_office",           0.15),
    ("analyst",               0.08),
    ("branch_manager",        0.05),
    ("it",                    0.04),
]


def _weighted_choice(choices: list) -> str:
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


class EmployeeGenerator:
    def __init__(self, conn, settings: Settings):
        self.conn = conn
        self.settings = settings
        random.seed(settings.seed + 1)
        Faker.seed(settings.seed + 1)

    def _build_record(self, branch_id: int, seq: int) -> dict:
        hire_date = date.today() - timedelta(days=random.randint(30, 365 * 15))
        return {
            "employee_code": f"EMP{str(seq).zfill(5)}",
            "full_name": fake.name(),
            "email": f"emp{seq}.{fake.last_name().lower().replace(' ', '')}@bank.co.id",
            "role": _weighted_choice(ROLES),
            "branch_id": branch_id,
            "hire_date": hire_date,
            "is_active": random.random() > 0.08,
        }

    def generate(self, branch_ids: List[int], count: int) -> List[int]:
        """Generate employees and return list of employee_ids."""
        batch_size = self.settings.batch_size
        all_ids: List[int] = []

        with self.conn.cursor() as cur:
            batch = []
            for i in range(count):
                batch.append(self._build_record(random.choice(branch_ids), i + 1))
                if len(batch) >= batch_size or i == count - 1:
                    try:
                        psycopg2.extras.execute_batch(
                            cur,
                            """
                            INSERT INTO employees (
                                employee_code, full_name, email,
                                role, branch_id, hire_date, is_active
                            ) VALUES (
                                %(employee_code)s, %(full_name)s, %(email)s,
                                %(role)s, %(branch_id)s, %(hire_date)s, %(is_active)s
                            )
                            ON CONFLICT (employee_code) DO NOTHING
                            """,
                            batch,
                            page_size=batch_size,
                        )
                        self.conn.commit()
                        batch = []
                    except Exception as e:
                        self.conn.rollback()
                        logger.error(f"Batch insert failed: {e}")
                        raise

            cur.execute("SELECT employee_id FROM employees ORDER BY employee_id")
            all_ids = [r[0] for r in cur.fetchall()]

        logger.info(f"Generated {len(all_ids)} employees")
        return all_ids