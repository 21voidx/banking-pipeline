"""
Branch data generator for PostgreSQL core_banking.
Generates realistic Indonesian bank branch data.
"""
import logging
import random
from datetime import date, timedelta
from zoneinfo import ZoneInfo

WIB = ZoneInfo("Asia/Jakarta")
from typing import List

import psycopg2.extras
from faker import Faker

from config.settings import Settings

logger = logging.getLogger(__name__)

fake = Faker("id_ID")

# Must match CHECK constraint in postgres_schema.sql
BRANCH_TYPES = [
    ("main",       0.02),
    ("regional",   0.08),
    ("sub",        0.55),
    ("atm_center", 0.35),
]

INDONESIAN_CITIES = [
    "Jakarta Pusat", "Jakarta Selatan", "Jakarta Barat", "Jakarta Timur", "Jakarta Utara",
    "Bandung", "Surabaya", "Medan", "Bekasi", "Tangerang", "Depok", "Semarang",
    "Palembang", "Makassar", "Yogyakarta", "Bogor", "Batam", "Pekanbaru",
    "Banjarmasin", "Padang",
]

CITY_PROVINCE_MAP = {
    "Jakarta Pusat": "DKI Jakarta", "Jakarta Selatan": "DKI Jakarta",
    "Jakarta Barat": "DKI Jakarta", "Jakarta Timur": "DKI Jakarta",
    "Jakarta Utara": "DKI Jakarta", "Bekasi": "Jawa Barat",
    "Depok": "Jawa Barat", "Bogor": "Jawa Barat", "Bandung": "Jawa Barat",
    "Tangerang": "Banten", "Semarang": "Jawa Tengah",
    "Yogyakarta": "DI Yogyakarta", "Surabaya": "Jawa Timur",
    "Medan": "Sumatera Utara", "Palembang": "Sumatera Selatan",
    "Padang": "Sumatera Barat", "Pekanbaru": "Riau",
    "Makassar": "Sulawesi Selatan", "Banjarmasin": "Kalimantan Selatan",
    "Batam": "Kepulauan Riau",
}


def _weighted_choice(choices: list) -> str:
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


class BranchGenerator:
    def __init__(self, conn, settings: Settings):
        self.conn = conn
        self.settings = settings
        random.seed(settings.seed)
        Faker.seed(settings.seed)

    def _build_record(self, seq: int, range_end: date = None) -> dict:
        from datetime import datetime
        if range_end is None:
            range_end = datetime.now(tz=WIB).date()
        branch_type = _weighted_choice(BRANCH_TYPES)
        city = random.choice(INDONESIAN_CITIES)
        province = CITY_PROVINCE_MAP.get(city, "DKI Jakarta")
        # Cabang bisa dibuka jauh sebelum period (bank sudah lama berdiri)
        opened = range_end - timedelta(days=random.randint(365, 365 * 20))
        return {
            "branch_code": f"BRN{str(seq).zfill(4)}",
            "branch_name": f"Cabang {city} {fake.last_name()}",
            "branch_type": branch_type,
            "city": city,
            "province": province,
            "address": fake.street_address()[:200],
            "phone": fake.phone_number()[:20],
            "is_active": random.random() > 0.05,
            "opened_date": opened,
            "created_at": datetime(
                opened.year, opened.month, opened.day,
                random.randint(8, 17), random.randint(0, 59), 0,
                tzinfo=WIB,
            ),
        }

    def generate(self, count: int, range_end: date = None) -> List[int]:
        """Generate branches and return list of branch_ids."""
        from datetime import datetime
        if range_end is None:
            range_end = datetime.now(tz=WIB).date()
        batch_size = self.settings.batch_size
        all_ids: List[int] = []

        with self.conn.cursor() as cur:
            batch = []
            for i in range(count):
                batch.append(self._build_record(i + 1, range_end=range_end))
                if len(batch) >= batch_size or i == count - 1:
                    try:
                        psycopg2.extras.execute_batch(
                            cur,
                            """
                            INSERT INTO branches (
                                branch_code, branch_name, branch_type,
                                city, province, address, phone,
                                is_active, opened_date, created_at, updated_at
                            ) VALUES (
                                %(branch_code)s, %(branch_name)s, %(branch_type)s,
                                %(city)s, %(province)s, %(address)s, %(phone)s,
                                %(is_active)s, %(opened_date)s, %(created_at)s, %(created_at)s
                            )
                            ON CONFLICT (branch_code) DO NOTHING
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

            cur.execute("SELECT branch_id FROM branches ORDER BY branch_id")
            all_ids = [r[0] for r in cur.fetchall()]

        logger.info(f"Generated {len(all_ids)} branches")
        return all_ids