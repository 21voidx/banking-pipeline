"""
Merchant data generator for MySQL transaction_db.
Generates realistic Indonesian merchant profiles.
"""
import logging
import random
import uuid
from datetime import date, datetime
from typing import List
from zoneinfo import ZoneInfo

WIB = ZoneInfo("Asia/Jakarta")

from faker import Faker

from config.settings import Settings

logger = logging.getLogger(__name__)

fake = Faker("id_ID")

MCC_CODES = [
    "5411", "5812", "5541", "5912", "5732",
    "5311", "7011", "4511", "7995", "6051",
    "5999", "7372", "8099", "5661", "5691",
]

INDONESIAN_CITIES = [
    "Jakarta", "Surabaya", "Bandung", "Medan", "Bekasi",
    "Tangerang", "Depok", "Semarang", "Palembang", "Makassar",
    "Yogyakarta", "Bogor", "Batam", "Pekanbaru", "Banjarmasin",
]

CITY_PROVINCE = {
    "Jakarta": "DKI Jakarta", "Bekasi": "Jawa Barat", "Depok": "Jawa Barat",
    "Bogor": "Jawa Barat", "Bandung": "Jawa Barat", "Tangerang": "Banten",
    "Semarang": "Jawa Tengah", "Yogyakarta": "DI Yogyakarta",
    "Surabaya": "Jawa Timur", "Medan": "Sumatera Utara",
    "Palembang": "Sumatera Selatan", "Pekanbaru": "Riau",
    "Makassar": "Sulawesi Selatan", "Banjarmasin": "Kalimantan Selatan",
    "Batam": "Kepulauan Riau",
}

RISK_LEVELS = [("normal", 0.85), ("elevated", 0.12), ("high", 0.03)]


def _weighted_choice(choices: list) -> str:
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


class MerchantGenerator:
    def __init__(self, conn, settings: Settings):
        self.conn = conn
        self.settings = settings
        random.seed(settings.seed + 50)
        Faker.seed(settings.seed + 50)

    def _build_record(self, range_end=None) -> dict:
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        WIB = ZoneInfo("Asia/Jakarta")
        if range_end is None:
            range_end = datetime.now(tz=WIB).date()
        city = random.choice(INDONESIAN_CITIES)
        mcc = random.choice(MCC_CODES)
        is_online = random.random() < 0.25
        # Merchant bisa sudah eksis sebelum period
        days_back = random.randint(90, 365 * 5)
        reg_date = range_end - timedelta(days=days_back)
        created_at = datetime(
            reg_date.year, reg_date.month, reg_date.day,
            random.randint(8, 17), random.randint(0, 59), 0,
        )
        return {
            "merchant_uuid": str(uuid.uuid4()),
            "merchant_name": fake.company()[:200],
            "merchant_legal_name": fake.company()[:200] if random.random() < 0.6 else None,
            "mcc_code": mcc,
            "city": None if is_online else city,
            "province": None if is_online else CITY_PROVINCE.get(city),
            "country": "ID",
            "is_online": int(is_online),
            "risk_level": _weighted_choice(RISK_LEVELS),
            "is_active": int(random.random() > 0.05),
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def generate(self, count: int, range_end: date = None) -> List[int]:
        # range_end: batas akhir periode; merchant bisa lebih tua dari range ini
        """Generate merchants and return list of merchant_ids."""
        cursor = self.conn.cursor()
        batch = []
        all_ids: List[int] = []

        insert_sql = """
            INSERT INTO merchants (
                merchant_uuid, merchant_name, merchant_legal_name,
                mcc_code, city, province, country,
                is_online, risk_level, is_active,
                created_at, updated_at
            ) VALUES (
                %(merchant_uuid)s, %(merchant_name)s, %(merchant_legal_name)s,
                %(mcc_code)s, %(city)s, %(province)s, %(country)s,
                %(is_online)s, %(risk_level)s, %(is_active)s,
                %(created_at)s, %(updated_at)s
            )
        """

        for i in range(count):
            batch.append(self._build_record(range_end=range_end))
            if len(batch) >= self.settings.batch_size or i == count - 1:
                try:
                    cursor.executemany(insert_sql, batch)
                    self.conn.commit()
                    batch = []
                except Exception as e:
                    self.conn.rollback()
                    logger.error(f"Merchant batch insert failed: {e}")
                    raise

        cursor.execute("SELECT merchant_id FROM merchants WHERE is_active = 1")
        all_ids = [r[0] for r in cursor.fetchall()]
        cursor.close()

        logger.info(f"Generated {len(all_ids)} merchants")
        return all_ids