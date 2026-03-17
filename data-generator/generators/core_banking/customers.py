"""
Customer data generator for PostgreSQL core_banking.
Generates realistic Indonesian banking customer profiles.

Semua timestamp menggunakan WIB (Asia/Jakarta, UTC+7).
"""

import logging
import random
from datetime import date, datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo

import psycopg2.extras
from faker import Faker

from config.settings import Settings

logger = logging.getLogger(__name__)

WIB = ZoneInfo("Asia/Jakarta")


def _rand_date_in_range(start: date, end: date) -> date:
    """Return random date between start and end (inclusive)."""
    span = max((end - start).days, 0)
    return start + timedelta(days=random.randint(0, span))

# Indonesian locale for realistic data
fake = Faker("id_ID")
Faker.seed(42)

INCOME_RANGES = [
    ("0-3jt", 0.20),
    ("3-5jt", 0.25),
    ("5-10jt", 0.30),
    ("10-25jt", 0.15),
    ("25-50jt", 0.07),
    (">50jt",  0.03),
]

CUSTOMER_SEGMENTS = [
    ("retail",    0.70),
    ("priority",  0.15),
    ("premier",   0.08),
    ("private",   0.02),
    ("sme",       0.04),
    ("corporate", 0.01),
]

ACQUISITION_CHANNELS = [
    ("branch",      0.40),
    ("mobile_app",  0.30),
    ("web",         0.15),
    ("agent",       0.08),
    ("referral",    0.05),
    ("telemarketing", 0.02),
]

INDONESIAN_PROVINCES = [
    "DKI Jakarta", "Jawa Barat", "Jawa Tengah", "Jawa Timur",
    "Banten", "DI Yogyakarta", "Bali", "Sumatera Utara",
    "Sumatera Selatan", "Kalimantan Timur", "Sulawesi Selatan",
]


def _weighted_choice(choices: list) -> str:
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


class CustomerGenerator:
    def __init__(self, conn, settings: Settings):
        self.conn = conn
        self.settings = settings
        random.seed(settings.seed)
        Faker.seed(settings.seed)

    def _generate_nik(self) -> str:
        """Generate a realistic Indonesian NIK (16 digits)."""
        province_code = str(random.randint(11, 94)).zfill(2)
        city_code = str(random.randint(1, 99)).zfill(2)
        district_code = str(random.randint(1, 99)).zfill(2)
        dob = fake.date_of_birth(minimum_age=17, maximum_age=75)
        gender = random.choice(["M", "F"])
        day = dob.day + 40 if gender == "F" else dob.day
        dob_str = f"{str(day).zfill(2)}{str(dob.month).zfill(2)}{str(dob.year)[-2:]}"
        sequence = str(random.randint(1, 9999)).zfill(4)
        return f"{province_code}{city_code}{district_code}{dob_str}{sequence}"

    def _build_record(self, branch_id: int, range_start: date = None, range_end: date = None) -> dict:
        today = datetime.now(tz=WIB).date()
        if range_end is None:
            range_end = today
        if range_start is None:
            range_start = range_end - timedelta(days=364)

        gender = random.choice(["M", "F"])
        if gender == "M":
            full_name = fake.name_male()
        else:
            full_name = fake.name_female()

        dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
        kyc_status = random.choices(
            ["verified", "pending", "rejected", "expired"],
            weights=[0.85, 0.08, 0.04, 0.03]
        )[0]
        kyc_verified_at = None
        if kyc_status == "verified":
            # kyc_verified_at: random dalam range_start s/d range_end
            span = max((range_end - range_start).days, 0)
            kyc_day = range_start + timedelta(days=random.randint(0, span))
            kyc_verified_at = datetime(
                kyc_day.year, kyc_day.month, kyc_day.day,
                random.randint(7, 20), random.randint(0, 59), random.randint(0, 59),
                tzinfo=WIB,
            )

        return {
            "full_name": full_name,
            "national_id": self._generate_nik(),
            "date_of_birth": dob,
            "gender": gender,
            "marital_status": random.choice(["single", "married", "divorced", "widowed"]),
            "occupation": fake.job()[:100],
            "income_range": _weighted_choice(INCOME_RANGES),
            "email": fake.ascii_email(),
            "phone_primary": fake.phone_number()[:20],
            "phone_secondary": fake.phone_number()[:20] if random.random() < 0.3 else None,
            "address_street": fake.street_address()[:200],
            "address_city": fake.city(),
            "address_province": random.choice(INDONESIAN_PROVINCES),
            "address_postal_code": fake.postcode()[:10],
            "customer_segment": _weighted_choice(CUSTOMER_SEGMENTS),
            "kyc_status": kyc_status,
            "kyc_verified_at": kyc_verified_at,
            "onboarding_branch_id": branch_id,
            "acquisition_channel": _weighted_choice(ACQUISITION_CHANNELS),
            "risk_rating": random.choices(
                ["low", "medium", "high", "blacklist"],
                weights=[0.80, 0.15, 0.04, 0.01]
            )[0],
            "is_politically_exposed": random.random() < 0.002,
            # Audit timestamps — tersebar merata dalam [range_start, range_end]
            "created_at": datetime(
                *_rand_date_in_range(range_start, range_end).timetuple()[:3],
                random.randint(0, 23), random.randint(0, 59), random.randint(0, 59),
                tzinfo=WIB,
            ),
        }

    def generate(self, count: int, branch_ids: List[int], range_start: date = None, range_end: date = None) -> List[int]:
        """Generate customers in batches and return list of customer_ids."""
        today = datetime.now(tz=WIB).date()
        if range_end is None:
            range_end = today
        if range_start is None:
            range_start = range_end - timedelta(days=364)

        all_ids = []
        batch_size = self.settings.batch_size

        with self.conn.cursor() as cur:
            batch = []
            for i in range(count):
                record = self._build_record(random.choice(branch_ids), range_start=range_start, range_end=range_end)
                batch.append(record)

                if len(batch) >= batch_size or i == count - 1:
                    try:
                        psycopg2.extras.execute_batch(
                            cur,
                            """
                            INSERT INTO customers (
                                full_name, national_id, date_of_birth, gender,
                                marital_status, occupation, income_range,
                                email, phone_primary, phone_secondary,
                                address_street, address_city, address_province, address_postal_code,
                                customer_segment, kyc_status, kyc_verified_at,
                                onboarding_branch_id, acquisition_channel,
                                risk_rating, is_politically_exposed,
                                created_at, updated_at
                            ) VALUES (
                                %(full_name)s, %(national_id)s, %(date_of_birth)s, %(gender)s,
                                %(marital_status)s, %(occupation)s, %(income_range)s,
                                %(email)s, %(phone_primary)s, %(phone_secondary)s,
                                %(address_street)s, %(address_city)s, %(address_province)s, %(address_postal_code)s,
                                %(customer_segment)s, %(kyc_status)s, %(kyc_verified_at)s,
                                %(onboarding_branch_id)s, %(acquisition_channel)s,
                                %(risk_rating)s, %(is_politically_exposed)s,
                                %(created_at)s, %(created_at)s
                            )
                            ON CONFLICT (national_id) DO NOTHING
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

            # Fetch all generated customer IDs
            cur.execute("SELECT customer_id FROM customers ORDER BY customer_id")
            all_ids = [r[0] for r in cur.fetchall()]

        logger.info(f"Generated {len(all_ids)} customers")
        return all_ids
