"""
Fraud flag data generator for MySQL transaction_db.
Tags a realistic subset of transactions as suspicious.
"""
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List

from config.settings import Settings

logger = logging.getLogger(__name__)

FLAG_TYPES = [
    ("velocity",            0.35),
    ("amount_anomaly",      0.25),
    ("geo_anomaly",         0.20),
    ("device_fingerprint",  0.12),
    ("blacklist_merchant",  0.08),
]

SEVERITIES = [
    ("low",      0.40),
    ("medium",   0.35),
    ("high",     0.18),
    ("critical", 0.07),
]


def _weighted_choice(choices: list) -> str:
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


class FraudFlagGenerator:
    def __init__(self, conn, settings: Settings):
        self.conn = conn
        self.settings = settings
        random.seed(settings.seed + 99)

    def _build_record(
        self,
        transaction_id: int,
        customer_id: int,
    ) -> dict:
        severity = _weighted_choice(SEVERITIES)
        created_at = datetime.now() - timedelta(seconds=random.randint(0, 86400 * 30))

        # 70% still pending review, 20% confirmed fraud, 10% false positive
        review_outcome = random.choices(
            [None, 1, 0], weights=[0.70, 0.20, 0.10]
        )[0]
        reviewed_at = created_at + timedelta(hours=random.randint(1, 72)) \
            if review_outcome is not None else None

        return {
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "flag_type": _weighted_choice(FLAG_TYPES),
            "severity": severity,
            "confidence_score": round(random.uniform(0.50, 0.99), 4),
            "model_version": random.choice(["v1.0", "v1.1", "v2.0"]),
            "is_confirmed_fraud": review_outcome,
            "reviewed_by": f"analyst_{random.randint(1, 20):02d}@bank.co.id" if reviewed_at else None,
            "reviewed_at": reviewed_at.strftime("%Y-%m-%d %H:%M:%S") if reviewed_at else None,
            "notes": None,
        }

    def generate(
        self,
        transaction_ids: List[int],
        account_customer_map: Dict[int, int],
        fraud_rate: float = 0.008,
    ) -> List[int]:
        """
        Flag a random subset of transactions as fraudulent.
        Returns list of flag_ids created.
        """
        flagged = random.sample(
            transaction_ids,
            k=max(1, int(len(transaction_ids) * fraud_rate)),
        )

        # customer_id lookup: account_customer_map values are customer_ids
        customer_ids = list(account_customer_map.values())

        insert_sql = """
            INSERT INTO fraud_flags (
                transaction_id, customer_id, flag_type, severity,
                confidence_score, model_version,
                is_confirmed_fraud, reviewed_by, reviewed_at, notes
            ) VALUES (
                %(transaction_id)s, %(customer_id)s, %(flag_type)s, %(severity)s,
                %(confidence_score)s, %(model_version)s,
                %(is_confirmed_fraud)s, %(reviewed_by)s, %(reviewed_at)s, %(notes)s
            )
        """

        cursor = self.conn.cursor()
        batch = []
        all_ids: List[int] = []

        for i, txn_id in enumerate(flagged):
            customer_id = random.choice(customer_ids)
            batch.append(self._build_record(txn_id, customer_id))

            if len(batch) >= self.settings.batch_size or i == len(flagged) - 1:
                try:
                    cursor.executemany(insert_sql, batch)
                    self.conn.commit()
                    batch = []
                except Exception as e:
                    self.conn.rollback()
                    logger.error(f"Fraud flag batch insert failed: {e}")
                    raise

        cursor.execute("SELECT flag_id FROM fraud_flags ORDER BY flag_id DESC LIMIT %s", (len(flagged),))
        all_ids = [r[0] for r in cursor.fetchall()]
        cursor.close()

        logger.info(f"Flagged {len(all_ids)} transactions as suspicious (rate={fraud_rate:.1%})")
        return all_ids