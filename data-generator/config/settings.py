"""
Configuration settings for the data generator.
Values are loaded from environment variables.
"""

import os
from dataclasses import dataclass


@dataclass
class Settings:
    # PostgreSQL Core Banking
    postgres_host: str     = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int     = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str       = os.getenv("POSTGRES_DB", "core_banking")
    postgres_user: str     = os.getenv("POSTGRES_USER", "banking_core")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "")

    # MySQL Transaction System
    mysql_host: str        = os.getenv("MYSQL_HOST", "localhost")
    mysql_port: int        = int(os.getenv("MYSQL_PORT", "3306"))
    mysql_db: str          = os.getenv("MYSQL_DB", "transaction_db")
    mysql_user: str        = os.getenv("MYSQL_USER", "banking_txn")
    mysql_password: str    = os.getenv("MYSQL_PASSWORD", "")

    # Generation parameters
    seed: int              = int(os.getenv("SEED", "42"))
    num_customers: int     = int(os.getenv("NUM_CUSTOMERS", "10000"))
    num_accounts: int      = int(os.getenv("NUM_ACCOUNTS", "15000"))
    num_transactions: int  = int(os.getenv("NUM_TRANSACTIONS", "100000"))
    num_merchants: int     = int(os.getenv("NUM_MERCHANTS", "500"))
    num_branches: int      = int(os.getenv("NUM_BRANCHES", "50"))
    num_employees: int     = int(os.getenv("NUM_EMPLOYEES", "200"))

    # Batch insert size (tune for performance)
    batch_size: int        = int(os.getenv("BATCH_SIZE", "500"))
