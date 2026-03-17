"""Database connection utilities for the data generator."""

import logging
import time

import psycopg2
import psycopg2.extras
import mysql.connector

from config.settings import Settings

logger = logging.getLogger(__name__)


def get_postgres_conn(settings: Settings, retries: int = 5, delay: int = 5):
    """Get a PostgreSQL connection with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                dbname=settings.postgres_db,
                user=settings.postgres_user,
                password=settings.postgres_password,
                options="-c timezone=Asia/Jakarta",
                connect_timeout=10,
            )
            conn.autocommit = False
            logger.info(f"✅ PostgreSQL connected: {settings.postgres_host}:{settings.postgres_port}")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < retries:
                logger.warning(f"PostgreSQL connection failed (attempt {attempt}/{retries}): {e}")
                time.sleep(delay)
            else:
                raise


def get_mysql_conn(settings: Settings, retries: int = 5, delay: int = 5):
    """Get a MySQL connection with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            conn = mysql.connector.connect(
                host=settings.mysql_host,
                port=settings.mysql_port,
                database=settings.mysql_db,
                user=settings.mysql_user,
                password=settings.mysql_password,
                charset="utf8mb4",
                collation="utf8mb4_unicode_ci",
                time_zone="+07:00",
                autocommit=False,
                connection_timeout=10,
            )
            logger.info(f"✅ MySQL connected: {settings.mysql_host}:{settings.mysql_port}")
            return conn
        except mysql.connector.Error as e:
            if attempt < retries:
                logger.warning(f"MySQL connection failed (attempt {attempt}/{retries}): {e}")
                time.sleep(delay)
            else:
                raise
