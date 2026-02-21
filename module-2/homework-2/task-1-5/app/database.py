import logging
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool

from app.config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    _pool = None

    @classmethod
    def get_pool(cls):
        if cls._pool is None:
            try:
                cls._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=5,
                    maxconn=20,
                    user=settings.postgres_user,
                    password=settings.postgres_password,
                    host=settings.postgres_host,
                    port=settings.postgres_port,
                    database=settings.postgres_db
                )
                logger.info("Connection pool created.")
            except Exception as error:
                logger.error(f"Failed to create connection pool: {error}")
                raise

        return cls._pool

    @classmethod
    @contextmanager
    def get_connection(cls):
        conn = cls.get_pool().getconn()
        try:
            yield conn
        finally:
            cls.get_pool().putconn(conn)

    @classmethod
    def close_pool(cls):
        if cls._pool:
            cls._pool.closeall()
            logger.info("Connection pool closed.")

    @classmethod
    def init_schema(cls):
        """
        Creates the necessary table.
        """
        with cls.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_counter (
                        user_id INTEGER PRIMARY KEY,
                        counter INTEGER DEFAULT 0,
                        version INTEGER DEFAULT 0
                    );
                """)
            connection.commit()
        logger.info("Schema initialized.")

    @classmethod
    def reset_counter(cls):
        """
        Resets the counter for a fresh test run.
        """
        with cls.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM user_counter WHERE user_id = %s", (settings.target_user_id,))
                cursor.execute(
                    "INSERT INTO user_counter (user_id, counter, version) VALUES (%s, 0, 0)",
                    (settings.target_user_id,)
                )
            connection.commit()
