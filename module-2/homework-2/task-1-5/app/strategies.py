import psycopg2

from psycopg2 import errors

from app.config import settings
from app.database import DatabaseManager


class Strategies:
    """
    Collection of update strategies.
    """

    @staticmethod
    def lost_update():
        """
        Lost update strategy.
        """
        with DatabaseManager.get_connection() as connection:
            with connection.cursor() as cursor:
                for _ in range(settings.iterations):
                    cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (settings.target_user_id,))

                    value = cursor.fetchone()[0]
                    value += 1

                    cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s",
                                   (value, settings.target_user_id))
                    connection.commit()

    @staticmethod
    def serializable_update():
        """
        Serializable update strategy - fixed version.
        """
        with DatabaseManager.get_connection() as connection:
            connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

            with connection.cursor() as cursor:
                for _ in range(settings.iterations):
                    while True:
                        try:
                            cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s",
                                           (settings.target_user_id,))
                            row = cursor.fetchone()

                            if not row:
                                break

                            value = row[0] + 1
                            cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s",
                                           (value, settings.target_user_id))
                            connection.commit()
                            break
                        except errors.SerializationFailure:
                            connection.rollback()
                            continue
                        except Exception:
                            connection.rollback()
                            break

            connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

    @staticmethod
    def in_place_update():
        """
        In-place update strategy.
        """
        with DatabaseManager.get_connection() as connection:
            with connection.cursor() as cursor:
                for _ in range(settings.iterations):
                    cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s",
                                   (settings.target_user_id,))
                    connection.commit()

    @staticmethod
    def row_level_locking():
        """
        Row-level locking strategy.
        """
        with DatabaseManager.get_connection() as connection:
            with connection.cursor() as cursor:
                for _ in range(settings.iterations):
                    cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s FOR UPDATE",
                                   (settings.target_user_id,))

                    val = cursor.fetchone()[0]
                    val += 1

                    cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s",
                                   (val, settings.target_user_id))
                    connection.commit()

    @staticmethod
    def optimistic_concurrency_control():
        """
        Optimistic concurrency control strategy.
        """
        with DatabaseManager.get_connection() as connection:
            with connection.cursor() as cursor:
                for _ in range(settings.iterations):
                    while True:
                        cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = %s",
                                       (settings.target_user_id,))

                        row = cursor.fetchone()
                        if not row:
                            break

                        counter, version = row

                        cursor.execute(
                            """
                            UPDATE user_counter
                            SET counter = %s,
                                version = %s
                            WHERE user_id = %s
                              AND version = %s
                            """, (counter + 1, version + 1, settings.target_user_id, version)
                        )

                        connection.commit()
                        if cursor.rowcount > 0:
                            break
