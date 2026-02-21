import os
import time

import psycopg2
import threading

from abc import ABC, abstractmethod
from psycopg2 import pool


class CounterStorage(ABC):
    @abstractmethod
    def increment(self) -> None:
        pass

    @abstractmethod
    def get_value(self) -> int:
        pass

    @abstractmethod
    def reset(self) -> None:
        pass


class InMemoryStorage(CounterStorage):
    def __init__(self):
        self._counter = 0
        self._lock = threading.Lock()

    def increment(self) -> None:
        with self._lock:
            self._counter += 1

    def get_value(self) -> int:
        return self._counter

    def reset(self) -> None:
        with self._lock:
            self._counter = 0


class DiskStorage(CounterStorage):
    def __init__(self, filepath: str = "data/counter.txt"):
        self.filepath = filepath
        self._lock = threading.Lock()

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if not os.path.exists(filepath):
            with open(filepath, "w") as file:
                file.write("0")

    def increment(self) -> None:
        with self._lock:
            current_value = 0
            if os.path.exists(self.filepath):
                with open(self.filepath, "r") as file:
                    content = file.read().strip()
                    if content.isdigit():
                        current_value = int(content)

            current_value += 1

            with open(self.filepath, "w") as file:
                file.write(str(current_value))
                file.flush()
                os.fsync(file.fileno())

    def get_value(self) -> int:
        with self._lock:
            if not os.path.exists(self.filepath):
                return 0
            with open(self.filepath, "r") as file:
                content = file.read().strip()
                return int(content) if content.isdigit() else 0

    def reset(self) -> None:
        with self._lock:
            with open(self.filepath, "w") as f:
                f.write("0")
                f.flush()
                os.fsync(f.fileno())


class PostgresStorage(CounterStorage):
    def __init__(self, dsn: str):
        for retry in range(5):
            try:
                self.pool = psycopg2.pool.ThreadedConnectionPool(1, 20, dsn)
                break
            except psycopg2.OperationalError as error:
                if retry == 4:
                    raise error
                time.sleep(2)

        self._init_db()

    def _init_db(self):
        connection = self.pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                        CREATE TABLE IF NOT EXISTS user_counter
                        (
                            user_id INTEGER PRIMARY KEY,
                            counter INTEGER DEFAULT 0
                        );
                    """)

                cursor.execute(
                    "INSERT INTO user_counter (user_id, counter) VALUES (1, 0) ON CONFLICT (user_id) DO NOTHING;"
                )
            connection.commit()
        finally:
            self.pool.putconn(connection)

    def increment(self) -> None:
        connection = self.pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
            connection.commit()
        finally:
            self.pool.putconn(connection)

    def get_value(self) -> int:
        connection = self.pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                row = cursor.fetchone()
                return row[0] if row else 0
        finally:
            self.pool.putconn(connection)

    def reset(self) -> None:
        connection = self.pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE user_counter SET counter = 0 WHERE user_id = 1")
            connection.commit()
        finally:
            self.pool.putconn(connection)

    def close(self):
        if self.pool:
            self.pool.closeall()
