import os
import threading

from abc import ABC, abstractmethod
from pymongo import MongoClient


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


class MongoStorage(CounterStorage):

    def __init__(self, uri: str = "mongodb://localhost:27017/"):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.db = self.client["web_counter_db"]
        self.collection = self.db["counters"]

        self.collection.update_one(
            {"_id": "main_counter"},
            {"$setOnInsert": {"count": 0}},
            upsert=True
        )

    def increment(self) -> None:
        self.collection.update_one(
            {"_id": "main_counter"},
            {"$inc": {"count": 1}}
        )

    def get_value(self) -> int:
        doc = self.collection.find_one({"_id": "main_counter"})
        return doc.get("count", 0) if doc else 0

    def reset(self) -> None:
        self.collection.update_one(
            {"_id": "main_counter"},
            {"$set": {"count": 0}}
        )
