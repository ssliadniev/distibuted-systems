import os
import threading

from abc import ABC, abstractmethod


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
