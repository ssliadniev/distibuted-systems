import threading


class MessageStorage:
    """
    Simple in-memory storage manager for messages.
    """

    def __init__(self):
        self._messages: list[str] = []
        self._lock = threading.Lock()

    def add_message(self, msg: str):
        with self._lock:
            self._messages.append(msg)

    def get_all(self):
        with self._lock:
            return list(self._messages)


storage = MessageStorage()
