import bisect
import logging
import threading

logger = logging.getLogger("uvicorn")


class MessageStorage:
    """
    Thread-safe in-memory storage manager.
    """

    def __init__(self):
        self._messages: dict[int, str] = {}
        self._sorted_ids: list[int] = []

        self._lock = threading.Lock()

    def add_message(self, msg_id: int, msg_content: str) -> bool:
        """
        Idempotently adds a message to storage, maintaining sorted order.

        Args:
            msg_id (int): The unique sequence ID of the message.
            msg_content (str): The content of the message.

        Returns:
            bool: True if the message was new and added; False if it was a duplicate.
        """
        with self._lock:
            if msg_id in self._messages:
                return False

            self._messages[msg_id] = msg_content
            bisect.insort(self._sorted_ids, msg_id)

            return True

    def get_all(self):
        """
        Retrieves all stored messages, strictly ordered by their ID.

        Returns:
            List[str]: A list of message strings sorted by their sequence ID.
        """
        with self._lock:
            if not self._sorted_ids:
                return []

            consistent_messages = []
            expected_id = 1

            for msg_id in self._sorted_ids:
                if msg_id == expected_id:
                    consistent_messages.append(self._messages[msg_id])
                    expected_id += 1
                else:
                    break

            logger.info(f"[Storage] Returning {len(consistent_messages)} sequential messages: {consistent_messages}")
            return consistent_messages


storage = MessageStorage()
