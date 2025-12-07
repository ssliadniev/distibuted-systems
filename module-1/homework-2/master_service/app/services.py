import logging
import threading

from fastapi import HTTPException

from .replicator import GrpcReplicator
from .config import settings

logger = logging.getLogger("uvicorn")


class MessageService:
    """
    Service for managing the write-ahead log and coordinating replication with write concern.
    """

    _messages: list[str] = []
    _messages_lock = threading.Lock()

    _id_counter = 0
    _id_lock = threading.Lock()

    _replicator = GrpcReplicator(settings.secondary_hosts, settings.timeout)

    def _get_next_id(self) -> int:
        """
        Increments and returns the next unique message ID.
        """
        with self._id_lock:
            self._id_counter += 1
            return self._id_counter

    async def append_message(self, content: str, write_concern: int) -> int:
        """
        Appends a message to the log with a specific write concern.

        Args:
            content (str): The message payload.
            write_concern (int): Total number of nodes that must acknowledge to write.

        Returns:
            int: The assigned Message ID.

        Raises:
            HTTPException: If the write concern cannot be met (500).
        """
        msg_id = self._get_next_id()

        with self._messages_lock:
            self._messages.append(content)

        success = await self._replicator.replicate_message(msg_id, content, write_concern)

        if not success:
            raise HTTPException(
                status_code=500,
                detail="Write concern not satisfied. Message persisted on Master only."
            )

        return msg_id

    def get_messages(self) -> list:
        """
        Retrieves all messages committed to the Master's log.
        """
        with self._messages_lock:
            stored_messages = list(self._messages)
            logger.info(f"[Message Service] Send stored messages: {stored_messages}")
            return stored_messages
