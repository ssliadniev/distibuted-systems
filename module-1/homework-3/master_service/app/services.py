import asyncio
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

    def __init__(self):
        self._messages: list[str] = []
        self._id_counter: int = 0

        self._lock = threading.Lock()
        self._replicator = GrpcReplicator(settings.secondary_hosts, settings.timeout)

    async def start_background_tasks(self):
        """
        Starts the replicator's background tasks.
        """
        await self._replicator.start()

    async def stop_background_tasks(self):
        """
        Stops background tasks gracefully.
        """
        await self._replicator.stop()

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
        if not self._replicator.get_quorum_status():
            raise HTTPException(
                status_code=503,
                detail="Quorum lost. Master is in Read-Only mode."
            )

        with self._lock:
            self._id_counter += 1
            msg_id = self._id_counter
            self._messages.append(content)

        logger.info(f"[Message Service] Assigned ID={msg_id} to '{content}'")

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
        with self._lock:
            stored_messages = list(self._messages)
            logger.info(f"[Message Service] Retrieve stored messages: {stored_messages}")
            return stored_messages

    def get_health(self):
        """
        Returns the health status of all nodes.
        """
        return {
            "master": "Healthy",
            "secondaries": asyncio.run(self._replicator.get_health()),
            "quorum": self._replicator.get_quorum_status()
        }
