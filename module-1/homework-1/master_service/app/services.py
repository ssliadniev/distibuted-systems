import threading

from fastapi import HTTPException

from .replicator import GrpcReplicator
from .config import settings


class MessageService:
    _messages: list[str] = []
    _messages_lock = threading.Lock()
    _replicator = GrpcReplicator(settings.secondary_hosts, settings.timeout)

    async def append_message(self, content: str):
        success = await self._replicator.replicate_to_all(content)

        if not success:
            raise HTTPException(status_code=500, detail="Replication failed. Message not committed.")

        with self._messages_lock:
            self._messages.append(content)

        return content

    def get_messages(self):
        with self._messages_lock:
            return list(self._messages)
