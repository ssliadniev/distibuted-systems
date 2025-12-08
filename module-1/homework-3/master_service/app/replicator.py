import grpc
import asyncio
import logging

from . import log_pb2, log_pb2_grpc
from .enums import NodeStatus

logger = logging.getLogger("uvicorn")


class GrpcReplicator:
    """
    Manages gRPC communication with Secondary nodes to replicate messages.
    """

    def __init__(self, secondaries: list[str], timeout: int):
        """
        Args:
            secondaries (List[str]): A list of host addresses (e.g., "secondary-1:50051").
            timeout (int): The request timeout in seconds for gRPC calls.
        """
        self.secondaries = secondaries
        self.timeout = timeout

        self.health_status: dict[str, NodeStatus] = {host: NodeStatus.HEALTHY for host in secondaries}

    async def _heartbeat_monitor(self):
        """
        Periodically checks the health of all secondaries.
        """
        while True:
            for host in self.secondaries:
                try:
                    async with grpc.aio.insecure_channel(host) as channel:
                        stub = log_pb2_grpc.ReplicationServiceStub(channel)
                        await stub.Heartbeat(log_pb2.Empty(), timeout=1)

                        if self.health_status[host] != NodeStatus.HEALTHY:
                            logger.info(f"[Health] Node {host} recovered -> HEALTHY")
                        self.health_status[host] = NodeStatus.HEALTHY
                except Exception:
                    if self.health_status[host] == NodeStatus.HEALTHY:
                        self.health_status[host] = NodeStatus.SUSPECTED
                        logger.warning(f"[Health] Node {host} suspected...")
                    elif self.health_status[host] == NodeStatus.SUSPECTED:
                        self.health_status[host] = NodeStatus.UNHEALTHY
                        logger.error(f"[Health] Node {host} marked UNHEALTHY")

            await asyncio.sleep(5)

    def get_quorum_status(self) -> bool:
        """
        Returns True if enough nodes are healthy to accept writes.
        """
        total_nodes = len(self.secondaries) + 1
        quorum_needed = (total_nodes // 2) + 1

        healthy_count = 1 + sum(1 for s in self.health_status.values() if self.health_status[s] == NodeStatus.HEALTHY)

        return healthy_count >= quorum_needed

    async def replicate_message(self, msg_id: int, message: str, write_concern: int) -> bool:
        """
        Orchestrates the replication process to satisfy the requested write concern.
        """
        if not self.get_quorum_status():
            logger.error("Quorum lost! Switching to Read-Only mode.")
            return False

        target_acks = self._calculate_target_acks(write_concern)
        tasks = self._spawn_replication_tasks(msg_id, message)

        return await self._wait_for_acks(tasks, target_acks)

    def _calculate_target_acks(self, write_concern: int) -> int:
        """
        Determines the number of remote ACKs required based on write concern.
        """
        total_secondaries = len(self.secondaries)
        target = write_concern - 1

        if target <= 0:
            return 0

        if target > total_secondaries:
            logger.warning(
                f"Write concern {write_concern} cannot be satisfied "
                f"(only {total_secondaries} secondaries). Capping requirements."
            )
            return total_secondaries

        return target

    def _spawn_replication_tasks(self, msg_id: int, message: str) -> list[asyncio.Task]:
        """
        Creates and starts asynchronous replication tasks for all secondaries.
        """
        return [
            asyncio.create_task(self._replicate_with_retry(host, msg_id, message))
            for host in self.secondaries
        ]

    @staticmethod
    async def _wait_for_acks(tasks: list[asyncio.Task], target_acks: int) -> bool:
        """
        Waits for a specific number of tasks to complete successfully.
        """
        if target_acks == 0:
            return True

        acks_received = 0
        for future in asyncio.as_completed(tasks):
            is_success = await future

            if is_success:
                acks_received += 1

            if acks_received >= target_acks:
                return True

        logger.error(f"Write concern failed. Wanted {target_acks}, got {acks_received}")
        return False

    async def _replicate_with_retry(self, host: str, msg_id: int, message: str) -> bool:
        """
        Sends a single gRPC append request to a specific secondary host.
        """
        attempt = 0
        while True:
            try:
                if self.health_status[host] == NodeStatus.UNHEALTHY:
                    await asyncio.sleep(5)

                async with grpc.aio.insecure_channel(host) as channel:
                    stub = log_pb2_grpc.ReplicationServiceStub(channel)
                    request = log_pb2.LogMessage(id=msg_id, content=message)
                    await stub.AppendMessage(request, timeout=self.timeout)
                    return True

            except Exception as error:
                attempt += 1
                logger.warning(f"[Retry] Failed to send {msg_id} to {host} (Attempt {attempt}): {error}")
                await asyncio.sleep(1)
