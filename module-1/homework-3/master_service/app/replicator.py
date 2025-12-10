import grpc
import asyncio
import logging

from typing import Optional

from . import log_pb2, log_pb2_grpc
from .enums import NodeStatus

logger = logging.getLogger("uvicorn")


class GrpcReplicator:
    """
    Manages gRPC communication with Secondary nodes to replicate messages.
    with eventual consistency, infinite retries, and health monitoring.
    """

    HEARTBEAT_INTERVAL = 5
    HEARTBEAT_TIMEOUT = 1
    RETRY_INITIAL_BACKOFF = 1
    RETRY_MAX_BACKOFF = 30

    def __init__(self, secondaries: list[str], timeout: int):
        """
        Args:
            secondaries (List[str]): A list of host addresses (e.g., "secondary-1:50051").
            timeout (int): The request timeout in seconds for gRPC calls.
        """
        self.secondaries = secondaries
        self.timeout = timeout

        self.health_status: dict[str, NodeStatus] = {
            host: NodeStatus.HEALTHY for host in secondaries
        }
        self._background_tasks: list[asyncio.Task] = []

    async def start(self):
        """
        Starts background maintenance tasks.
        """
        logger.info("[Replicator] Starting heartbeat monitor...")
        task = asyncio.create_task(self._heartbeat_monitor())
        self._background_tasks.append(task)

    async def stop(self):
        """
        Stops all background tasks gracefully.
        """
        logger.info("[Replicator] Stopping background tasks...")
        for task in self._background_tasks:
            task.cancel()

        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    async def get_health(self) -> dict[str, str]:
        """
        Returns a snapshot of the cluster health.
        """
        return {host: status.value for host, status in self.health_status.items()}

    def get_quorum_status(self) -> bool:
        """
        Returns True if (healthy nodes + master) > (total nodes / 2).
        """
        total_nodes = len(self.secondaries) + 1
        quorum_needed = (total_nodes // 2) + 1

        healthy_count = 1 + sum(
            1 for status in self.health_status.values()
            if status == NodeStatus.HEALTHY
        )

        return healthy_count >= quorum_needed

    async def replicate_message(self, msg_id: int, message: str, write_concern: int) -> bool:
        """
        Orchestrates the replication process to satisfy the requested write concern.
        """
        if not self.get_quorum_status():
            logger.error("[Replicator] Quorum lost! Switching to Read-Only mode.")
            return False

        target_acks = self._calculate_target_acks(write_concern)
        tasks = self._spawn_replication_tasks(msg_id, message)

        return await self._wait_for_acks(tasks, target_acks)

    async def _heartbeat_monitor(self):
        """
        Periodically checks the health of secondaries nodes.
        """
        while True:
            await asyncio.gather(*(self._check_single_node(host) for host in self.secondaries))
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)

    async def _check_single_node(self, host: str):
        """
        Performs a single heartbeat check and updates status.
        """
        try:
            async with grpc.aio.insecure_channel(host) as channel:
                stub = log_pb2_grpc.ReplicationServiceStub(channel)
                await stub.Heartbeat(log_pb2.Empty(), timeout=self.HEARTBEAT_TIMEOUT)

                if self.health_status[host] != NodeStatus.HEALTHY:
                    logger.info(f"[Heartbeat] Node {host} recovered -> HEALTHY")
                self.health_status[host] = NodeStatus.HEALTHY

        except Exception:
            current = self.health_status[host]
            if current == NodeStatus.HEALTHY:
                self.health_status[host] = NodeStatus.SUSPECTED
                logger.warning(f"[Heartbeat] Node {host} is SUSPECTED...")
            elif current == NodeStatus.SUSPECTED:
                self.health_status[host] = NodeStatus.UNHEALTHY
                logger.error(f"[Heartbeat] Node {host} marked UNHEALTHY")

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
                f"[Replicator] Write concern {write_concern} cannot be satisfied "
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
            if await future:
                acks_received += 1

            if acks_received >= target_acks:
                return True

        return False

    async def _replicate_with_retry(self, host: str, msg_id: int, message: str) -> Optional[bool]:
        """
        Sends a single gRPC append request to a specific secondary host.
        Retries indefinitely with exponential backoff until success.
        """
        attempt = 0
        backoff = self.RETRY_INITIAL_BACKOFF

        while True:
            try:
                if self.health_status[host] == NodeStatus.UNHEALTHY:
                    logger.info(f"[Replicator] Node {host} is UNHEALTHY. Waiting...")
                    await asyncio.sleep(5)
                    continue

                logger.info(f"[Replicator] Sending ID={msg_id} to {host} (Attempt {attempt})...")
                async with grpc.aio.insecure_channel(host) as channel:
                    stub = log_pb2_grpc.ReplicationServiceStub(channel)
                    request = log_pb2.LogMessage(id=msg_id, content=message)

                    await stub.AppendMessage(request, timeout=self.timeout)
                    logger.info(f"[Replicator] Success: Replicated ID={msg_id} to {host}")

                    return True

            except Exception as error:
                attempt += 1
                logger.warning(f"[Replicator]  Failed to send {msg_id} to {host}: {error}. Retrying in {backoff}s...")

                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.RETRY_MAX_BACKOFF)
