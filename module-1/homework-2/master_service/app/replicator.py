import grpc
import asyncio
import logging

from . import log_pb2, log_pb2_grpc

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

    async def replicate_message(self, msg_id: int, message: str, write_concern: int) -> bool:
        """
        Orchestrates the replication process to satisfy the requested write concern.
        """
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
            asyncio.create_task(self._replicate_single(host, msg_id, message))
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

    async def _replicate_single(self, host: str, msg_id: int, message: str) -> bool:
        """
        Sends a single gRPC append request to a specific secondary host.
        """
        try:
            logger.info(f"[Replicator] Sending ID={msg_id} to {host}...")
            async with grpc.aio.insecure_channel(host) as channel:
                stub = log_pb2_grpc.ReplicationServiceStub(channel)
                request = log_pb2.LogMessage(id=msg_id, content=message)

                response = await stub.AppendMessage(request, timeout=self.timeout)
                return response.success

        except (grpc.RpcError, asyncio.TimeoutError) as error:
            logger.error(f"[Replicator] Failed to replicate to {host}: {error}")
            return False
