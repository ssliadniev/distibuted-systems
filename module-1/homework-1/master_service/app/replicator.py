import grpc
import asyncio
import logging

from . import log_pb2, log_pb2_grpc

logger = logging.getLogger("uvicorn")


class GrpcReplicator:
    def __init__(self, secondaries: list[str], timeout: int):
        self.secondaries = secondaries
        self.timeout = timeout

    async def _replicate_single(self, host: str, message: str) -> bool:
        """
        Sends gRPC request to one secondary.
        """
        try:
            logger.info(f"[Replicator] Sending '{message}' to {host}...")
            async with grpc.aio.insecure_channel(host) as channel:
                stub = log_pb2_grpc.ReplicationServiceStub(channel)
                request = log_pb2.LogMessage(content=message)

                response = await stub.AppendMessage(request, timeout=self.timeout)

                if response.success:
                    logger.info(f"[Replicator] ACK from {host}")
                    return True

                return False
        except grpc.RpcError as error:
            logger.error(f"[Replicator] Failed connecting to {host}: {error}")
            return False

    async def replicate_to_all(self, message: str) -> bool:
        """
        Broadcasts to all secondaries and waits for all ACKs.
        """
        if not self.secondaries:
            return True

        tasks = [self._replicate_single(host, message) for host in self.secondaries]
        results = await asyncio.gather(*tasks)

        return all(results)
