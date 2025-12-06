import asyncio
import logging

import grpc

from . import log_pb2, log_pb2_grpc
from .config import settings
from .storage import storage

logger = logging.getLogger("uvicorn")


class ReplicationService(log_pb2_grpc.ReplicationServiceServicer):
    """
    gRPC ReplicationService defined in log.proto.
    """

    async def AppendMessage(self, request, context):
        delay = settings.delay_seconds

        logger.info(f"[gRPC] Received append: '{request.content}'")
        if delay > 0:
            logger.info(f"[gRPC] Sleeping for {delay} seconds...")
            await asyncio.sleep(delay)

        storage.add_message(request.content)
        logger.info("[gRPC] Message committed.")

        return log_pb2.Ack(success=True)


async def start_grpc_server():
    server = grpc.aio.server()
    log_pb2_grpc.add_ReplicationServiceServicer_to_server(ReplicationService(), server)
    server.add_insecure_port('[::]:50051')

    logger.info("gRPC Server listening on port 50051")
    await server.start()
    await server.wait_for_termination()
