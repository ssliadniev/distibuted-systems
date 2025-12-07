import asyncio
import uvicorn
import logging

from .grpc_server import start_grpc_server
from .http_server import http_app

logging.basicConfig(level=logging.INFO)


async def start_http_server():
    config = uvicorn.Config(app=http_app, host="0.0.0.0", port=8000)
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    await asyncio.gather(
        start_grpc_server(),
        start_http_server()
    )

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
