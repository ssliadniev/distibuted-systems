import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from routes import router
from storage import DiskStorage, InMemoryStorage, PostgresStorage

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("server")


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage_type = os.getenv("STORAGE_TYPE", "memory")

    logger.info(f"Initializing storage mode: {storage_type.upper()}")

    if storage_type == "disk":
        app.state.storage = DiskStorage()
    elif storage_type == "postgres":
        dsn = (
            f"dbname={os.getenv('POSTGRES_DB')} "
            f"user={os.getenv('POSTGRES_USER')} "
            f"password={os.getenv('POSTGRES_PASSWORD')} "
            f"host={os.getenv('DB_HOST')} "
            f"port={os.getenv('DB_PORT')}"
        )
        app.state.storage = PostgresStorage(dsn)
    else:
        app.state.storage = InMemoryStorage()

    yield
    logger.info("Shutting down storage...")

    if isinstance(app.state.storage, PostgresStorage):
        app.state.storage.close()


app = FastAPI(lifespan=lifespan)
app.include_router(router)
