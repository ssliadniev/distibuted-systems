import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from routes import router
from storage import DiskStorage, InMemoryStorage, MongoStorage

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
    elif storage_type == "mongo":
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        app.state.storage = MongoStorage(uri=mongo_uri)
    else:
        app.state.storage = InMemoryStorage()

    yield
    logger.info("Shutting down storage...")


app = FastAPI(lifespan=lifespan)
app.include_router(router)
