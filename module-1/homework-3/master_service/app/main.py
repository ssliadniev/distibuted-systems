from fastapi import FastAPI
from contextlib import asynccontextmanager

from .routes import router, message_service
from .config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager to handle startup and shutdown events.
    """
    await message_service.start_background_tasks()
    yield


app = FastAPI(title="Master node", lifespan=lifespan)
app.include_router(router, prefix="/api")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.port)
