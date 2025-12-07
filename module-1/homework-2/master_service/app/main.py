from fastapi import FastAPI

from .routes import router
from .config import settings

app = FastAPI(title="Master node")
app.include_router(router, prefix="/api")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.port)
