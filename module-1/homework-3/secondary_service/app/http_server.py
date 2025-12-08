from fastapi import FastAPI

from .storage import storage

http_app = FastAPI(title="Secondary Node")


@http_app.get(path="/api/messages", tags=["API Messages"])
async def get_messages():
    return {"messages": storage.get_all()}
