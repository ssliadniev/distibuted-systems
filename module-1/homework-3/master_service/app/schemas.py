from pydantic import BaseModel
from typing import List


class MessageCreate(BaseModel):
    message: str
    write_concern: int = 1


class MessageResponse(BaseModel):
    id: int
    message: str
    status: str


class MessageList(BaseModel):
    messages: List[str]
