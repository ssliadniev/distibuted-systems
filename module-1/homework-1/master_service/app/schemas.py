from pydantic import BaseModel
from typing import List


class MessageCreate(BaseModel):
    message: str


class MessageResponse(BaseModel):
    status: str
    message: str


class MessageList(BaseModel):
    messages: List[str]
