from fastapi import APIRouter, Depends

from .schemas import MessageCreate, MessageResponse, MessageList
from .services import MessageService

router = APIRouter()


def get_service():
    return MessageService()


@router.post(path="/messages", tags=["API Messages"], response_model=MessageResponse)
async def append_message(
    data: MessageCreate,
    service: MessageService = Depends(get_service)
):
    await service.append_message(data.message)
    return {"status": "success", "message": data.message}


@router.get(path="/messages", tags=["API Messages"], response_model=MessageList)
async def list_messages(service: MessageService = Depends(get_service)):
    return {"messages": service.get_messages()}
