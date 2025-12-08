from fastapi import APIRouter, Depends

from .schemas import MessageCreate, MessageResponse, MessageList
from .services import MessageService

router = APIRouter()
message_service = MessageService()


def get_service() -> MessageService:
    return message_service


@router.post(path="/messages", tags=["API Messages"], response_model=MessageResponse)
async def append_message(
    data: MessageCreate,
    service: MessageService = Depends(get_service)
):
    msg_id = await service.append_message(data.message, data.write_concern)
    return {"id": msg_id, "message": data.message, "status": "success"}


@router.get(path="/messages", tags=["API Messages"], response_model=MessageList)
async def list_messages(service: MessageService = Depends(get_service)):
    return {"messages": service.get_messages()}


@router.get("/health")
async def health_check(service: MessageService = Depends(get_service)):
    return service.get_health()
