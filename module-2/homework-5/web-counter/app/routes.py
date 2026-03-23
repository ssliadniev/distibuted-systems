from fastapi import APIRouter, Depends, Request, Response
from storage import CounterStorage

router = APIRouter()


def get_storage(request: Request) -> CounterStorage:
    return request.app.state.storage


@router.post(path="/inc", tags=["API Web Counter"])
def increment_counter(storage: CounterStorage = Depends(get_storage)):
    storage.increment()
    return Response(status_code=200)


@router.get(path="/count", tags=["API Web Counter"])
def get_count(storage: CounterStorage = Depends(get_storage)):
    return {"count": storage.get_value()}


@router.post(path="/reset", tags=["API Web Counter"])
def reset_counter(storage: CounterStorage = Depends(get_storage)):
    storage.reset()
    return {"status": "reset"}
