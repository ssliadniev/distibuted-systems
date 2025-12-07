FROM python:3.12-slim

WORKDIR /app

COPY master_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY master_service/app ./app

COPY protos/ ./protos/

RUN python -m grpc_tools.protoc -I./protos --python_out=./app --grpc_python_out=./app ./protos/log.proto
RUN sed -i 's/import log_pb2/from . import log_pb2/' app/log_pb2_grpc.py

CMD ["python", "-m", "app.main"]
