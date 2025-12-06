# Replicated Log - Iteration 1

This first iteration of a **Replicated Log** with one **Master** node and multiple **Secondary** nodes.

- **Master** exposes an HTTP API for clients:
  - `POST /api/messages` – append a message to the log
  - `GET /api/messages` – list all committed messages
- **Secondaries** expose:
  - a **gRPC** server used by the Master to replicate messages
  - an **HTTP** endpoint `GET /api/messages` to read the replicated log

Replication is **blocking**: a `POST` request on the Master completes **only after all Secondaries acknowledge (ACK)** the message.

Everything runs in Docker via `docker-compose`.

---

## Architecture

**Languages & Tech**

- Python 3.12 (base Docker image)
- FastAPI + Uvicorn – HTTP APIs for Master and Secondaries
- gRPC (`grpcio`, `grpcio-tools`) – Master → Secondary replication
- Pydantic / pydantic-settings – configuration & request validation
- Docker / docker-compose – runtime

**High-level flow**

1. Client sends `POST /api/messages` to the **Master**.
2. Master uses `GrpcReplicator` to send the message via gRPC to all configured Secondaries (`ReplicationService.AppendMessage`).
3. Each Secondary:
   - simulates an optional delay (configured via `DELAY_SECONDS`)
   - appends the message to its in-memory storage
   - returns `Ack(success = true)` back to the Master.
4. Only when **all** ACKs are received:
   - Master appends the message to its own in-memory list
   - HTTP request returns `200 OK` with `{ "status": "success", ... }`.
5. If any Secondary fails or times out:
   - Master returns HTTP `500` with `Replication failed. Message not committed.`
   - Message is **not** appended on the Master.

Secondaries also expose `GET /api/messages` so you can verify that replication works.

---

## Repository Structure

```text
.
├── master_service
│   ├── app
│   │   ├── config.py        # Settings for Master (SECONDARIES, TIMEOUT, PORT)
│   │   ├── main.py          # FastAPI app + uvicorn entry point
│   │   ├── routes.py        # HTTP routes (/api/messages)
│   │   ├── schemas.py       # Pydantic models for requests/responses
│   │   ├── services.py      # MessageService + in-memory log with locks
│   │   └── replicator.py    # GrpcReplicator: sends messages to all Secondaries
│   ├── Dockerfile
│   └── requirements.txt
├── secondary_service
│   ├── app
│   │   ├── config.py        # Settings for Secondary (DELAY_SECONDS)
│   │   ├── main.py          # Runs HTTP + gRPC servers concurrently
│   │   ├── grpc_server.py   # ReplicationService implementation (AppendMessage)
│   │   ├── http_server.py   # HTTP GET /api/messages
│   │   └── storage.py       # Thread-safe in-memory storage
│   ├── Dockerfile
│   └── requirements.txt
├── protos
│   └── log.proto            # gRPC ReplicationService definition
├── .env.example             # Example configuration file
├── .env                     # Real configuration file
├── docker-compose.yml       # 1 Master + 2 Seconds (secondary-1, secondary-2)
└── README.md
```

---

## How to run?

```bash
  make build_and_run
```

## How to test?

Master API swagger:
```text
http://0.0.0.0:8000/docs
```

Secondaries API swagger:
```text
http://0.0.0.0:8001/docs
http://0.0.0.0:8002/docs
```