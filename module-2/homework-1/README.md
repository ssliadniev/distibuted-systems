# Web Counter Performance Benchmark

A high-performance Web Counter application built with **FastAPI** designed to demonstrate and compare the throughput of different storage strategies (In-Memory vs. On-Disk) under concurrent load.

The project includes a server application that handles concurrent requests and a benchmarking tool that simulates multiple clients to measure Requests Per Second (RPS) and verify thread safety.

## 📂 Project Structure

```text
.
├── web_counter/          # Server Source Code
│   ├── main.py           # Application entry point
│   ├── routes.py         # API Endpoints (/inc, /count, /reset)
│   └── storage.py        # Storage logic (Memory & Disk implementations)
├── benchmark.py          # Python benchmarking client
├── docker-compose.yml    # Container orchestration
├── Dockerfile            # Server container definition
└── requirements.txt      # Python dependencies
```

---

## 🚀 Features
1. Two Storage Engines:
    * In-Memory: Ultra-fast, volatile storage using Python variables.

    * On-Disk: Persistent storage using file I/O with os.fsync to ensure data durability.

2. Thread Safety: Implements robust locking mechanisms (Mutex) to prevent race conditions and "lost updates" during concurrent access.

3. Benchmark Tool: automated Python script to simulate high-concurrency loads (up to 10 parallel clients).

---

## 🛠️ Quick Start (Docker)
This is the recommended way to run the project.

1. Run In-Memory Benchmark (Part I)
    This runs the server using RAM storage. High throughput is expected.
    ```bash
    # Linux / macOS
    STORAGE_TYPE=memory docker compose up -d --build
    
    # Windows (PowerShell)
    $Env:STORAGE_TYPE="memory"; docker compose up -d --build
    ```

2. Run On-Disk Benchmark (Part II)
   This runs the server using File System storage. Throughput will be lower due to I/O latency.
    ```bash
    # Linux / macOS
    STORAGE_TYPE=disk docker compose up -d --build
    
    # Windows (PowerShell)
    $Env:STORAGE_TYPE="disk"; docker compose up -d --build
    ```
   
Note: The benchmark will run automatically once the server is healthy. Check the logs of the benchmark container for results.