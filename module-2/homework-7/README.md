## Prerequisites
* Docker
* Docker Compose

## Project Structure
* `docker-compose.yml`: Defines the 3-node MongoDB cluster and the Python client container.
* `part1_replication.py`: Simulates timeout scenarios, replica set elections, eventual consistency, and data rollbacks.
* `part2_performance.py`: Simulates high-concurrency writes using `findOneAndUpdate` to test data consistency during failovers.
* `requirements.txt`: Python dependencies (PyMongo).
* `Dockerfile`: Containerizes the Python client to run inside the same network as the database.

---

## Setup & Initialization

**Start the MongoDB Cluster**
Open your terminal in the project directory and run:
```bash
docker compose up -d --build
```

## Running Part I: Replication Scenarios
This script is interactive. You will need two terminal windows:

Terminal 1: Runs the Python script.
Terminal 2: Used to execute docker compose commands (kill/start) when prompted by the script.

To run the test:
```bash
docker compose run --rm -it app python part1_replication.py
```

## Running Part II: Performance & Integrity Analysis
This script launches 10 concurrent clients performing 10,000 updates each. 
It tests both w=1 and w=majority write concerns, including failover scenarios.

To run the test:
```bash
docker compose run --rm -it app python part2_performance.py
```