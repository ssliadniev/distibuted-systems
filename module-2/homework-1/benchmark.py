import logging
import multiprocessing
import os
import sys
import time

import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("benchmark")


SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8000")
REQUESTS_PER_CLIENT = 10_000


def worker_task(client_id):
    """
    Function executed by each worker process.
    """
    with requests.Session() as session:
        for _ in range(REQUESTS_PER_CLIENT):
            try:
                response = session.post(f"{SERVER_URL}/inc")
                if response.status_code != 200:
                    logger.error(f"Client {client_id} error: status {response.status_code}")
            except requests.exceptions.RequestException as error:
                logger.error(f"Client {client_id} connection failed: {error}")


def run_benchmark(num_clients):
    logger.info(f"--- Starting benchmark with {num_clients} client(s) ---")
    expected_count = num_clients * REQUESTS_PER_CLIENT
    logger.info(f"Target total requests: {expected_count}")

    try:
        requests.post(f"{SERVER_URL}/reset")
    except Exception as error:
        logger.critical(f"Failed to reset server: {error}")
        return

    processes = []
    for i in range(num_clients):
        p = multiprocessing.Process(target=worker_task, args=(i,))
        processes.append(p)

    start_time = time.time()

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    end_time = time.time()

    total_time = end_time - start_time

    try:
        final_count_resp = requests.get(f"{SERVER_URL}/count").json()
        final_count = final_count_resp["count"]
    except Exception as error:
        logger.error(f"Could not fetch final count: {error}")
        final_count = -1

    rps = final_count / total_time if total_time > 0 else 0

    logger.info(f"Time taken: {total_time:.4f} seconds")
    logger.info(f"Final counter value: {final_count} (Expected: {expected_count})")
    logger.info(f"Throughput: {rps:.2f} req/sec")

    if final_count != expected_count:
        logger.warning("!! WARNING: LOST UPDATES DETECTED !!")
    else:
        logger.info(">> Success: no lost updates.")


if __name__ == "__main__":
    logger.info(f"Connecting to server at {SERVER_URL}...")

    server_ready = False
    for _ in range(10):
        try:
            requests.get(f"{SERVER_URL}/count")
            server_ready = True
            break
        except requests.exceptions.ConnectionError:
            logger.warning("Waiting for server...")
            time.sleep(1)

    if not server_ready:
        logger.critical("Server is not reachable. Exiting.")
        sys.exit(1)

    scenarios = [1, 2, 5, 10]

    for clients in scenarios:
        run_benchmark(clients)
