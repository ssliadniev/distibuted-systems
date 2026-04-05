import logging
import os
import sys
import time

from concurrent.futures import ThreadPoolExecutor

from neo4j import GraphDatabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def get_neo4j_driver():
    """
    Establishes connection with an increased retry timeout for heavy lock contention.
    """

    uri = os.getenv("NEO4J_URI", "neo4j://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")

    max_retries = 30

    for attempt in range(max_retries):
        try:
            driver = GraphDatabase.driver(
                uri,
                auth=(user, password),
                max_transaction_retry_time=120.0
            )
            driver.verify_connectivity()
            logger.info("Successfully connected to Neo4j for Benchmark!")
            return driver
        except Exception:
            logger.warning(f"Neo4j not ready yet. Retrying in 5 seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(5)

    return None


def setup_likes(driver):
    """
    Initializes the likes field on an existing item.
    """

    with driver.session() as session:
        session.run("MERGE (i:Item {item_id: 'I1'}) SET i.likes = 0")
        logger.info(msg="Initialized 'likes' counter to 0 for Item I1.")


def increment_likes(driver, client_id, calls):
    """
    Function to increment likes transactionally.
    """

    logger.info(msg=f"Client {client_id} starting {calls} calls...")
    query = "MATCH (i:Item {item_id: 'I1'}) SET i.likes = i.likes + 1"

    for _ in range(calls):
        with driver.session() as session:
            session.execute_write(lambda tx: tx.run(query))

    logger.info(msg=f"Client {client_id} finished.")


def run_benchmark():
    driver = get_neo4j_driver()

    driver.verify_connectivity()

    setup_likes(driver)

    clients = 10
    calls_per_client = 10000
    expected_total = clients * calls_per_client

    logger.info(msg="=========================================")
    logger.info(msg="STARTING NEO4J CONCURRENCY TEST")
    logger.info(msg=f"Clients: {clients} | Calls per client: {calls_per_client} | Total expected: {expected_total}")
    logger.info(msg="=========================================")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=clients) as executor:
        for i in range(clients):
            executor.submit(increment_likes, driver, i + 1, calls_per_client)

    end_time = time.time()
    duration = end_time - start_time

    with driver.session() as session:
        result = session.run("MATCH (i:Item {item_id: 'I1'}) RETURN i.likes AS final_likes").single()
        final_count = result["final_likes"] if result else 0

    logger.info(msg="=========================================")
    logger.info(msg="BENCHMARK PROTOCOL RESULTS")
    logger.info(msg=f"Final Likes Count: {final_count} (Expected: {expected_total})")

    if final_count == expected_total:
        logger.info(msg=">> SUCCESS: No lost updates!")
    else:
        logger.error(msg=">> FAILED: Data loss detected.")

    logger.info(msg=f"Time Taken: {duration} seconds")
    logger.info(msg=f"Throughput: {expected_total / duration} req/sec")
    logger.info(msg="=========================================")

    driver.close()


if __name__ == "__main__":
    run_benchmark()
