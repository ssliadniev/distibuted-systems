import argparse
import logging
import sys

from neo4j_repo import StoreInterface
from database import get_neo4j_driver
from benchmark import run_benchmark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def run_queries():
    driver = get_neo4j_driver()
    try:
        repo = StoreInterface(driver)
        repo.setup_and_seed()
        repo.run_queries()
    finally:
        driver.close()


def main():
    logger.info("=========================================")
    logger.info("STARTING NEO4J PROTOCOL EXECUTION")
    logger.info("=========================================")

    driver = get_neo4j_driver()

    try:
        repo = StoreInterface(driver)
        repo.setup_and_seed()
        repo.run_queries()
    except Exception as error:
        logger.error(f"Execution failed: {error}")
    finally:
        driver.close()

    logger.info("=========================================")
    logger.info("PROTOCOL EXECUTION FINISHED")
    logger.info("=========================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Neo4j Task Execution")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["queries", "benchmark"],
        default="queries",
        help="Choose which part of the task to execute: 'queries' (Part 1) or 'benchmark' (Part 2)"
    )

    args = parser.parse_args()

    if args.mode == "benchmark":
        logger.info("--- Launching Benchmark Mode ---")
        run_benchmark()
    else:
        logger.info("--- Launching Standard Queries Mode ---")
        run_queries()
