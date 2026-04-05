import os
import time
import logging

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

logger = logging.getLogger(__name__)


def get_neo4j_driver():
    """
    Establishes connection to the Neo4j database.
    """

    uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")

    max_retries = 30
    for attempt in range(max_retries):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()
            logger.info(msg="Successfully connected to Neo4j!")

            return driver
        except (ServiceUnavailable, Exception):
            logger.warning(f"Neo4j not ready yet. Retrying in 5 seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(5)

    raise Exception("Could not connect to Neo4j.")
