import logging
import os
import time

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

logger = logging.getLogger(__name__)


def get_cassandra_session():
    """
    Connects to Cassandra with a retry mechanism.
    """

    host = os.getenv("CASSANDRA_HOST", "cassandra")

    max_retries = 30
    for attempt in range(max_retries):
        try:
            cluster = Cluster([host])
            session = cluster.connect()
            session.row_factory = dict_factory
            logger.info("Successfully connected to Cassandra!")
            return cluster, session
        except Exception as error:
            logger.warning(f"Cassandra not ready yet. Retrying in 10 seconds... "
                           f"(Attempt {attempt + 1}/{max_retries})")
            time.sleep(10)

    raise Exception("Could not connect to Cassandra.")
