import os
import logging

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure


logger = logging.getLogger(__name__)


def get_database() -> Database:
    """
    Establishes a connection to the MongoDB cluster.

    Returns:
        Database: The connected MongoDB database instance.
    """
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    try:
        client: MongoClient = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        logger.info("Successfully connected to MongoDB.")

        return client["online_store"]
    except ConnectionFailure as error:
        logger.error(f"Failed to connect to MongoDB: {error}")
        raise
