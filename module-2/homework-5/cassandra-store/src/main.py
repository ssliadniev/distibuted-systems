import logging
import sys

from database import get_cassandra_session
from items_repo import ItemsInterface
from orders_repo import OrdersInterface

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=========================================")
    logger.info("STARTING CASSANDRA TASK EXECUTION")
    logger.info("=========================================")

    cluster, session = get_cassandra_session()

    try:
        items_repo = ItemsInterface(cluster, session)
        items_repo.setup_schema()
        items_repo.seed_data()
        items_repo.describe_table()
        items_repo.run_queries()
        items_repo.update_item()

        orders_repo = OrdersInterface(cluster, session)
        orders_repo.setup_schema()
        orders_repo.seed_data()
        orders_repo.describe_table()
        orders_repo.run_queries()
        orders_repo.update_and_meta_queries()

    except Exception as error:
        logger.error(f"Execution failed: {error}")
    finally:
        cluster.shutdown()

    logger.info("=========================================")
    logger.info("TASK EXECUTION FINISHED")
    logger.info("=========================================")


if __name__ == "__main__":
    main()
