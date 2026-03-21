import logging
import sys

from datetime import datetime
from typing import List

from bson.objectid import ObjectId
from pymongo.database import Database

from database import get_database
from interfaces.items import ItemsInterface
from interfaces.orders import OrdersInterface
from interfaces.reviews import ReviewsInterface

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(module)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


def execute_items_tasks(db: Database) -> List[ObjectId]:
    """
    Executes all tasks related to the items collection.
    """

    logger.info(msg="---- STARTING TASK: ITEMS ----")
    items_interface = ItemsInterface(db)
    inserted_ids = items_interface.seed_items()

    items_interface.show_all_items()
    items_interface.count_by_category("Phone")
    items_interface.count_distinct_categories()
    items_interface.list_distinct_producers()
    items_interface.complex_queries()
    items_interface.update_and_increase_price()

    return inserted_ids


def execute_orders_tasks(db: Database, inserted_ids: List[ObjectId]) -> None:
    """
    Executes all tasks related to the orders collection.
    """

    logger.info(msg="---- STARTING TASK: ORDERS ----")
    orders_interface = OrdersInterface(db)
    orders_interface.seed_orders(inserted_ids)

    orders_interface.print_all_orders()
    orders_interface.orders_above_sum(2000)
    orders_interface.find_by_customer("Rodinov")

    orders_interface.find_orders_with_item(inserted_ids[0])
    orders_interface.update_orders_with_item(
        search_item_id=inserted_ids[0],
        new_item_id=inserted_ids[4],
        sum_increase=500.0
    )

    orders_interface.projected_customer_info(2000)

    orders_interface.delete_item_from_recent_orders(
        item_to_remove=inserted_ids[1],
        start_date=datetime(year=2015, month=1, day=1),
        end_date=datetime(year=2015, month=12, day=31)
    )

    orders_interface.rename_customer_field()
    orders_interface.join_items_to_order(201513)


def execute_reviews_tasks(db: Database) -> None:
    """
    Executes tasks related to the capped collection.
    """

    logger.info(msg="---- STARTING TASK: CAPPED COLLECTION ----")
    reviews_repo = ReviewsInterface(db)
    reviews_repo.test_capped_collection()


def main():
    logger.info(msg="STARTING TASK 4 (PART 1) EXECUTION")
    logger.info(msg="======================================")

    db = get_database()

    inserted_ids = execute_items_tasks(db)
    execute_orders_tasks(db, inserted_ids)
    execute_reviews_tasks(db)

    logger.info(msg="=====================================")
    logger.info(msg="TASK EXECUTION FINISHED")
    logger.info(msg="=======================================")


if __name__ == "__main__":
    main()
