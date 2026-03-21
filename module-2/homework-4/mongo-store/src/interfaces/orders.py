import logging

from datetime import datetime
from typing import List

from pymongo.collection import Collection
from pymongo.database import Database
from bson.json_util import dumps
from bson.objectid import ObjectId


logger = logging.getLogger(__name__)


class OrdersInterface:
    """
    Handling operations related to customer orders.
    """

    def __init__(self, db: Database):
        self.collection: Collection = db["orders"]

    def seed_orders(self, item_ids: List[ObjectId]) -> None:
        """
        Creates orders with cost, embedded customer and referenced items.
        Ensures one item is shared across multiple orders.
        """

        self.collection.delete_many({})
        orders = [
            {
                "order_number": 201513,
                "date": datetime(year=2015, month=4, day=14),
                "total_sum": 1923.4,
                "customer": {
                    "name": "Andrii",
                    "surname": "Rodinov",
                    "phones": [9876543, 1234567],
                    "address": "PTI, Peremohy 37, Kyiv, UA"
                },
                "payment": {"card_owner": "Andrii Rodionov", "cardId": 12345678},
                "items_id": [item_ids[0], item_ids[1]]
            },
            {
                "order_number": 201514,
                "date": datetime(year=2015, month=6, day=20),
                "total_sum": 2500.0,
                "customer": {
                    "name": "Ivan",
                    "surname": "Petrov",
                    "phones": [1112223],
                    "address": "Khreshchatyk 1, Kyiv, UA"
                },
                "payment": {"card_owner": "Ivan Petrov", "cardId": 87654321},
                "items_id": [item_ids[0], item_ids[3]]
            }
        ]
        self.collection.insert_many(orders)
        logger.info(msg=f"Seeded {len(orders)} orders.")

    def print_all_orders(self) -> None:
        """
        Outputs all orders.
        """

        logger.info(msg="\n---- Display all orders ----")
        logger.info(msg=f"\n{dumps(list(self.collection.find()), indent=2)}")

    def orders_above_sum(self, min_sum: float) -> None:
        """
        Finds orders with a cost greater than a specified value.
        """

        logger.info(msg=f"\n---- Orders with total_sum > {min_sum} ----")
        logger.info(msg=f"\n{dumps(list(self.collection.find({'total_sum': {'$gt': min_sum}})), indent=2)}")

    def find_by_customer(self, surname: str) -> None:
        """
        Finds orders made by a specific customer.
        """

        logger.info(msg=f"\n---- Orders for customer surname: {surname} ----")
        logger.info(msg=f"\n{dumps(list(self.collection.find({'customer.surname': surname})), indent=2)}")

    def find_orders_with_item(self, target_item_id: ObjectId) -> None:
        """
        Finds all orders containing a specific item by ObjectId.
        """

        logger.info(msg=f"\n---- Orders containing item ID: {target_item_id} ----")
        logger.info(msg=f"\n{dumps(list(self.collection.find({"items_id": target_item_id})), indent=2)}")

    def update_orders_with_item(self, search_item_id: ObjectId, new_item_id: ObjectId, sum_increase: float) -> None:
        """
        Adds a new item and increases total cost for orders containing a specific item.
        """

        logger.info(msg=f"\n---- Adding new item to orders containing {search_item_id} and "
                    f"increasing sum by {sum_increase} ----")
        self.collection.update_many(
            {"items_id": search_item_id},
            {
                "$push": {"items_id": new_item_id},
                "$inc": {"total_sum": sum_increase}
            }
        )

    def projected_customer_info(self, min_sum: float) -> None:
        """
        Outputs only customer info and credit card numbers for orders > a certain sum.
        """

        logger.info(msg=f"\n---- Projected customer/card info for orders > {min_sum} ----")
        projection = {"_id": 0, "customer": 1, "payment.cardId": 1}
        logger.info(msg=f"\n{dumps(list(self.collection.find({"total_sum": {"$gt": min_sum}}, projection)), indent=2)}")

    def delete_item_from_recent_orders(
            self, item_to_remove: ObjectId, start_date: datetime, end_date: datetime
    ) -> None:
        """
        Deletes a specific item from orders made within a date range.
        """

        logger.info(msg=f"\n---- Removing item {item_to_remove} from orders"
                    f" between {start_date.date()} and {end_date.date()} ----")
        self.collection.update_many(
            {"date": {"$gte": start_date, "$lte": end_date}},
            {"$pull": {"items_id": item_to_remove}}
        )

    def rename_customer_field(self) -> None:
        """
        Renames the customer name field in all orders.
        """

        logger.info("\n---- Renaming 'customer.name' to 'customer.first_name' globally ----")
        self.collection.update_many({}, {"$rename": {"customer.name": "customer.first_name"}})

    def join_items_to_order(self, order_number: int) -> None:
        """
        Replaces item ObjectIds with actual item names and prices and projects only customer surname and item details.
        """

        logger.info(msg=f"\n---- Order {order_number} details (join with items collection) ----")
        pipeline = [
            {"$match": {"order_number": order_number}},
            {"$lookup": {
                "from": "items",
                "localField": "items_id",
                "foreignField": "_id",
                "as": "resolved_items"
            }},
            {"$project": {
                "_id": 0,
                "customer_surname": "$customer.surname",
                "items": {
                    "$map": {
                        "input": "$resolved_items",
                        "as": "item",
                        "in": {"name": "$$item.model", "price": "$$item.price"}
                    }
                }
            }}
        ]
        result = list(self.collection.aggregate(pipeline))
        logger.info(msg=f"\n{dumps(result, indent=2)}")
