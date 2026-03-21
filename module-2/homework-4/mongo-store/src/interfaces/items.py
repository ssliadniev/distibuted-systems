import logging

from typing import List, Any

from pymongo.collection import Collection
from pymongo.database import Database
from bson.json_util import dumps


logger = logging.getLogger(__name__)


class ItemsInterface:
    """
    Handling CRUD operations and queries for store items.
    """

    def __init__(self, db: Database):
        self.collection: Collection = db["items"]

    def seed_items(self) -> List[Any]:
        """
        Clears the collection and inserts initial diverse items.
        """

        self.collection.delete_many({})
        items = [
            {"category": "Phone", "model": "iPhone 6", "producer": "Apple", "price": 600, "color": "Silver"},
            {"category": "Phone", "model": "Galaxy S23", "producer": "Samsung", "price": 900, "waterproof": True},
            {"category": "TV", "model": "OLED55", "producer": "LG", "price": 1200, "smart_tv": True},
            {"category": "Smart Watch", "model": "Watch Series 8", "producer": "Apple", "price": 400},
            {"category": "TV", "model": "Bravia", "producer": "Sony", "price": 1500, "resolution": "4K"}
        ]

        result = self.collection.insert_many(items)
        logger.info(msg=f"Seeded {len(result.inserted_ids)} items.")

        return result.inserted_ids

    def show_all_items(self) -> None:
        """
        Outputs all items formatted as JSON.
        """

        logger.info(msg="\n---- Display all items in JSON format ----")
        items = list(self.collection.find({}, {"_id": 0}))
        logger.info(msg=f"\n{dumps(items, indent=2)}")

    def count_by_category(self, category: str) -> None:
        """
        Counts how many items belong to a specific category.
        """

        count = self.collection.count_documents({"category": category})
        logger.info(msg=f"\n---- Total items in '{category}' category: {count} ----")

    def count_distinct_categories(self) -> None:
        """
        Counts the total number of unique categories.
        """

        categories = self.collection.distinct("category")
        logger.info(msg=f"\n---- Distinct categories count: {len(categories)} {categories} ----")

    def list_distinct_producers(self) -> None:
        """
        Outputs a list of all item producers without duplicates.
        """

        producers = self.collection.distinct("producer")
        logger.info(msg=f"\n---- Distinct producers: {producers} ----")

    def complex_queries(self) -> None:
        """
        Demonstrates complex $and, $or, and $in queries.
        """

        logger.info("\n---- Query ($and): category is 'Phone' AND price is between 500-1000 ----")
        and_query = {"$and": [{"category": "Phone"}, {"price": {"$gte": 500, "$lte": 1000}}]}
        logger.info(msg=f"\n{dumps(list(self.collection.find(and_query)), indent=2)}")

        logger.info("\n---- Query ($or): model is 'iPhone 6' OR 'OLED55' ----")
        or_query = {"$or": [{"model": "iPhone 6"}, {"model": "OLED55"}]}
        logger.info(msg=f"\n{dumps(list(self.collection.find(or_query)), indent=2)}")

        logger.info("\n---- Query ($in): producers are in ['Apple', 'Sony'] ----")
        in_query = {"producer": {"$in": ["Apple", "Sony"]}}
        logger.info(msg=f"\n{dumps(list(self.collection.find(in_query)), indent=2)}")

    def update_and_increase_price(self) -> None:
        """
        Updates specific fields, adds new ones and increments price based on field existence.
        """

        logger.info(msg="\n---- UpdateMany: modify existing 'color' and add 'warranty_months' to 'iPhone 6' ----")
        self.collection.update_many(
            {"model": "iPhone 6"},
            {"$set": {"color": "Space Gray", "warranty_months": 12}}
        )

        target_property = "smart_tv"
        items_to_update = list(self.collection.find({target_property: {"$exists": True}}))
        logger.info(msg=f"Found {len(items_to_update)} item(s) with '{target_property}'. Increasing price by 100.")

        self.collection.update_many(
            {target_property: {"$exists": True}},
            {"$inc": {"price": 100}}
        )
        logger.info(msg=f"\n{dumps(list(self.collection.find({target_property: {"$exists": True}})), indent=2)}")
