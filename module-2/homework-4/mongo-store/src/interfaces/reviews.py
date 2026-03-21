import logging

from pymongo.database import Database
from pymongo.collection import Collection


logger = logging.getLogger(__name__)


class ReviewsInterface:
    """
    Handles operations for the capped collection used for reviews.
    """

    def __init__(self, db: Database):
        self.db = db
        self.collection_name = "reviews"

    def test_capped_collection(self) -> None:
        """
        Creates a capped collection (max 5 records).
        Inserts 7 records to verify that the oldest ones are overwritten.
        """
        logger.info(msg="---- Testing capped collection (max 5 items) ----")

        self.db.drop_collection(self.collection_name)
        self.db.create_collection(self.collection_name, capped=True, size=100000, max=5)

        collection: Collection = self.db[self.collection_name]

        logger.info(msg="Inserting 7 reviews sequentially...")
        for i in range(1, 8):
            collection.insert_one({"review_text": f"Review number {i}", "rating": 5})

        logger.info(msg="Verifying capped behavior. Only reviews 3 through 7 should remain:")
        for review in collection.find({}, {"_id": 0}):
            logger.info(msg=f" - {review['review_text']}")
