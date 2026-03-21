from pymongo.database import Database
from pymongo.collection import Collection


class ReviewsRepository:
    """Handles operations for the Capped Collection used for reviews."""

    def __init__(self, db: Database):
        self.db = db
        self.collection_name = "reviews"

    def test_capped_collection(self) -> None:
        """
        Creates a capped collection (max 5 records)[cite: 57].
        Inserts 7 records to verify that the oldest ones are overwritten[cite: 58].
        """
        print("\n--- Testing Capped Collection (Max 5 items) ---")

        # Reset the collection completely to guarantee fresh state
        self.db.drop_collection(self.collection_name)

        # Create a capped collection restricted to 5 documents maximum
        self.db.create_collection(self.collection_name, capped=True, size=100000, max=5)
        collection: Collection = self.db[self.collection_name]

        print("Inserting 7 reviews sequentially...")
        for i in range(1, 8):
            collection.insert_one({"review_text": f"Review number {i}", "rating": 5})

        print("\nVerifying capped behavior. Only reviews 3 through 7 should remain:")
        for review in collection.find({}, {"_id": 0}):
            print(f" - {review['review_text']}")
