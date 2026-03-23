import logging
import uuid

from cassandra.cluster import Cluster

logger = logging.getLogger(__name__)


class ItemsInterface:
    def __init__(self, cluster: Cluster, session):
        self.cluster = cluster
        self.session = session
        self.keyspace = "store"

    def setup_schema(self):
        logger.info("----- SETUP: creating keyspace and items table -----")

        self.session.execute(
            f"""
                CREATE KEYSPACE IF NOT EXISTS {self.keyspace} 
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
            """
        )
        self.session.set_keyspace(self.keyspace)

        self.session.execute(
            """
                CREATE TABLE IF NOT EXISTS items
                (
                    category text,
                    price decimal,
                    item_id uuid,
                    name text,
                    producer text,
                    attributes map<text, text>,
                    
                    PRIMARY KEY ((category), price, item_id)
                ) WITH CLUSTERING ORDER BY (price ASC, item_id ASC);
            """
        )

        self.session.execute("CREATE INDEX IF NOT EXISTS ON items (name);")
        self.session.execute("CREATE INDEX IF NOT EXISTS ON items (producer);")
        self.session.execute("CREATE INDEX IF NOT EXISTS ON items (KEYS(attributes));")
        self.session.execute("CREATE INDEX IF NOT EXISTS ON items (ENTRIES(attributes));")

    def seed_data(self):
        logger.info("----- INSERT: seeding items -----")
        self.session.execute("TRUNCATE items;")

        items = [
            ("Phone", 600, "iPhone 6", "Apple", "{'color': 'Silver', 'memory': '64GB'}"),
            ("Phone", 900, "Galaxy S23", "Samsung", "{'waterproof': 'true', 'color': 'Black'}"),
            ("TV", 1200, "OLED55", "LG", "{'smart_tv': 'true', 'resolution': '4K'}"),
            ("TV", 1500, "Bravia", "Sony", "{'smart_tv': 'true', 'refresh_rate': '120Hz'}"),
            ("Phone", 1000, "iPhone 13", "Apple", "{'color': 'Blue', 'memory': '128GB'}")
        ]

        for cat, price, name, prod, attrs in items:
            self.session.execute(
                f"""
                    INSERT INTO items (category, price, item_id, name, producer, attributes)
                    VALUES ('{cat}', {price}, {uuid.uuid4()}, '{name}', '{prod}', {attrs});
                """
            )

    def describe_table(self):
        logger.info("----- DESCRIBE: items table -----")
        table_meta = self.cluster.metadata.keyspaces[self.keyspace].tables["items"]
        logger.info(f"\n{table_meta.export_as_string()}")

    def run_queries(self):
        logger.info("----- QUERY: all 'Phone' category items sorted by price -----")
        rows = self.session.execute("SELECT name, price, producer FROM items WHERE category = 'Phone';")
        for row in rows:
            logger.info(row)

        logger.info("----- QUERY: 'Phone' category by name ('iPhone 6') -----")
        rows = self.session.execute("SELECT name, price FROM items WHERE category = 'Phone' AND name = 'iPhone 6';")
        for row in rows:
            logger.info(row)

        logger.info("----- QUERY: 'Phone' category with price between 500 and 950 -----")
        rows = self.session.execute(
            "SELECT name, price FROM items WHERE category = 'Phone' AND price >= 500 AND price <= 950;")
        for row in rows:
            logger.info(row)

        logger.info("----- QUERY: 'Phone' category by exact price and producer -----")
        rows = self.session.execute(
            "SELECT name, price, producer FROM items WHERE category = 'Phone' AND price = 600 AND producer = 'Apple';")
        for row in rows:
            logger.info(row)

        logger.info("----- QUERY: 'TV' category possessing 'smart_tv' characteristic -----")
        rows = self.session.execute(
            "SELECT name, attributes FROM items WHERE category = 'TV' AND attributes CONTAINS KEY 'smart_tv';")
        for row in rows:
            logger.info(row)

        logger.info("----- QUERY: 'Phone' category where color is 'Black' -----")
        rows = self.session.execute(
            "SELECT name, attributes FROM items WHERE category = 'Phone' AND attributes['color'] = 'Black';")
        for row in rows:
            logger.info(row)

    def update_item(self):
        item = self.session.execute("SELECT * FROM items WHERE category = 'Phone' AND name = 'iPhone 6' LIMIT 1;").one()
        cat, price, i_id = item["category"], item["price"], item["item_id"]

        logger.info("----- UPDATE: change color to space gray -----")
        self.session.execute(
            f"UPDATE items SET attributes['color'] = 'Space Gray' WHERE category='{cat}' AND price={price} AND item_id={i_id};")

        logger.info("--- UPDATE: Add warranty property ---")
        self.session.execute(
            f"UPDATE items SET attributes = attributes + {{'warranty': '12 months'}} WHERE category='{cat}' AND price={price} AND item_id={i_id};")

        logger.info("----- UPDATE: remove memory property -----")
        self.session.execute(
            f"UPDATE items SET attributes = attributes - {{'memory'}} WHERE category='{cat}' AND price={price} AND item_id={i_id};")

        updated = self.session.execute(
            f"SELECT name, attributes FROM items WHERE category='{cat}' AND price={price} AND item_id={i_id};").one()
        logger.info(f"Updated item: {updated}")
