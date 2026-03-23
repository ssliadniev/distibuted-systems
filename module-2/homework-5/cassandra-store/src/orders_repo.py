import logging
import time
import uuid
from datetime import datetime, timedelta

from cassandra.cluster import Cluster

logger = logging.getLogger(__name__)


class OrdersInterface:
    def __init__(self, cluster: Cluster, session):
        self.cluster = cluster
        self.session = session
        self.keyspace = "store"

    def setup_schema(self):
        logger.info("----- SETUP: creating orders table -----")

        self.session.execute(
            """
                CREATE TABLE IF NOT EXISTS orders
                (
                    customer_name text,
                    order_date timestamp,
                    order_id uuid,
                    cost decimal,
                    item_names set<text>,
                    
                    PRIMARY KEY ((customer_name), order_date, order_id)
                ) WITH CLUSTERING ORDER BY (order_date DESC, order_id ASC);
            """
        )

        self.session.execute("CREATE INDEX IF NOT EXISTS ON orders (item_names);")

    def seed_data(self):
        logger.info("----- INSERT: seeding orders -----")
        self.session.execute("TRUNCATE orders;")

        d1 = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        d2 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

        orders = [
            ("Ivan", d1, 1500, "{'Galaxy S23', 'iPhone 6'}"),
            ("Ivan", d2, 1200, "{'OLED55'}"),
            ("Maria", d1, 600, "{'iPhone 6'}")
        ]

        for cust, date, cost, items in orders:
            self.session.execute(
                f"""
                    INSERT INTO orders (customer_name, order_date, order_id, cost, item_names)
                    VALUES ('{cust}', '{date}', {uuid.uuid4()}, {cost}, {items});
                """
            )

    def describe_table(self):
        logger.info("----- DESCRIBE: orders table -----")
        table_meta = self.cluster.metadata.keyspaces[self.keyspace].tables["orders"]
        logger.info(f"\n{table_meta.export_as_string()}")

    def run_queries(self):
        logger.info("----- QUERY: Ivan's orders sorted by time -----")
        rows = self.session.execute("SELECT customer_name, order_date, cost FROM orders WHERE customer_name = 'Ivan';")
        for row in rows:
            logger.info(row)

        logger.info("----- QUERY: Ivan's orders containing 'iPhone 6' -----")
        rows = self.session.execute(
            "SELECT order_date, item_names FROM orders WHERE customer_name = 'Ivan' AND item_names CONTAINS 'iPhone 6';"
        )
        for row in rows:
            logger.info(row)

        logger.info("----- QUERY: Ivan's order count in the last 7 days -----")
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        count = self.session.execute(
            f"SELECT COUNT(*) as total FROM orders WHERE customer_name = 'Ivan' AND order_date >= '{seven_days_ago}';"
        ).one()
        logger.info(f"Total orders: {count['total']}")

        logger.info("----- QUERY: total sum of Ivan's orders -----")
        total_sum = self.session.execute(
            "SELECT SUM(cost) as total_sum FROM orders WHERE customer_name = 'Ivan';"
        ).one()
        logger.info(f"Sum: {total_sum['total_sum']}")

        logger.info("----- QUERY: max cost of Ivan's orders -----")
        max_cost = self.session.execute("SELECT MAX(cost) as max_cost FROM orders WHERE customer_name = 'Ivan';").one()
        logger.info(f"Max Cost: {max_cost['max_cost']}")

    def update_and_meta_queries(self):
        order = self.session.execute("SELECT * FROM orders WHERE customer_name = 'Ivan' LIMIT 1;").one()
        cust, date, o_id = order["customer_name"], order["order_date"].strftime('%Y-%m-%d %H:%M:%S.%f%z'), order["order_id"]

        logger.info("----- UPDATE: ddd item to set, remove item from set, change cost -----")
        self.session.execute(f"""
            UPDATE orders 
            SET item_names = item_names + {{'AirPods'}}, 
                item_names = item_names - {{'OLED55'}},
                cost = 1450
            WHERE customer_name='{cust}' AND order_date='{date}' AND order_id={o_id};
        """)

        logger.info("----- QUERY: WRITETIME for cost column -----")
        wt = self.session.execute(
            f"SELECT WRITETIME(cost) as wt FROM orders WHERE customer_name='{cust}' AND order_date='{date}' AND order_id={o_id};"
        ).one()
        logger.info(f"WriteTime Timestamp: {wt['wt']}")

        logger.info("----- INSERT: temporary order with 5 seconds TTL -----")
        self.session.execute(f"""
            INSERT INTO orders (customer_name, order_date, order_id, cost, item_names)
            VALUES ('TemporaryUser', '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', {uuid.uuid4()}, 99, {{'TestItem'}})
            USING TTL 5;
        """)

        logger.info("Waiting 6 seconds to verify TTL deletion...")
        time.sleep(6)
        ttl_check = self.session.execute("SELECT * FROM orders WHERE customer_name = 'TemporaryUser';").all()
        logger.info(f"TemporaryUser orders found: {len(ttl_check)} (Expected: 0)")
