import logging

from neo4j import Driver

logger = logging.getLogger(__name__)


class StoreInterface:
    def __init__(self, driver: Driver):
        self.driver = driver

    def setup_and_seed(self):
        """
        Clears the DB and create models.
        """
        clear_query = "MATCH (n) DETACH DELETE n"

        seed_query = """
        // create Items
        CREATE (i1:Item {name: 'MacBook Pro', price: 2000, item_id: 'I1'})
        CREATE (i2:Item {name: 'AirPods', price: 200, item_id: 'I2'})
        CREATE (i3:Item {name: 'Magic Mouse', price: 100, item_id: 'I3'})
        CREATE (i4:Item {name: 'iPad', price: 800, item_id: 'I4'})

        // create customers 
        CREATE (c1:Customer {name: 'Andrii', customer_id: 'C1'})
        CREATE (c2:Customer {name: 'Ivan', customer_id: 'C2'})

        // create orders
        CREATE (o1:Order {order_id: 'ORD-001'})
        CREATE (o2:Order {order_id: 'ORD-002'})
        CREATE (o3:Order {order_id: 'ORD-003'})

        // customer places order
        CREATE (c1)-[:PLACES]->(o1)
        CREATE (c1)-[:PLACES]->(o2)
        CREATE (c2)-[:PLACES]->(o3)

        // order contains items
        CREATE (o1)-[:CONTAINS {quantity: 1}]->(i1)
        CREATE (o1)-[:CONTAINS {quantity: 1}]->(i3)
        CREATE (o2)-[:CONTAINS {quantity: 2}]->(i2)
        CREATE (o3)-[:CONTAINS {quantity: 1}]->(i1)
        CREATE (o3)-[:CONTAINS {quantity: 1}]->(i2)

        // customer views items
        CREATE (c1)-[:VIEWS]->(i1)
        CREATE (c1)-[:VIEWS]->(i3)
        CREATE (c1)-[:VIEWS]->(i4)
        CREATE (c2)-[:VIEWS]->(i1)
        """

        with self.driver.session() as session:
            session.run(clear_query)
            session.run(seed_query)
            logger.info("Database successfully seeded with nodes and relationships.")

    def run_queries(self):
        with self.driver.session() as session:
            logger.info("--- QUERY 1: items in order ORD-001 ---")
            result = session.run(
                "MATCH (o:Order {order_id: 'ORD-001'})-[:CONTAINS]->(i:Item)"
                "RETURN i.name AS name, i.price AS price"
            )
            for item in result:
                logger.info(dict(item))

            logger.info("--- QUERY 2: total cost of order ORD-001 ---")
            result = session.run(
                "MATCH (o:Order {order_id: 'ORD-001'})-[rel:CONTAINS]->(i:Item)"
                "RETURN sum(i.price * rel.quantity) AS total_cost"
            )
            for item in result:
                logger.info(dict(item))

            logger.info("--- QUERY 3: all orders placed by Andrii ---")
            result = session.run(
                "MATCH (c:Customer {name: 'Andrii'})-[:PLACES]->(o:Order)"
                "RETURN o.order_id AS order"
            )
            for item in result:
                logger.info(dict(item))

            logger.info("--- QUERY 4: all items bought by Andrii ---")
            result = session.run(
                "MATCH (c:Customer {name: 'Andrii'})-[:PLACES]->(:Order)-[:CONTAINS]->(i:Item)"
                "RETURN DISTINCT i.name AS item"
            )
            for item in result:
                logger.info(dict(item))

            logger.info("--- QUERY 5: total quantity of items bought by Andrii ---")
            result = session.run(
                "MATCH (c:Customer {name: 'Andrii'})-[:PLACES]->(:Order)-[rel:CONTAINS]->(:Item)"
                "RETURN sum(rel.quantity) AS total_items"
            )
            for item in result:
                logger.info(dict(item))

            logger.info("--- QUERY 6: total amount spent by Andrii ---")
            result = session.run(
                "MATCH (c:Customer {name: 'Andrii'})-[:PLACES]->(:Order)-[rel:CONTAINS]->(i:Item)"
                "RETURN sum(i.price * rel.quantity) AS total_spent"
            )
            for item in result:
                logger.info(dict(item))

            logger.info("--- QUERY 7: times each item was bought (sorted) ---")
            result = session.run(
                "MATCH (:Order)-[rel:CONTAINS]->(i:Item) "
                "RETURN i.name AS item, sum(rel.quantity) AS times_bought "
                "ORDER BY times_bought DESC"
            )
            for item in result:
                logger.info(dict(item))

            logger.info("--- QUERY 8: items viewed by Andrii ---")
            result = session.run(
                "MATCH (c:Customer {name: 'Andrii'})-[:VIEWS]->(i:Item)"
                "RETURN i.name AS item"
            )
            for item in result:
                logger.info(dict(item))

            logger.info("--- QUERY 9: items bought together with MacBook Pro ---")
            result = session.run(
                "MATCH (i1:Item {name: 'MacBook Pro'})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(i2:Item)"
                "WHERE i1 <> i2 RETURN DISTINCT i2.name AS item"
            )
            for item in result:
                logger.info(dict(item))

            logger.info("--- QUERY 10: customers who bought AirPods ---")
            result = session.run(
                "MATCH (c:Customer)-[:PLACES]->(:Order)-[:CONTAINS]->(i:Item {name: 'AirPods'})"
                "RETURN DISTINCT c.name AS customer"
            )
            for item in result:
                logger.info(dict(item))

            logger.info("--- QUERY 11: items Andrii viewed but did NOT buy ---")
            result = session.run(
                "MATCH (c:Customer {name: 'Andrii'})-[:VIEWS]->(i:Item)"
                "WHERE NOT (c)-[:PLACES]->(:Order)-[:CONTAINS]->(i)"
                "RETURN i.name AS missed_opportunity"
            )
            for item in result:
                logger.info(dict(item))
