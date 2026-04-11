import threading
import time
from typing import List

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement

NUM_CLIENTS: int = 10
INCREMENTS: int = 10000


def setup_database(session: Session) -> None:
    print("Setting up Keyspace (RF=3) and Counter table...")

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS ks_counters 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
    """)
    session.execute("USE ks_counters")

    session.execute("""
        CREATE TABLE IF NOT EXISTS likes_table(
            post_id text PRIMARY KEY,
            likes counter
        );
    """)


def run_test(session: Session, consistency_level: ConsistencyLevel, cl_name: str) -> None:
    print(f"\n=== Testing Write Consistency: {cl_name} ===")
    session.execute("USE ks_counters")
    session.execute("TRUNCATE likes_table;")

    query = SimpleStatement(
        "UPDATE likes_table SET likes = likes + 1 WHERE post_id = 'post1'",
        consistency_level=consistency_level
    )

    def worker() -> None:
        for _ in range(INCREMENTS):
            session.execute(query)

    threads: List[threading.Thread] = []
    start_time = time.time()

    for _ in range(NUM_CLIENTS):
        thread = threading.Thread(target=worker)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    end_time = time.time()

    read_query = SimpleStatement(
        "SELECT likes FROM likes_table WHERE post_id = 'post1'",
        consistency_level=ConsistencyLevel.QUORUM
    )
    result = session.execute(read_query).one()
    final_count = result.likes if result else 0
    expected = NUM_CLIENTS * INCREMENTS

    print(f"-> Time taken: {end_time - start_time:.2f} seconds")
    print(f"-> Final Likes: {final_count} / {expected}")

    if final_count == expected:
        print("-> RESULT: PERFECT CONSISTENCY! No updates were lost.")
    else:
        print("-> RESULT: DATA LOST OR INCONSISTENT!")


def main() -> None:
    print("Connecting to Cassandra cluster...")
    cluster = Cluster(["cassandra1"])

    try:
        session = cluster.connect()
        setup_database(session)

        run_test(session, ConsistencyLevel.ONE, cl_name="ONE")
        run_test(session, ConsistencyLevel.QUORUM, cl_name="QUORUM")

        print("\n--- Performance Tests Complete! ---")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
