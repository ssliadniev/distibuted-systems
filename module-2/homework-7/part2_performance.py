import concurrent.futures
import time

import pymongo
from pymongo import WriteConcern

INCREMENTS_PER_CLIENT = 10000
NUM_CLIENTS = 10


def client_worker(uri, wc_level):
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=20000)
    collection = client.task7_db.get_collection("likes_collection", write_concern=WriteConcern(w=wc_level))

    increments_done = 0
    while increments_done < INCREMENTS_PER_CLIENT:
        try:
            collection.find_one_and_update(
                {"_id": "post_1"},
                {"$inc": {"likes": 1}},
                upsert=True
            )
            increments_done += 1
        except Exception:
            time.sleep(0.5)


def execute_performance_test(test_name, wc_level, pause_for_failover=False, retry_writes="true"):
    print(f"\n=== {test_name} (Write Concern: {wc_level}) ===")

    uri = f"mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=myReplicaSet&retryWrites={retry_writes}"

    client = pymongo.MongoClient(uri)
    collection = client.task7_db.likes_collection
    collection.delete_many({})
    collection.insert_one({"_id": "post_1", "likes": 0})

    if pause_for_failover:
        print("ACTION REQUIRED: Go to terminal and prepare to kill the PRIMARY node.")
        input("Press Enter to start the test, then immediately kill the node...")
    else:
        print("Starting test...")

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_CLIENTS) as executor:
        futures = [executor.submit(client_worker, uri, wc_level) for _ in range(NUM_CLIENTS)]
        concurrent.futures.wait(futures)

    end_time = time.time()

    final_doc = collection.find_one({"_id": "post_1"})
    final_likes = final_doc.get("likes", 0) if final_doc else 0
    expected = NUM_CLIENTS * INCREMENTS_PER_CLIENT

    print(f"-> Execution Time: {end_time - start_time:.2f} seconds")
    print(f"-> Final Likes Counter: {final_likes} / {expected}")

    if final_likes != expected:
        print("-> STATUS: DATA LOSS DETECTED")
    else:
        print("-> STATUS: PERFECT CONSISTENCY")


def main():
    print("Initializing Part II Performance & Integrity Tests\n")

    execute_performance_test("Test 1: Standard Execution", 1)
    execute_performance_test("Test 2: Standard Execution", "majority")

    execute_performance_test("Test 3: Sudden Failover Emulation", 1, pause_for_failover=True, retry_writes="false")

    print("\nACTION REQUIRED: Restore the killed node from Test 3 before proceeding.")
    input("Press Enter when cluster is healthy...")

    execute_performance_test("Test 4: Sudden Failover Emulation", "majority", pause_for_failover=True)


if __name__ == "__main__":
    main()
