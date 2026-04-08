import time

import pymongo
from pymongo import WriteConcern
from pymongo.errors import WTimeoutError
from pymongo.read_concern import ReadConcern

URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=myReplicaSet"


def setup_collection(client):
    db = client.task7_db
    collection = db.replication_tests
    collection.delete_many({})
    return collection


def run_scenario_1_infinite_timeout(collection):
    print("\n--- SCENARIO 1: Infinite Timeout ---")
    print("ACTION REQUIRED: Run 'docker-compose kill mongo3'")
    input("Press Enter once mongo3 is stopped...")

    print("\nExecuting write with w=3 and wtimeout=0. HANGING...")
    print("ACTION REQUIRED: Run 'docker-compose start mongo3'")

    collection.with_options(write_concern=WriteConcern(w=3, wtimeout=0)).insert_one({"test": "infinite"})
    print("-> Success: Write completed after node restoration.")


def run_scenario_2_finite_timeout(collection):
    print("\n--- SCENARIO 2: Finite Timeout ---")
    print("ACTION REQUIRED: Run 'docker-compose kill mongo3'")
    input("Press Enter once mongo3 is stopped...")

    print("\nExecuting write with w=3 and wtimeout=5000ms...")
    try:
        collection.with_options(write_concern=WriteConcern(w=3, wtimeout=5000)).insert_one({"test": "finite"})
    except WTimeoutError:
        print("-> Success: Caught expected WTimeoutError.")

    doc = collection.with_options(read_concern=ReadConcern("majority")).find_one({"test": "finite"})
    print(f"-> Verification (readConcern majority): {doc}")

    print("\nACTION REQUIRED: Run 'docker-compose start mongo3'")
    input("Press Enter once mongo3 is restarted...")


def run_scenario_3_elections_and_sync(collection):
    print("\n--- SCENARIO 3: Standard Elections & Sync ---")
    print("ACTION REQUIRED: Run 'docker-compose kill mongo1'")
    input("Press Enter once mongo1 is stopped...")

    collection.with_options(write_concern=WriteConcern(w="majority")).insert_one({"test": "election_sync"})
    print("-> Data written to new primary.")

    print("ACTION REQUIRED: Run 'docker-compose start mongo1'")
    input("Press Enter once mongo1 is restarted and synced...")

    doc = collection.with_options(read_concern=ReadConcern("local")).find_one({"test": "election_sync"})
    print(f"-> Verification on restored node: {doc}")


def run_scenario_4_inconsistent_state_rollback(collection):
    print("\n--- SCENARIO 4: Inconsistent State & Rollback ---")
    print("ACTION REQUIRED: Run 'docker-compose kill mongo2 mongo3'")
    input("Press Enter IMMEDIATELY (within 5s)...")

    collection.with_options(write_concern=WriteConcern(w=1)).insert_one({"test": "rollback_data"})

    local_doc = collection.with_options(read_concern=ReadConcern("local")).find_one({"test": "rollback_data"})
    majority_doc = collection.with_options(read_concern=ReadConcern("majority")).find_one({"test": "rollback_data"})

    print(f"-> Read 'local': {local_doc}")
    print(f"-> Read 'majority': {majority_doc}")

    print("\nACTION REQUIRED: Run 'docker-compose kill mongo1', then 'docker-compose start mongo2 mongo3'")
    input("Press Enter once new master is elected...")

    collection.with_options(write_concern=WriteConcern(w="majority")).insert_one({"test": "divergence"})

    print("ACTION REQUIRED: Run 'docker-compose start mongo1'")
    input("Press Enter once mongo1 is back up...")

    time.sleep(5)
    final_doc = collection.with_options(read_concern=ReadConcern("local")).find_one({"test": "rollback_data"})
    print(f"-> Post-rollback check (expected None): {final_doc}")


def run_scenario_5_delayed_replica(client, collection):
    print("\n--- SCENARIO 5: Eventual Consistency (Delayed Replica) ---")

    config = client.admin.command("replSetGetConfig")
    config["config"]["version"] += 1
    for member in config["config"]["members"]:
        if "mongo3" in member["host"]:
            member["priority"] = 0
            member["hidden"] = True
            member["secondaryDelaySecs"] = 5
    client.admin.command("replSetReconfig", config["config"])
    print("-> Cluster reconfigured: mongo3 is now a delayed replica (5s).")

    print("ACTION REQUIRED: Run 'docker-compose kill mongo2'")
    input("Press Enter once mongo2 is stopped...")

    start_time = time.time()
    collection.with_options(write_concern=WriteConcern(w=1)).insert_one({"test": "eventual_consistency"})

    print("-> Reading with linearizable concern (waiting for delayed replica)...")
    doc = collection.with_options(read_concern=ReadConcern("linearizable")).find_one({"test": "eventual_consistency"})
    end_time = time.time()

    print(f"-> Result: {doc}")
    print(f"-> Delay observed: {end_time - start_time:.2f} seconds.")


def main():
    client = pymongo.MongoClient(URI)
    collection = setup_collection(client)

    run_scenario_1_infinite_timeout(collection)
    run_scenario_2_finite_timeout(collection)
    run_scenario_3_elections_and_sync(collection)
    run_scenario_4_inconsistent_state_rollback(collection)
    run_scenario_5_delayed_replica(client, collection)

    print("\n--- Part I Tests Complete! ---")


if __name__ == "__main__":
    main()
