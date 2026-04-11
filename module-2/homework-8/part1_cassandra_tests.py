import subprocess
import time

from cassandra import ConsistencyLevel, Unavailable, WriteTimeout
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import SimpleStatement


def get_real_name(base_name):
    """
    Asks Docker for the actual running container name.
    """

    res = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}", "--filter", f"name={base_name}"],
        capture_output=True, text=True
    )

    names = [n for n in res.stdout.strip().split('\n') if n]
    return names[0] if names else base_name


def get_network_name(node_name):
    """
    Inspects the container to find its exact attached network.
    """

    res = subprocess.run(
        ["docker", "inspect", "--format", "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}", node_name],
        capture_output=True,
        text=True
    )

    nets = res.stdout.strip().split('\n')
    return nets[0] if nets and nets[0] else "bridge"


def run_cmd(command_list):
    """
    Helper function to run docker commands and print the output.
    """

    print(f"\n[EXECUTING] {' '.join(command_list)}")
    result = subprocess.run(command_list, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(f"ERROR: {result.stderr.strip()}")


def execute_with_catch(session, query_str, cl, desc):
    """
    Helper to execute a query and catch expected consistency errors.
    """

    try:
        query = SimpleStatement(query_str, consistency_level=cl)
        session.execute(query)
        print(f"  [SUCCESS] {desc}")
    except (Unavailable, WriteTimeout, NoHostAvailable) as error:
        print(f"  [EXPECTED FAIL] {desc} -> {type(error).__name__}: {error}")
    except Exception as error:
        print(f"  [UNEXPECTED ERROR] {desc} -> {error}")


def main():
    print("Auto-detecting Docker environment...")
    NODE1 = get_real_name("cassandra1")
    NODE2 = get_real_name("cassandra2")
    NODE3 = get_real_name("cassandra3")
    NETWORK = get_network_name(NODE1)
    print(f"Found nodes: {NODE1}, {NODE2}, {NODE3} on network: {NETWORK}")

    print("\nConnecting to Cassandra cluster...")
    cluster = Cluster(["cassandra1"])
    session = cluster.connect()

    print("\n=== Checking Cluster Status ===")
    run_cmd(["docker", "exec", NODE1, "nodetool", "status"])

    print("\n=== 1. Creating keyspaces and tables  ===")
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS ks_rf1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS ks_rf2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};")
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS ks_rf3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};")

    for ks in ["ks_rf1", "ks_rf2", "ks_rf3"]:
        session.execute(f"CREATE TABLE IF NOT EXISTS {ks}.users (id int PRIMARY KEY, name text);")
    print("-> Keyspaces (RF1, RF2, RF3) and tables created.")

    print("\n=== 2. Connecting to different nodes ===")
    run_cmd(["docker", "exec", NODE2, "cqlsh", "-e", "INSERT INTO ks_rf3.users (id, name) VALUES (1, 'Alice');"])
    run_cmd(["docker", "exec", NODE3, "cqlsh", "-e", "SELECT * FROM ks_rf3.users WHERE id = 1;"])

    print("\n=== 3. Checking data distribution  ===")
    run_cmd(["docker", "exec", NODE1, "nodetool", "status", "ks_rf3"])
    run_cmd(["docker", "exec", NODE1, "nodetool", "getendpoints", "ks_rf3", "users", "1"])

    print("\n=== 4. Testing lightweight transactions (unpartitioned)  ===")
    execute_with_catch(
        session,
        query_str="INSERT INTO ks_rf3.users (id, name) VALUES (2, 'LWT_Healthy') IF NOT EXISTS;",
        cl=ConsistencyLevel.QUORUM,
        desc="LWT Write (Healthy Cluster)"
    )

    print("\n=== 5. Testing consistency levels with a down node  ===")
    run_cmd(["docker", "stop", NODE3])
    print("Sleeping for 15 seconds to let the cluster detect the downed node...")
    time.sleep(15)

    print("\nTesting ks_rf1 (RF=1)")
    execute_with_catch(
        session,
        query_str="INSERT INTO ks_rf1.users (id, name) VALUES (10, 'Test_RF1');",
        cl=ConsistencyLevel.ONE,
        desc="ks_rf1 CL=ONE"
    )

    print("\nTesting ks_rf2 (RF=2)")
    execute_with_catch(
        session,
        query_str="INSERT INTO ks_rf2.users (id, name) VALUES (10, 'Test_RF2');",
        cl=ConsistencyLevel.ONE,
        desc="ks_rf2 CL=ONE"
    )
    execute_with_catch(
        session,
        query_str="INSERT INTO ks_rf2.users (id, name) VALUES (11, 'Test_RF2');",
        cl=ConsistencyLevel.TWO,
        desc="ks_rf2 CL=TWO"
    )

    print("\nTesting ks_rf3 (RF=3)")
    execute_with_catch(
        session,
        query_str="INSERT INTO ks_rf3.users (id, name) VALUES (10, 'Test_RF3');",
        cl=ConsistencyLevel.ONE,
        desc="ks_rf3 CL=ONE (Requires 1 node)"
    )
    execute_with_catch(
        session,
        query_str="INSERT INTO ks_rf3.users (id, name) VALUES (11, 'Test_RF3');",
        cl=ConsistencyLevel.TWO,
        desc="ks_rf3 CL=TWO (Requires 2 nodes)"
    )
    execute_with_catch(
        session,
        query_str="INSERT INTO ks_rf3.users (id, name) VALUES (12, 'Test_RF3');",
        cl=ConsistencyLevel.THREE,
        desc="ks_rf3 CL=THREE (Requires 3 nodes - SHOULD FAIL)"
    )

    print(f"\nStarting {NODE3} back up...")
    run_cmd(["docker", "start", NODE3])
    print("Sleeping for 40 seconds to let gossip protocol repair the ring...")
    time.sleep(40)

    print("\n=== 6. Split brain & conflict resolution  ===")
    run_cmd(["docker", "network", "disconnect", NETWORK, NODE2])
    run_cmd(["docker", "network", "disconnect", NETWORK, NODE3])
    print("Sleeping for 15 seconds to ensure network partition is registered...")
    time.sleep(15)

    print("\nTesting LWT in partitioned cluster ")
    execute_with_catch(
        session,
        query_str="INSERT INTO ks_rf3.users (id, name) VALUES (99, 'LWT_Partitioned') IF NOT EXISTS;",
        cl=ConsistencyLevel.QUORUM,
        desc="LWT Write (Partitioned - cannot achieve paxos quorum)"
    )

    print("\nWriting conflicting data during partition...")
    execute_with_catch(
        session,
        query_str="INSERT INTO ks_rf3.users (id, name) VALUES (100, 'Conflict_Node1');",
        cl=ConsistencyLevel.ONE,
        desc="Write to Node 1 (CL=ONE)"
    )

    print("\nWriting to isolated Node 2 via cqlsh...")
    run_cmd(
        ["docker", "exec", NODE2, "cqlsh", "-e",
         "CONSISTENCY ONE; INSERT INTO ks_rf3.users (id, name) VALUES (100, 'Conflict_Node2');"]
    )

    print("\nRestoring the network...")
    run_cmd(["docker", "network", "connect", NETWORK, NODE2])
    run_cmd(["docker", "network", "connect", NETWORK, NODE3])
    print("Sleeping for 40 seconds to let the cluster heal and resolve conflicts...")
    time.sleep(40)

    print("\n=== 7. Checking conflict resolution ===")
    try:
        final_read = session.execute(
            SimpleStatement("SELECT * FROM ks_rf3.users WHERE id = 100;", consistency_level=ConsistencyLevel.ALL)
        ).one()
        if final_read:
            print(f"-> Final resolved value across cluster: '{final_read.name}'")
            print(
                "-> Conclusion: Cassandra resolves conflicts using Last-Write-Wins (LWW) based on internal cell timestamps."
            )
    except Exception as e:
        print(f"-> Could not read final value (cluster may still be healing): {e}")

    print("\n--- Cassandra part 1 & 2 tests complete! ---")


if __name__ == "__main__":
    main()
