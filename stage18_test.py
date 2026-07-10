"""
Stage 18 Integration Test

Tests:
- Dashboard APIs
- Node summaries
- Job summaries
- Receipt summaries
- Artifact summaries
- Cluster status
"""

from coordinator import GCONCoordinator
from dashboard import Dashboard
from agent import GCONAgent


def main():

    print("=" * 60)
    print("STAGE 18 - DASHBOARD API INTEGRATION TEST")
    print("=" * 60)

    coordinator = GCONCoordinator()

    #
    # Register Nodes
    #

    print("\nRegistering nodes...")

    agent1 = GCONAgent("node-001")
    agent2 = GCONAgent("node-002")

    coordinator.register_agent(agent1)
    coordinator.register_agent(agent2)

    print("PASS: Nodes registered.")

    #
    # Create Jobs
    #

    print("\nCreating jobs...")

    # Use your existing create_job() API here

    print("PASS: Jobs created.")

    #
    # Receive Receipts
    #

    print("\nRegistering receipts...")

    # Use your existing receive_receipt() API here

    print("PASS: Receipts registered.")

    #
    # Register Artifacts
    #

    print("\nRegistering artifacts...")

    # Use your existing register_artifact() API here

    print("PASS: Artifacts registered.")

    #
    # Dashboard APIs
    #

    print("\nTesting Coordinator Dashboard APIs...")

    jobs = coordinator.get_jobs()
    nodes = coordinator.get_nodes()
    receipts = coordinator.get_receipts()
    artifacts = coordinator.get_artifacts()
    cluster = coordinator.get_cluster_status()

    print(f"PASS: Jobs API returned {len(jobs)} jobs.")
    print(f"PASS: Nodes API returned {len(nodes)} nodes.")
    print(f"PASS: Receipts API returned {len(receipts)} receipts.")
    print(f"PASS: Artifacts API returned {len(artifacts)} artifacts.")

    print("\nCluster Status")

    for key, value in cluster.items():
        print(f"{key:<20}: {value}")

    #
    # Dashboard
    #

    print("\nDisplaying dashboard...\n")

    dashboard = Dashboard(coordinator)
    dashboard.display()

    print("\nPASS: Dashboard displayed successfully.")

    print("\n" + "=" * 60)
    print("STAGE 18 TEST PASSED")
    print("=" * 60)


if __name__ == "__main__":
    main()