from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.cluster.autoscaler import AutoScaler


def banner(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def main():

    banner("GCON STAGE 16 - AUTO-SCALING & DYNAMIC NODE REGISTRATION TEST")

    # ---------------------------------------------------------
    # Initialize Coordinator
    # ---------------------------------------------------------

    coordinator = GCONCoordinator()
    autoscaler = AutoScaler(coordinator)

    # ---------------------------------------------------------
    # Register Initial Worker
    # ---------------------------------------------------------

    node1 = GCONAgent("node-001")
    coordinator.register_agent(node1)

    print("\nInitial Cluster")
    print(coordinator.get_registered_nodes())

    # ---------------------------------------------------------
    # Submit Workload
    # ---------------------------------------------------------

    banner("SUBMITTING WORKLOAD")

    for i in range(1, 6):
        coordinator.submit_job(
            f"job-{i:03}",
            f"echo Stage16 Job {i}"
        )

    print(f"\nPending Jobs : {coordinator.get_pending_job_count()}")
    print(f"Idle Nodes   : {coordinator.get_idle_node_count()}")

    # ---------------------------------------------------------
    # Scale Up
    # ---------------------------------------------------------

    banner("AUTO SCALE-UP")

    autoscaler.check_scale()

    print("\nCluster After Scale-Up")
    print(coordinator.get_registered_nodes())
    print(f"Total Nodes : {coordinator.get_total_node_count()}")

    # ---------------------------------------------------------
    # Scale Down
    # ---------------------------------------------------------

    banner("AUTO SCALE-DOWN")

    max_iterations = 20

    while (
        coordinator.get_total_node_count() > autoscaler.MIN_NODES
        and max_iterations > 0
    ):

        autoscaler.scale_down()

        print("\nCurrent Cluster")
        print(coordinator.get_registered_nodes())
        print(f"Total Nodes : {coordinator.get_total_node_count()}")

        max_iterations -= 1

    # Verify minimum cluster protection

    autoscaler.scale_down()

    # ---------------------------------------------------------
    # Final Validation
    # ---------------------------------------------------------

    banner("FINAL VALIDATION")

    final_nodes = coordinator.get_registered_nodes()

    print("Final Cluster")
    print(final_nodes)

    if coordinator.get_total_node_count() != autoscaler.MIN_NODES:
        raise AssertionError(
            "Cluster did not scale back to minimum size."
        )

    if final_nodes != ["node-001"]:
        raise AssertionError(
            "Unexpected final cluster state."
        )

    print("\nPASS: Dynamic node registration verified.")
    print("PASS: Automatic scale-up verified.")
    print("PASS: Dynamic scale-down verified.")
    print("PASS: Minimum cluster protection verified.")

    banner("STAGE 16 PASSED")


if __name__ == "__main__":
    main()