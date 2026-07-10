from coordinator import GCONCoordinator
from agent import GCONAgent
from autoscaler import AutoScaler

print("=" * 60)
print("GCON STAGE 16 - AUTO SCALING TEST")
print("=" * 60)

# Create coordinator
coordinator = GCONCoordinator()

# Register one worker
node1 = GCONAgent("node-001")
coordinator.register_agent(node1)

print("\nInitial cluster:")
print(coordinator.get_registered_nodes())

# Create workload
for i in range(1, 6):
    coordinator.submit_job(f"job-{i:03}", f"echo Job {i}")
    
print(f"\nPending jobs: {coordinator.get_pending_job_count()}")
print(f"Idle nodes : {coordinator.get_idle_node_count()}")

# Run autoscaler
autoscaler = AutoScaler(coordinator)

print("\nRunning AutoScaler...")
autoscaler.check_scale()

print("\nCluster after scaling:")
print(coordinator.get_registered_nodes())

print(f"\nTotal nodes: {coordinator.get_total_node_count()}")

print("\nSTAGE 16 SCALE-UP TEST COMPLETE")

print("\n" + "=" * 60)
print("TESTING SCALE DOWN")
print("=" * 60)

while coordinator.get_total_node_count() > autoscaler.MIN_NODES:
    autoscaler.scale_down()

    print("Current cluster:")
    print(coordinator.get_registered_nodes())
    print(f"Total nodes: {coordinator.get_total_node_count()}")
    print("-" * 40)

# One extra call to verify minimum protection
autoscaler.scale_down()

print("\nScale-down test complete.")
print("Final cluster:")
print(coordinator.get_registered_nodes())