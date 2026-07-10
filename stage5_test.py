from coordinator import GCONCoordinator
from network import GCONNetwork
from dispatcher import JobDispatcher
from registry import NodeRegistry
from node import GCONNode

# Create registry
registry = NodeRegistry()

# Register three nodes
registry.register(GCONNode("node-001"))
registry.register(GCONNode("node-002"))
registry.register(GCONNode("node-003"))

# Build network stack
dispatcher = JobDispatcher(registry)
network = GCONNetwork(dispatcher)

# Create coordinator
coordinator = GCONCoordinator(network)

# Submit a job
coordinator.submit_job(
    "job-001",
    "echo Stage 5 Integration Successful"
)

# Assign it (agent_id is still required for compatibility)
result = coordinator.assign_job("job-001")


print(result)

print("\nJob Status:")
print(coordinator.get_job_status("job-001"))