from gcon.cluster.coordinator import GCONCoordinator
from gcon.cluster.registry import NodeRegistry
from gcon.cluster.node import GCONNode

# Create registry
registry = NodeRegistry()

# Register three nodes
registry.register(GCONNode("node-001"))
registry.register(GCONNode("node-002"))
registry.register(GCONNode("node-003"))

# Create coordinator (cluster.dispatcher.JobDispatcher / cluster.network.GCONNetwork
# used to be built here, but GCONCoordinator never actually called them --
# its `network` param was stored and never used. Both were dead code and
# have been removed; this script's coordinator was always driven entirely
# by its own registry/communication stack.)
coordinator = GCONCoordinator()

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