from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent

# Create coordinator (cluster.dispatcher.JobDispatcher / cluster.network.GCONNetwork
# used to be built here, but GCONCoordinator never actually called them --
# its `network` param was stored and never used. Both were dead code and
# have been removed; this script's coordinator was always driven entirely
# by its own registry/communication stack.)
coordinator = GCONCoordinator()

# Register three nodes directly with the coordinator -- it owns its own
# NodeRegistry internally (coordinator.registry); a separately-constructed
# NodeRegistry, as this script previously used, is never seen by
# coordinator.assign_job()/scheduler, so it always finds zero available
# nodes regardless of what was registered into the standalone registry.
#
# GCONAgent (not GCONNode -- a different, incompatible class with a
# similar-looking constructor) is the node type the coordinator actually
# expects here: assign_job() calls node.heartbeat(), which GCONAgent
# implements and GCONNode does not.
coordinator.register_agent(GCONAgent("node-001"))
coordinator.register_agent(GCONAgent("node-002"))
coordinator.register_agent(GCONAgent("node-003"))

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