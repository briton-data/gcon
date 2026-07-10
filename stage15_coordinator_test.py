"""
GCON STAGE 15 - COORDINATOR INTEGRATION TEST
"""

from coordinator import GCONCoordinator
from workflow import Workflow, WorkflowJob
from workflow_engine import WorkflowEngine
from agent import GCONAgent


agent1 = GCONAgent("node-001")
agent2 = GCONAgent("node-002")
agent3 = GCONAgent("node-003")

print("=" * 60)
print(" GCON STAGE 15 - COORDINATOR WORKFLOW TEST")
print("=" * 60)

# --------------------------------------------------
# Coordinator
# --------------------------------------------------

coordinator = GCONCoordinator()

engine = WorkflowEngine(coordinator)

print("PASS: Coordinator initialized")

# --------------------------------------------------
# Register Nodes
# --------------------------------------------------
coordinator.register_agent(agent1)
coordinator.register_agent(agent2)
coordinator.register_agent(agent3)


print("PASS: Nodes registered")

# --------------------------------------------------
# Workflow
# --------------------------------------------------

workflow = Workflow("workflow-002")

workflow.add_job(
    WorkflowJob("A", "echo Root Job")
)

workflow.add_job(
    WorkflowJob("B", "echo Child Job")
)

workflow.add_dependency("A", "B")

print("PASS: Workflow created")

# --------------------------------------------------
# Submit Workflow
# --------------------------------------------------

state = engine.submit_workflow(workflow)

print("PASS: Workflow submitted")

# --------------------------------------------------
# Dispatch READY jobs
# --------------------------------------------------

engine.schedule_ready_jobs(
    workflow,
    state
)

print("PASS: Ready jobs scheduled")

# --------------------------------------------------
# Simulate completion
# --------------------------------------------------

engine.process_completed_job(
    workflow,
    engine.dags[workflow.workflow_id],
    state,
    "A"
)

print("PASS: Root completed")

engine.schedule_ready_jobs(
    workflow,
    state
)

engine.process_completed_job(
    workflow,
    engine.dags[workflow.workflow_id],
    state,
    "B"
)

assert state.workflow_completed()

print("PASS: Workflow completed")

print("\nWorkflow Summary")
print("----------------")
print(state.summary())

print("\n" + "=" * 60)
print(" STAGE 15 COORDINATOR TEST PASSED")
print("=" * 60)