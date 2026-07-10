"""
GCON STAGE 15 - WORKFLOW ENGINE TEST
"""

from workflow import Workflow, WorkflowJob
from dag import DAG
from workflow_state import WorkflowState
from workflow_engine import WorkflowEngine


class DummyCoordinator:
    """
    Minimal coordinator used for workflow testing.
    """

    def submit_job(self, command):
        print(f"Submitting job: {command}")
        return True


print("=" * 60)
print(" GCON STAGE 15 - WORKFLOW ENGINE TEST")
print("=" * 60)

# --------------------------------------------------
# Initialize
# --------------------------------------------------

coordinator = DummyCoordinator()
engine = WorkflowEngine(coordinator)

# --------------------------------------------------
# Create Workflow
# --------------------------------------------------

workflow = Workflow("workflow-001")

workflow.add_job(
    WorkflowJob("A", "echo Job A")
)

workflow.add_job(
    WorkflowJob("B", "echo Job B")
)

workflow.add_job(
    WorkflowJob("C", "echo Job C")
)

workflow.add_job(
    WorkflowJob("D", "echo Job D")
)

print("PASS: Workflow created")

# --------------------------------------------------
# Dependencies
# --------------------------------------------------

workflow.add_dependency("A", "B")
workflow.add_dependency("A", "C")
workflow.add_dependency("B", "D")
workflow.add_dependency("C", "D")

print("PASS: Dependencies added")

# --------------------------------------------------
# Validation
# --------------------------------------------------

workflow.validate()

print("PASS: Workflow validation")

# --------------------------------------------------
# DAG
# --------------------------------------------------

dag = DAG(workflow)

assert not dag.has_cycle()

print("PASS: No dependency cycle")

# --------------------------------------------------
# Roots
# --------------------------------------------------

roots = dag.roots()

assert len(roots) == 1
assert roots[0].job_id == "A"

print("PASS: Root detection")

# --------------------------------------------------
# Leaves
# --------------------------------------------------

leaves = dag.leaves()

assert len(leaves) == 1
assert leaves[0].job_id == "D"

print("PASS: Leaf detection")

# --------------------------------------------------
# Topological Sort
# --------------------------------------------------

order = [job.job_id for job in dag.topological_sort()]

assert order.index("A") < order.index("B")
assert order.index("A") < order.index("C")
assert order.index("B") < order.index("D")
assert order.index("C") < order.index("D")

print("PASS: Topological ordering")

# --------------------------------------------------
# Initialize Workflow
# --------------------------------------------------

state = engine.submit_workflow(workflow)

assert "A" in state.ready_jobs

print("PASS: Workflow initialized")

# --------------------------------------------------
# Complete A
# --------------------------------------------------

engine.process_completed_job(
    workflow,
    dag,
    state,
    "A"
)

assert "B" in state.ready_jobs
assert "C" in state.ready_jobs

print("PASS: Dependency unlocking")

# --------------------------------------------------
# Complete B
# --------------------------------------------------

engine.process_completed_job(
    workflow,
    dag,
    state,
    "B"
)

assert "D" not in state.ready_jobs

print("PASS: Dependency enforcement")

# --------------------------------------------------
# Complete C
# --------------------------------------------------

engine.process_completed_job(
    workflow,
    dag,
    state,
    "C"
)

assert "D" in state.ready_jobs

print("PASS: Multi-parent dependency")

# --------------------------------------------------
# Complete D
# --------------------------------------------------

engine.process_completed_job(
    workflow,
    dag,
    state,
    "D"
)

assert state.workflow_completed()

print("PASS: Workflow completion")

# --------------------------------------------------
# Summary
# --------------------------------------------------

print("\nWorkflow Summary")
print("----------------")
print(state.summary())

print("\n" + "=" * 60)
print(" STAGE 15 WORKFLOW TEST PASSED")
print("=" * 60)