from datetime import datetime, UTC
from typing import Optional

from workflow import Workflow
from dag import DAG
from workflow_state import WorkflowState

class WorkflowEngine:
    """
    Executes workflow DAGs by coordinating
    dependency resolution and job execution.
    """
    
    def __init__(self, coordinator):
        """
        Initialize the workflow execution engine.
        """
        self.coordinator = coordinator
        self.workflows = {}
        self.dags = {}
        self.states = {}
        
        
    def submit_workflow(self, workflow: Workflow) -> WorkflowState:
        """
        Submit a workflow for execution.

        Validates the workflow, constructs its DAG,
        initializes runtime state, and prepares it
        for execution.
        """
    # Validate workflow definition
        workflow.validate()

    # Build dependency graph
        dag = DAG(workflow)

    # Ensure the workflow is acyclic
        if dag.has_cycle():
            raise ValueError(
                "Workflow contains a dependency cycle."
        )

    # Create runtime state
        state = WorkflowState(workflow.workflow_id)
        self.workflows[workflow.workflow_id] = workflow
        self.dags[workflow.workflow_id] = dag
        self.states[workflow.workflow_id] = state

    # Initialize execution state
        self.initialize_workflow(workflow, dag, state)

        return state
    
    
    def initialize_workflow(
        self,
        workflow: Workflow,
        dag: DAG,
        state: WorkflowState
):
        """
        Initialize the runtime state of a workflow.
        """

    # Mark every job as pending
        for job_id in workflow.jobs:
            state.mark_pending(job_id)

    # Root jobs are immediately ready
        for job in dag.roots():
            state.mark_ready(job.job_id)

    # Update workflow status
        state.status = "READY"
        
    def schedule_ready_jobs(
        self,
        workflow: Workflow,
        state: WorkflowState
):
        """
        Schedule all jobs that are ready for execution.
        """
        for job_id in list(state.ready_jobs):

            job = workflow.get_job(job_id)

        self.coordinator.submit_job(
    job_id=job.job_id,
    command=job.command
)

        state.mark_running(job.job_id)
            
    def process_completed_job(
        self,
        workflow: Workflow,
        dag: DAG,
        state: WorkflowState,
        job_id: str
):
        """
        Process a successfully completed workflow job.
        """
    # Update runtime state
        state.mark_completed(job_id)

    # Update newly ready jobs
        self.update_ready_jobs(dag, state)

    # Schedule newly ready jobs
        
        
    def process_failed_job(
        self,
        dag: DAG,
        state: WorkflowState,
        job_id: str
):
        """
        Process a failed workflow job.
        """
        state.mark_failed(job_id)

    # Block direct dependent jobs
        for child in dag.children(job_id):
            state.mark_blocked(child.job_id)

        state.status = "FAILED"
        
    def update_ready_jobs(
        self,
        dag: DAG,
        state: WorkflowState
):
        """
        Update the set of ready jobs.
        """
        ready_jobs = dag.ready_jobs(
            state.completed_jobs
    )

        for job in ready_jobs:

            if job.job_id not in state.ready_jobs:
                state.mark_ready(job.job_id)
                
    def execute(
        self,
        workflow: Workflow
):
        """
        Execute a workflow.
        """
        state = self.submit_workflow(workflow)

        while not state.workflow_completed():

            self.schedule_ready_jobs(
                workflow,
                state
        )
        # Wait for coordinator callbacks
            break
        return state
    
    def is_complete(
        self,
        state: WorkflowState
) ->    bool:
        """
        Return True if the workflow has completed.
        """
        return state.workflow_completed()
    
    def summary(
        self,
        state: WorkflowState
):
        """
        Return a workflow execution summary.
        """
        return state.summary()