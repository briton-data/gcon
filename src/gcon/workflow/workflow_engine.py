from datetime import datetime, UTC

from .workflow import Workflow
from .dag import DAG
from .workflow_state import WorkflowState

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

    # Create runtime state (carrying real ownership metadata from the
    # submitted Workflow through to the state summary exposed by
    # get_workflows())
        state = WorkflowState(workflow.workflow_id, created_by=workflow.created_by)
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

    # Update workflow status and actually dispatch the root jobs --
    # marking them "ready" above is bookkeeping only; without this
    # call nothing ever runs (submit_workflow() previously returned a
    # READY-looking state whose jobs were never submitted to the
    # coordinator at all).
        state.status = "RUNNING"
        state.started_at = datetime.now(UTC)
        self.schedule_ready_jobs(workflow, state)
        
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
                command=job.command,
                created_by=workflow.created_by,
                workflow_id=workflow.workflow_id,
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
        self.schedule_ready_jobs(workflow, state)

        if state.workflow_completed():
            state.status = "COMPLETED"
            state.completed_at = datetime.now(UTC)
        
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
        state.completed_at = datetime.now(UTC)

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

            # Only PENDING jobs can newly become ready. dag.ready_jobs()
            # only excludes already-completed jobs, so without this
            # check a job that's already RUNNING (dispatched by an
            # earlier call here, dependencies satisfied) would still
            # show up every time this runs -- and get incorrectly
            # moved back into ready_jobs and re-submitted as a
            # duplicate the next time a sibling job completes.
            if job.job_id in state.pending_jobs:
                state.mark_ready(job.job_id)
                
    def execute(
        self,
        workflow: Workflow
):
        """
        Submit a workflow and return its initial state.

        Dispatch of the workflow's jobs (both the initial root jobs
        and every subsequent layer as earlier jobs complete) now
        happens automatically -- driven by submit_workflow() and the
        coordinator's job-completion callback into
        process_completed_job()/process_failed_job(), not by polling
        here. This method is a thin, synchronous convenience alias for
        submit_workflow() and does not itself wait for completion;
        check workflow_completed() on the returned state, or poll
        get_workflows(), to observe progress.
        """
        return self.submit_workflow(workflow)
    
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