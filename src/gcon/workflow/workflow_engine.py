import logging
from datetime import datetime, UTC

from .workflow import Workflow
from .dag import DAG
from .workflow_state import WorkflowState

logger = logging.getLogger("gcon.workflow")

# Workflow statuses that mean "a job did not succeed; the rest of the DAG
# downstream of it will not run".
_STOPPED = ("FAILED", "CANCELLED")


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

    # Refuse anything that would silently clobber or collide with existing
    # work, BEFORE registering state or submitting a single job -- a
    # half-submitted workflow is much worse than a clear rejection.
        self._check_no_collisions(workflow)

    # Create runtime state (carrying real ownership metadata from the
    # submitted Workflow through to the state summary exposed by
    # get_workflows())
        state = WorkflowState(
            workflow.workflow_id, created_by=workflow.created_by,
            org_id=workflow.org_id, name=workflow.name,
        )
        self.workflows[workflow.workflow_id] = workflow
        self.dags[workflow.workflow_id] = dag
        self.states[workflow.workflow_id] = state

    # Initialize execution state
        try:
            self.initialize_workflow(workflow, dag, state)
        except Exception:
            # Nothing ran (the only exception that escapes initialization is
            # one that must reach the caller, e.g. this coordinator is not
            # the HA leader): forget the registration so a retry is clean.
            self._forget(workflow.workflow_id)
            raise

        self._persist(state)
        return state

    def _forget(self, workflow_id):
        self.workflows.pop(workflow_id, None)
        self.dags.pop(workflow_id, None)
        self.states.pop(workflow_id, None)

    def _check_no_collisions(self, workflow: Workflow):
        if workflow.workflow_id in self.workflows:
            raise ValueError(
                f"Workflow '{workflow.workflow_id}' already exists; "
                "submit it under a new workflow_id."
            )
        taken = set(self.coordinator.jobs)
        for other in self.workflows.values():
            taken.update(other.jobs)
        clash = sorted(job_id for job_id in workflow.jobs if job_id in taken)
        if clash:
            raise ValueError(
                "Job id(s) already in use by another job or workflow: "
                + ", ".join(clash)
            )
    
    
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
        from gcon.cluster.coordinator import NotLeaderError

        for job_id in list(state.ready_jobs):

            job = workflow.get_job(job_id)

            try:
                self.coordinator.submit_job(
                    job_id=job.job_id,
                    command=job.command,
                    created_by=workflow.created_by,
                    workflow_id=workflow.workflow_id,
                    # The workflow's customer: its jobs count against that
                    # org's limits and are visible only to that org.
                    org_id=workflow.org_id,
                    client_reference=job.metadata.get("client_reference"),
                    # Set by the public API on workflows it creates.
                    sandbox_required=bool(workflow.metadata.get("sandbox_required")),
                )
            except NotLeaderError:
                raise
            except Exception as e:
                # The coordinator refused this job (policy rejection, org
                # concurrency limit, ...). Record why and fail the job and
                # everything downstream, instead of leaving it READY forever
                # or crashing the caller halfway through a DAG.
                logger.warning("workflow %s: job %s was not accepted: %s",
                               workflow.workflow_id, job_id, e)
                state.errors[job_id] = str(e)
                dag = self.dags.get(workflow.workflow_id)
                if dag is not None:
                    self.process_failed_job(dag, state, job_id)
                else:
                    state.mark_failed(job_id)
                continue

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
        else:
            self._settle(state)
        self._persist(state)
        
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
        self._recompute_blocked(dag, state)
        state.status = "FAILED"
        self._settle(state)
        self._persist(state)

    def process_cancelled_job(
        self,
        dag: DAG,
        state: WorkflowState,
        job_id: str
):
        """
        A workflow job was cancelled. A cancelled job never succeeds, so
        everything downstream of it is blocked; independent branches keep
        running. (A FAILED workflow stays FAILED -- failure outranks
        cancellation.)
        """
        state.mark_cancelled(job_id)
        self._recompute_blocked(dag, state)
        if state.status != "FAILED":
            state.status = "CANCELLED"
        self._settle(state)
        self._persist(state)

    def reopen_job(
        self,
        dag: DAG,
        state: WorkflowState,
        job_id: str
):
        """
        A failed workflow job was retried: put it back in flight and release
        the jobs that were blocked only because of it. When it completes the
        normal completion path dispatches whatever became ready.
        """
        state.errors.pop(job_id, None)
        state.mark_running(job_id)
        self._recompute_blocked(dag, state)
        if state.failed_jobs:
            state.status = "FAILED"
        elif state.cancelled_jobs:
            state.status = "CANCELLED"
        else:
            state.status = "RUNNING"
            state.completed_at = None
        self._persist(state)

    def _recompute_blocked(self, dag: DAG, state: WorkflowState):
        """
        BLOCKED = every job downstream (at any depth) of a failed or cancelled
        job that has not itself started. Recomputed from scratch each time so
        it is also correct after a retry un-fails a job.
        """
        should_block = set()
        for broken in (state.failed_jobs | state.cancelled_jobs):
            should_block |= dag.descendants(broken)
        for job_id in should_block:
            if state.job_states.get(job_id) in ("PENDING", "READY", "BLOCKED"):
                state.mark_blocked(job_id)
        for job_id in list(state.blocked_jobs):
            if job_id not in should_block:
                state.mark_pending(job_id)

    def _settle(self, state: WorkflowState):
        """completed_at is when the workflow stopped doing anything, not the
        moment the first job failed while other branches were still running."""
        if state.status in _STOPPED and not state.running_jobs and not state.ready_jobs:
            if state.completed_at is None:
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


    # ------------------------------------------------------------------
    # Durable storage + restart recovery
    # ------------------------------------------------------------------

    def _persist(self, state: WorkflowState):
        """Write this workflow's definition + state to the control-plane DB.
        Best effort, like job-status persistence: a storage hiccup must never
        break scheduling, so it is logged and the in-memory state carries on."""
        cp = getattr(self.coordinator, "control_plane", None)
        workflow = self.workflows.get(state.workflow_id)
        if cp is None or workflow is None:
            return
        try:
            cp.workflows.save(workflow.to_dict(), state.to_dict())
        except Exception as e:
            logger.warning("could not persist workflow %s: %r", state.workflow_id, e)

    def _job_status(self, job_id):
        """Current coordinator status of a job, from memory or, if the job has
        since been evicted from memory, from the durable jobs table."""
        job = self.coordinator.jobs.get(job_id)
        if job is not None:
            return (job.get("status") or "").lower()
        cp = getattr(self.coordinator, "control_plane", None)
        if cp is not None:
            try:
                row = cp.jobs.get(job_id)
                if row:
                    return (row.get("status") or "").lower()
            except Exception:
                pass
        return None

    def restore(self) -> int:
        """
        Rebuild every persisted workflow after a coordinator restart, then
        reconcile each with the jobs table: a job that finished while the
        engine's state was being written (or whose completion callback was
        lost in a crash) advances the DAG now instead of stalling it forever.
        Returns how many workflows were restored. Never raises.
        """
        cp = getattr(self.coordinator, "control_plane", None)
        if cp is None:
            return 0
        try:
            rows = cp.workflows.list_all()
        except Exception as e:
            logger.warning("could not load workflows: %r", e)
            return 0

        restored = 0
        for row in rows:
            try:
                workflow = Workflow.from_dict(row["workflow"])
                state = WorkflowState.from_dict(row["state"])
                dag = DAG(workflow)
            except Exception as e:
                logger.warning("skipping unreadable workflow %s: %r", row.get("workflow_id"), e)
                continue
            self.workflows[workflow.workflow_id] = workflow
            self.dags[workflow.workflow_id] = dag
            self.states[workflow.workflow_id] = state
            restored += 1
            try:
                self._reconcile(workflow, dag, state)
            except Exception as e:
                logger.warning("could not reconcile workflow %s: %r", workflow.workflow_id, e)
        return restored

    def _reconcile(self, workflow: Workflow, dag: DAG, state: WorkflowState):
        if state.status in ("COMPLETED",):
            return
        for job_id in sorted(state.running_jobs | state.ready_jobs):
            status = self._job_status(job_id)
            if status == "completed":
                self.process_completed_job(workflow, dag, state, job_id)
            elif status == "failed":
                self.process_failed_job(dag, state, job_id)
            elif status == "cancelled":
                self.process_cancelled_job(dag, state, job_id)
            elif status is None and job_id in state.ready_jobs:
                # Marked ready but the crash came before the job was
                # submitted: submit it now.
                self.schedule_ready_jobs(workflow, state)
            # pending/running: the coordinator's own restart recovery is
            # already re-queuing it; its completion will advance the DAG.
