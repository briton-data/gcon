from datetime import datetime, UTC
from typing import Dict, Set 

class WorkflowState:
    """
    Tracks the runtime execution state of a workflow.
    """
    
    def __init__(self, workflow_id: str, created_by=None, org_id=None, name: str = ""):
        
        self.workflow_id = workflow_id
        self.name = name
        # Owning organization (copied from the Workflow); lets the API list
        # only a customer's own workflows.
        self.org_id = org_id
        # job_id -> why the engine could not submit it (policy rejection, org
        # concurrency limit, ...). Such a job is marked FAILED, never lost.
        self.errors: Dict[str, str] = {}
        self.status = "PENDING"
        self.job_states: Dict[str, str] = {}
        # Ownership metadata copied from the source Workflow at
        # initialization time, so it survives even though WorkflowState
        # (not Workflow) is what get_workflows()/summary() expose.
        self.created_by = created_by

        self.pending_jobs: Set[str] = set()
        self.ready_jobs: Set[str] = set()
        self.running_jobs: Set[str] = set()

        self.completed_jobs: Set[str] = set()
        self.failed_jobs: Set[str] = set()
        self.cancelled_jobs: Set[str] = set()
        self.blocked_jobs: Set[str] = set()

        self.created_at = datetime.now(UTC)
        self.started_at = None
        self.completed_at = None
        
    def _move_job(self, job_id: str, new_state: str):
        """
        Move a job to a new execution state.
        """

    # Remove from all state sets
        self.pending_jobs.discard(job_id)
        self.ready_jobs.discard(job_id)
        self.running_jobs.discard(job_id)
        self.completed_jobs.discard(job_id)
        self.failed_jobs.discard(job_id)
        self.cancelled_jobs.discard(job_id)
        self.blocked_jobs.discard(job_id)

    # Add to the appropriate state set
        if new_state == "PENDING":
            self.pending_jobs.add(job_id)

        elif new_state == "READY":
           self.ready_jobs.add(job_id)

        elif new_state == "RUNNING":
            self.running_jobs.add(job_id)

        elif new_state == "COMPLETED":
            self.completed_jobs.add(job_id)

        elif new_state == "FAILED":
            self.failed_jobs.add(job_id)

        elif new_state == "CANCELLED":
            self.cancelled_jobs.add(job_id)

        elif new_state == "BLOCKED":
            self.blocked_jobs.add(job_id)

        else:
            raise ValueError(f"Unknown job state '{new_state}'.")

        self.job_states[job_id] = new_state
        
    def mark_pending(self, job_id: str):
        """
        Mark a job as pending.
        """
        self._move_job(job_id, "PENDING")
        
        
    def mark_ready(self, job_id: str):
        """
        Mark a job as ready for execution.
        """
        self._move_job(job_id, "READY")
        
    def mark_running(self, job_id: str):
        """
        Mark a job as currently executing.
        """
        self._move_job(job_id, "RUNNING")
        
    def mark_completed(self, job_id: str):
        """
        Mark a job as completed.
        """
        self._move_job(job_id, "COMPLETED")
        
    def mark_failed(self, job_id: str):
        """
        Mark a job as failed.
        """
        self._move_job(job_id, "FAILED")
        
    def mark_cancelled(self, job_id: str):
        """
        Mark a job as cancelled.
        """
        self._move_job(job_id, "CANCELLED")

    def mark_blocked(self, job_id: str):
        """
        Mark a job as blocked.
        """
        self._move_job(job_id, "BLOCKED")
        
    def workflow_completed(self) -> bool:
        """
        Return True if the workflow has completed successfully.
        """
        return (
            not self.pending_jobs
            and not self.ready_jobs
            and not self.running_jobs
            and not self.failed_jobs
            and not self.cancelled_jobs
            and not self.blocked_jobs
    )
        
    def workflow_failed(self) -> bool:
        """
        Return True if the workflow has failed.
        """
        return bool(self.failed_jobs)
    
    
    def summary(self) -> Dict:
        """
        Return a summary of the workflow state.
        """
        return {
            "workflow_id": self.workflow_id,
            "status": self.status,
            "name": self.name,
            "created_by": self.created_by,
            "org_id": self.org_id,
            "pending_jobs": len(self.pending_jobs),
            "ready_jobs": len(self.ready_jobs),
            "running_jobs": len(self.running_jobs),
            "completed_jobs": len(self.completed_jobs),
            "failed_jobs": len(self.failed_jobs),
            "cancelled_jobs": len(self.cancelled_jobs),
            "blocked_jobs": len(self.blocked_jobs),
            "errors": dict(self.errors),
            "created_at": self.created_at.isoformat(),
            "started_at": (
                self.started_at.isoformat()
                if self.started_at else None
        ),
            "completed_at": (
                self.completed_at.isoformat()
                if self.completed_at else None
        ),
    }


    # ---- durable storage -------------------------------------------------
    def to_dict(self) -> Dict:
        """Everything needed to rebuild this state after a restart."""
        return {
            "workflow_id": self.workflow_id,
            "name": self.name,
            "org_id": self.org_id,
            "created_by": self.created_by,
            "status": self.status,
            "job_states": dict(self.job_states),
            "errors": dict(self.errors),
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "WorkflowState":
        state = cls(
            data["workflow_id"], created_by=data.get("created_by"),
            org_id=data.get("org_id"), name=data.get("name", ""),
        )
        state.status = data.get("status", "PENDING")
        state.errors = dict(data.get("errors") or {})
        # Re-place every job through _move_job so the per-state sets and
        # job_states can never disagree.
        for job_id, job_state in (data.get("job_states") or {}).items():
            state._move_job(job_id, job_state)
        for attr in ("created_at", "started_at", "completed_at"):
            if data.get(attr):
                setattr(state, attr, datetime.fromisoformat(data[attr]))
        return state
