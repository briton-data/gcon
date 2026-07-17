from datetime import datetime, UTC
from typing import Dict, Set 

class WorkflowState:
    """
    Tracks the runtime execution state of a workflow.
    """
    
    def __init__(self, workflow_id: str):
        
        self.workflow_id = workflow_id
        self.status = "PENDING"
        self.job_states: Dict[str, str] = {}

        self.pending_jobs: Set[str] = set()
        self.ready_jobs: Set[str] = set()
        self.running_jobs: Set[str] = set()

        self.completed_jobs: Set[str] = set()
        self.failed_jobs: Set[str] = set()
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
            "pending_jobs": len(self.pending_jobs),
            "ready_jobs": len(self.ready_jobs),
            "running_jobs": len(self.running_jobs),
            "completed_jobs": len(self.completed_jobs),
            "failed_jobs": len(self.failed_jobs),
            "blocked_jobs": len(self.blocked_jobs),
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