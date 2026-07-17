from datetime import datetime, UTC
from typing import Dict, List, Optional

class WorkflowJob:
    def __init__(
        self,
        job_id: str,
        command: str,
        metadata: Optional[dict] = None,
):
        self.job_id=job_id
        self.command= command
        self.metadata= metadata or {}
        
        self.status= "PENDING"
        
        self.created_at = datetime.now(UTC)
        self.started_at = None
        self.completed_at = None
        
    def to_dict(self) -> dict:
        """
        Serialize the WorkflowJob into a dictionary.
        """
        return {
            "job_id": self.job_id,
            "command": self.command,
            "metadata": self.metadata,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "started_at": (
                self.started_at.isoformat()
                if self.started_at
                else None
        ),
            "completed_at": (
                self.completed_at.isoformat()
                if self.completed_at
                else None
        ),
    }
        
    
    @classmethod
    def from_dict(cls, data: dict):
        """
        Create a WorkflowJob from a serialized dictionary.
        """
        job = cls(
            job_id=data["job_id"],
            command=data["command"],
            metadata=data.get("metadata"),
    )

        job.status = data.get("status", "PENDING")

        if data.get("created_at"):
            job.created_at = datetime.fromisoformat(data["created_at"])

        if data.get("started_at"):
            job.started_at = datetime.fromisoformat(data["started_at"])

        if data.get("completed_at"):
            job.completed_at = datetime.fromisoformat(data["completed_at"])

        return job
    
    def summary(self) -> dict:
        """
        Return a concise summary of the WorkflowJob.
        """
        return {
            "job_id": self.job_id,
            "status": self.status,
            "command": self.command,
            
    
            
    }  
    
class Workflow:
    """        
    Represents a workflow consisting of multiple jobs
    and their dependency relationships.
    """
   
    def __init__(
        self,
        workflow_id: str,
        name: str = "",
        metadata: Optional[Dict] = None,
):       
        
        self.workflow_id = workflow_id
        self.name = name
        self.metadata = metadata or {}

        self.jobs: Dict[str, WorkflowJob] = {}
        self.dependencies: Dict[str, List[str]] = {}
        self.state = "PENDING"

        self.created_at = datetime.now(UTC)
        self.completed_at = None
        
    
    def add_job(self, job: WorkflowJob):
        """
        Add a job to the workflow.
        """
        if job.job_id in self.jobs:
            raise ValueError(
                f"Job '{job.job_id}' already exists in workflow."
        )

        self.jobs[job.job_id] = job
        
    def remove_job(self, job_id: str):
        """
        Remove a job and all dependency references to it.
        """
        if job_id not in self.jobs:
           raise ValueError(
                f"Job '{job_id}' does not exist in workflow."
        )

    # Remove the job itself
        del self.jobs[job_id]

    # Remove it as a parent
        self.dependencies.pop(job_id, None)

    # Remove it as a child
        for parent in list(self.dependencies.keys()):
            if job_id in self.dependencies[parent]:
                self.dependencies[parent].remove(job_id)

        # Remove empty dependency lists
            if not self.dependencies[parent]:
                del self.dependencies[parent]
                
    def add_dependency(self, parent_job: str, child_job: str):
        """
        Add a dependency between two jobs.

        parent_job must complete before child_job can execute.
        """
        if parent_job not in self.jobs:
            raise ValueError(
                f"Parent job '{parent_job}' does not exist."
        )

        if child_job not in self.jobs:
            raise ValueError(
                f"Child job '{child_job}' does not exist."
        )

        if parent_job == child_job:
            raise ValueError(
                "A job cannot depend on itself."
        )

        self.dependencies.setdefault(parent_job, [])

        if child_job not in self.dependencies[parent_job]:
            self.dependencies[parent_job].append(child_job)
            
    
    def remove_dependency(self, parent_job: str, child_job: str):
        """
        Remove a dependency between two jobs.
        """
        if parent_job not in self.dependencies:
            raise ValueError(
                f"Parent job '{parent_job}' has no dependencies."
        )

        if child_job not in self.dependencies[parent_job]:
            raise ValueError(
                f"No dependency exists from '{parent_job}' to '{child_job}'."
        )

        self.dependencies[parent_job].remove(child_job)

    # Clean up empty dependency lists
        if not self.dependencies[parent_job]:
            del self.dependencies[parent_job]
    
    def get_job(self, job_id: str) -> WorkflowJob:
        """
        Return a job by its ID.
        """
        if job_id not in self.jobs:
            raise ValueError(
                f"Job '{job_id}' does not exist in workflow."
        )

        return self.jobs[job_id]
            

    def get_dependencies(self) -> Dict[str, List[str]]:
        """
        Return the workflow dependency graph.
        """
        return self.dependencies
    
    def get_root_jobs(self) -> List[WorkflowJob]:
        """
        Return all jobs that have no incoming dependencies.
        """
        child_jobs = set()

        for children in self.dependencies.values():
            child_jobs.update(children)

        return [
            job
            for job_id, job in self.jobs.items()
            if job_id not in child_jobs
    ]
        
    def get_leaf_jobs(self) -> List[WorkflowJob]:
        """
        Return all jobs that have no outgoing dependencies.
        """
        parent_jobs = set(self.dependencies.keys())

        return [
            job
            for job_id, job in self.jobs.items()
            if job_id not in parent_jobs
    ]
        
    def validate(self) -> bool:
        """
        Validate the workflow definition.

        Returns:
            True if the workflow is valid.

        Raises:
            ValueError: If the workflow definition is invalid.
        """

    # Workflow must contain at least one job
        if not self.jobs:
            raise ValueError("Workflow must contain at least one job.")

    # Validate dependency graph
        for parent_job, children in self.dependencies.items():

        # Parent must exist
            if parent_job not in self.jobs:
                raise ValueError(
                    f"Dependency references unknown parent job '{parent_job}'."
            )

            for child_job in children:

            # Child must exist
                if child_job not in self.jobs:
                    raise ValueError(
                        f"Dependency references unknown child job '{child_job}'."
                )

            # Prevent self-dependency
                if parent_job == child_job:
                    raise ValueError(
                        f"Job '{parent_job}' cannot depend on itself."
)

        return True
    
    