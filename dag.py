from collections import deque
from typing import Dict, List, Set
from workflow import Workflow, WorkflowJob
from workflow import Workflow
from typing import Set, List

class DAG:
    """
    Represents the dependency graph of a workflow.

    Provides graph algorithms such as parent/child lookup,
    cycle detection, topological sorting, and dependency
    resolution.
    """
    
    def __init__(self, workflow: Workflow):
        """
        Initialize a DAG from a Workflow.
        """
        self.workflow = workflow
        self.jobs = workflow.jobs
        self.dependencies = workflow.get_dependencies()
        
    def children(self, job_id: str) -> List:
        """
        Return all child jobs of the given job.        
        """
        if job_id not in self.jobs:
           raise ValueError(
                f"Job '{job_id}' does not exist."
        )

        child_ids = self.dependencies.get(job_id, [])

        return [
            self.jobs[child_id]
            for child_id in child_ids
            
    ]
        
    def parents(self, job_id: str) -> List[WorkflowJob]:
        """
        Return all parent jobs of the given job.
        """
        if job_id not in self.jobs:
            raise ValueError(
                f"Job '{job_id}' does not exist."
        )

        parents = []

        for parent_id, children in self.dependencies.items():
            if job_id in children:
                parents.append(self.jobs[parent_id])

        return parents
    
    def roots(self) -> List[WorkflowJob]:
        """
        Return all root jobs in the DAG.
        """
        roots = []

        for job_id, job in self.jobs.items():
            if not self.parents(job_id):
                roots.append(job)

        return roots
    
    def leaves(self) -> List[WorkflowJob]:
        """
        Return all leaf jobs in the DAG.
        """
        leaves = []

        for job_id, job in self.jobs.items():
            if not self.children(job_id):
                leaves.append(job)

        return leaves
    
    def has_cycle(self) -> bool:
        """
        Check whether the workflow graph contains a cycle.

        Implemented iteratively (explicit stack, white/gray/black
        coloring) rather than via recursive DFS: a recursive
        implementation blows Python's call stack (RecursionError) on
        large workflows -- a real workflow can easily exceed the
        default ~1000 frame recursion limit, and cycle detection must
        not crash on exactly the kind of input (large graphs) it
        exists to validate.
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {job_id: WHITE for job_id in self.jobs}

        for start in self.jobs:
            if color[start] != WHITE:
                continue

            # Each stack entry is [node_id, iterator over its children]
            stack = [(start, iter(self.dependencies.get(start, [])))]
            color[start] = GRAY

            while stack:
                node_id, children = stack[-1]
                advanced = False

                for child in children:
                    if color.get(child, WHITE) == GRAY:
                        # Back edge to a node still on the current
                        # path -- that's a cycle.
                        return True
                    if color.get(child, WHITE) == WHITE:
                        color[child] = GRAY
                        stack.append((child, iter(self.dependencies.get(child, []))))
                        advanced = True
                        break

                if not advanced:
                    color[node_id] = BLACK
                    stack.pop()

        return False
    
    def topological_sort(self) -> List[WorkflowJob]:
        """
        Return the jobs in topological (dependency-respecting) order.
        """
    # Calculate the in-degree (number of parents) for each job
        in_degree = {job_id: 0 for job_id in self.jobs}

        for children in self.dependencies.values():
            for child in children:
                in_degree[child] += 1

    # Start with all root jobs (in-degree == 0)
        queue = deque(
            job_id for job_id, degree in in_degree.items()
            if degree == 0
    )

        ordered_jobs = []

        while queue:
            job_id = queue.popleft()
            ordered_jobs.append(self.jobs[job_id])

            for child in self.dependencies.get(job_id, []):
                in_degree[child] -= 1

                if in_degree[child] == 0:
                    queue.append(child)

    # If we couldn't process every job, the graph contains a cycle
        if len(ordered_jobs) != len(self.jobs):
            raise ValueError(
                "Workflow contains a dependency cycle."
        )

        return ordered_jobs
    
    def ready_jobs(self, completed_jobs: Set[str]) -> List[WorkflowJob]:
        """
        Return all jobs whose dependencies have been satisfied.
        """
        ready = []

        for job_id, job in self.jobs.items():

        # Skip jobs already completed
            if job_id in completed_jobs:
                continue

            parents = self.parents(job_id)

            if all(parent.job_id in completed_jobs for parent in parents):
                ready.append(job)

        return ready