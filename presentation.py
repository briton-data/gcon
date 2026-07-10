"""
GCON Presentation Layer

Acts as the boundary between the GCON Core Engine and any user
interface (web, terminal, CLI, or future clients).

The Presentation Layer never owns cluster state or business logic.
It simply provides a unified interface to the coordinator.
"""


class PresentationLayer:
    """
    Unified presentation interface for GCON.

    This class provides a single entry point for all presentation
    clients. It delegates all operations to the GCON Core Engine
    while remaining independent of any specific UI technology.
    """

    def __init__(self, coordinator):
        """
        Initialize the Presentation Layer.

        Args:
            coordinator: The active GCONCoordinator instance.
        """
        self.coordinator = coordinator
  
    def get_nodes(self):
        """
        Return information about all registered nodes.
        """

        return self.coordinator.get_nodes()
    
    def get_jobs(self):
        """
        Return information about all jobs.
        """
        return self.coordinator.get_jobs()
    
    def get_storage(self):
        """
        Return storage information.
        """
        return {
            "artifacts": getattr(
                self.coordinator.artifact_registry,
                "artifacts",
            {}
        )
    }
        
    def get_workflows(self):
        """
        Return workflow information.
        """
        return []
    
    def get_metrics(self):
        """
        Return cluster metrics.
        """
        return self.coordinator.get_metrics()
  
    def get_events(self, limit=20):
        """
        Return the most recent cluster events, formatted for display.
        """
        events = self.coordinator.get_events()

        formatted = []
        for event in events[-limit:]:
            formatted.append({
                "timestamp": event.timestamp.strftime("%H:%M:%S"),
                "message": self._format_event_message(event),
            })

        # Most recent first
        formatted.reverse()
        return formatted

    @staticmethod
    def _format_event_message(event):
        """
        Build a short human-readable description of an event.
        """
        payload = event.payload or {}
        node_id = payload.get("node_id")
        job_id = payload.get("job_id")

        messages = {
            "NODE_REGISTERED": f"Node {node_id} registered",
            "NODE_DEREGISTERED": f"Node {node_id} deregistered",
            "NODE_ONLINE": f"Node {node_id} came online",
            "NODE_OFFLINE": f"Node {node_id} went offline",
            "NODE_IDLE": f"Node {node_id} is idle",
            "NODE_BUSY": f"Node {node_id} is busy",
            "JOB_SUBMITTED": f"Job {job_id} submitted",
            "JOB_ASSIGNED": f"Job {job_id} assigned to {node_id}",
            "JOB_STARTED": f"Job {job_id} started on {node_id}",
            "JOB_COMPLETED": f"Job {job_id} completed",
            "JOB_FAILED": f"Job {job_id} failed",
            "ARTIFACT_REGISTERED": f"Artifact registered for job {job_id}",
            "RECEIPT_GENERATED": f"Receipt generated for job {job_id}",
            "CLUSTER_STARTED": "Cluster started",
            "CLUSTER_STOPPED": "Cluster stopped",
        }

        return messages.get(event.event_type, f"{event.event_type} ({event.source})")

    def submit_job(self, job_id, command, artifacts=None):
        """
        Submit a new job to the cluster.
        """
        return self.coordinator.submit_job(
            job_id,
            command,
            artifacts
    ) 
    def register_node(self, node):
        """
        Register a new node with the cluster.
        """
        return self.coordinator.register_agent(node)


    def deregister_node(self, node_id):
        """
        Remove a node from the cluster.
        """
        return self.coordinator.deregister_agent(node_id)
    
    def get_cluster_state(self):
        return self.coordinator.get_cluster_state()
    
    def get_dashboard_metrics(self):
        """
        Return summary metrics for the dashboard.
        """
        jobs = self.coordinator.get_jobs()
        nodes = self.coordinator.get_nodes()

        running_jobs = 0
        completed_jobs = 0
        failed_jobs = 0

        for job in jobs:

            status = job.get("status", "").lower()

            if status == "running":
                running_jobs += 1

            elif status == "completed":
                completed_jobs += 1

            elif status == "failed":
                failed_jobs += 1

        return {
            "total_nodes": len(nodes),
            "total_jobs": len(jobs),
            "running_jobs": running_jobs,
            "completed_jobs": completed_jobs,
            "failed_jobs": failed_jobs,
        }

    def get_dashboard(self):
        """
        Return all data required by the dashboard.
        """
        return {
            "metrics": self.get_dashboard_metrics(),
            "nodes": self.get_nodes(),
            "jobs": self.get_jobs(),
            "events": self.get_events(),
            "storage": self.get_storage(),
            "workflows": self.get_workflows(),
            "receipts_count": len(self.coordinator.get_receipts()),
    }