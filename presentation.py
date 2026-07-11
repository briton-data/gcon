"""
GCON Presentation Layer

Acts as the boundary between the GCON Core Engine and any user
interface (web, terminal, CLI, or future clients).

The Presentation Layer never owns cluster state or business logic.
It simply provides a unified interface to the coordinator.
"""

from datetime import datetime, UTC

from autoscaler import AutoScaler


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
        self.autoscaler = AutoScaler(coordinator)
        self.started_at = datetime.now(UTC)
  
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
        return self.coordinator.get_workflows()
    
    def get_metrics(self):
        """
        Return cluster metrics.
        """
        return self.coordinator.get_metrics()
  
    def get_events(self, limit=20):
        """
        Return the most recent cluster events, formatted for display.
        """
        events = self.coordinator.get_events(limit=limit)

        formatted = []
        for event in events[-limit:]:
            formatted.append({
                "timestamp": event.timestamp.strftime("%H:%M:%S"),
                "message": self._format_event_message(event),
                "event_type": event.event_type,
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
    
    def get_cluster_health(self):
        """
        Return overall cluster health.
        """
        return self.coordinator.get_cluster_health()
    
    def get_health_details(self):
        """
        Return detailed health information.
        """
        return self.coordinator.get_health_details()
    
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

    # ------------------------------------------------------------------
    # Cluster Visualization
    # ------------------------------------------------------------------

    def get_topology(self):
        """
        Return a coordinator/node graph describing the current cluster
        shape, for the live topology view.
        """
        nodes = self.coordinator.get_nodes()

        return {
            "coordinator": {"id": "Coordinator-1"},
            "nodes": [
                {
                    "node_id": node["node_id"],
                    "status": node["status"],
                    "running_jobs": node["running_jobs"],
                }
                for node in nodes
            ],
        }

    # ------------------------------------------------------------------
    # Explorer views
    # ------------------------------------------------------------------

    def get_receipts(self):
        """
        Return all generated job receipts.
        """
        return self.coordinator.get_receipts()

    def get_artifacts(self):
        """
        Return all registered artifacts.
        """
        return self.coordinator.get_artifacts()

    # ------------------------------------------------------------------
    # Real-Time Monitoring
    # ------------------------------------------------------------------

    def get_system_metrics(self):
        """
        Return aggregate resource usage across all nodes, plus basic
        cluster throughput figures, for the monitoring view.
        """
        nodes = self.coordinator.get_nodes()

        cpu_values = [n["cpu"] for n in nodes if isinstance(n["cpu"], (int, float))]
        mem_values = [n["memory"] for n in nodes if isinstance(n["memory"], (int, float))]

        jobs = self.coordinator.get_jobs()
        completed = sum(1 for j in jobs if j["status"] == "completed")
        failed = sum(1 for j in jobs if j["status"] == "failed")
        running = sum(1 for j in jobs if j["status"] == "running")

        uptime_seconds = (datetime.now(UTC) - self.started_at).total_seconds()

        return {
            "avg_cpu": round(sum(cpu_values) / len(cpu_values), 1) if cpu_values else 0,
            "avg_memory": round(sum(mem_values) / len(mem_values), 1) if mem_values else 0,
            "running_jobs": running,
            "completed_jobs": completed,
            "failed_jobs": failed,
            "event_count": self.coordinator.event_bus.count(),
            "uptime_seconds": int(uptime_seconds),
        }

    # ------------------------------------------------------------------
    # Analytics & History
    # ------------------------------------------------------------------

    def get_analytics(self):
        """
        Return job outcome breakdown and a recent event timeline, for
        the analytics/history view.
        """
        jobs = self.coordinator.get_jobs()

        completed = sum(1 for j in jobs if j["status"] == "completed")
        failed = sum(1 for j in jobs if j["status"] == "failed")
        running = sum(1 for j in jobs if j["status"] == "running")
        pending = sum(1 for j in jobs if j["status"] == "pending")
        total = len(jobs) or 1

        return {
            "totals": {
                "completed": completed,
                "failed": failed,
                "running": running,
                "pending": pending,
            },
            "success_rate": round((completed / total) * 100, 1),
            "timeline": self.get_events(limit=50),
        }

    # ------------------------------------------------------------------
    # Administration
    # ------------------------------------------------------------------

    def get_admin_config(self):
        """
        Return cluster configuration and diagnostic information for
        the administration view.
        """
        return {
            "min_nodes": self.autoscaler.MIN_NODES,
            "total_nodes": self.coordinator.get_total_node_count(),
            "idle_nodes": self.coordinator.get_idle_node_count(),
            "pending_jobs": self.coordinator.get_pending_job_count(),
            "subscriber_count": self.coordinator.event_bus.subscriber_count(),
            "event_count": self.coordinator.event_bus.count(),
            "uptime_seconds": int((datetime.now(UTC) - self.started_at).total_seconds()),
        }

    def scale_up(self):
        """
        Manually add a worker node to the cluster.
        """
        self.autoscaler.scale_up()
        return self.get_admin_config()

    def scale_down(self):
        """
        Manually remove an idle worker node from the cluster.
        """
        self.autoscaler.scale_down()
        return self.get_admin_config()