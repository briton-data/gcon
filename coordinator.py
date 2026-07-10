import time
from agent import GCONAgent
from receipt import ReceiptManager
from verifier import ExecutionVerifier
from Noderegistry import NodeRegistry
from scheduler import Scheduler
from communication import CommunicationManager
from metrics import MetricsCollector, MetricsSummary
from dashboard import Dashboard
import threading
from queue import Queue
from artifact_registry import ArtifactRegistry
from storage_manager import StorageManager
from event import Event 
from event_types import EventType
from event_bus import EventBus
from datetime import datetime, UTC

class GCONCoordinator:
    """
    Coordinates GCON agents, job execution, and receipt management.
    """
    def __init__(self, network=None):
        self.network = network
        self.registry = NodeRegistry()
        self.nodes = {}
        
        self.scheduler = Scheduler(self.registry)
        self.communication = CommunicationManager()
        self.agents = {}
        self.event_bus = EventBus()
        
        self.jobs = {}
        self.job_queue = Queue()
        self.artifacts = {}
        self.receipts = {}
        self.artifact_registry = ArtifactRegistry() 
        self.storage_manager = StorageManager()
        
        self.scheduler_thread = threading.Thread(
                target=self.scheduler_loop,                       
                daemon=True
        )       
        self.scheduler_thread.start()
        
        print("GCON Coordinator initialized.")
        
    def register_agent(self, node):
        """
        Register a GCON agent with the coordinator.
        """
        self.registry.register(node)
        self.communication.register_node(node)

        print(f"Node '{node.node_id}' registered successfully.")
        self.event_bus.publish(
            Event(
                timestamp=datetime.now(UTC),
                event_type=EventType.NODE_REGISTERED,
                source="Coordinator",
                payload={
                    "node_id": node.node_id,
                    "status": node.status
        },
    )
)
    
    def submit_job(self, job_id, command, artifacts=None):
        """
        Submit a new job to the coordinator.
        """
        if artifacts is None:
             artifacts = []
             
            
        
        if job_id in self.jobs:
            raise ValueError(f"Job '{job_id}' already exists.")
        artifact_ids = []

        for filepath in artifacts:
            artifact_id = self.artifact_registry.register_artifact(filepath)
            artifact_ids.append(artifact_id)
        
        
        self.jobs[job_id] = {
            "command": command,
            "node_id": None,
            "status": "pending",
            "artifacts": artifact_ids,
            "created_at": datetime.now(UTC).isoformat(),
            "completed_at": None,
    }
        self.queue_job(job_id)
        
        self.event_bus.publish(
            Event(
                timestamp=datetime.now(),
                event_type="JOB_SUBMITTED",
                source="Coordinator",
                payload={
                    "job_id": job_id,
                    "command": command,
                    "artifacts": artifact_ids,
        },
    )
)
        print(f"[QUEUE] Job {job_id} queued")
        print(f"[QUEUE] Pending jobs: {self.job_queue.qsize()}")
    
    def assign_job(self, job_id):
        """
        Assign a job to an available node and execute it.
        """

        if job_id not in self.jobs:
            raise ValueError(f"Job '{job_id}' does not exist.")

        job = self.jobs[job_id]

        node = self.scheduler.select_node()

        if node is None:
            raise RuntimeError("No available nodes to execute the job.")

    # Mark node and job as busy/running
        node.status = "busy"
        
        self.registry.heartbeat(
            node.node_id,
            "busy",
            node.heartbeat()["timestamp"]
)       
        job["status"] = "running"
        job["node_id"] = node.node_id
        
        thread = threading.Thread(
        target=self._run_job,
        args=(node, job_id),
        daemon=True
    )

      
        self.event_bus.publish(
            Event(
                timestamp=datetime.now(),
                event_type="JOB_STARTED",
                source="Scheduler",
                payload={
                    "job_id": job_id,
                    "node_id":node.node_id,
        },
    )
)
        thread.start()
        return 
    
    
    def receive_receipt(self, job_id, receipt):
        """
        Store a receipt for a completed job.
        """

        if job_id not in self.jobs:
            raise ValueError(f"Job '{job_id}' does not exist.")

        self.receipts[job_id] = receipt

        print(f"Receipt received for job '{job_id}'.")
        
    def get_job_status(self, job_id):
        """
        Get the current status of a job.
        """

        if job_id not in self.jobs:
            raise ValueError(f"Job '{job_id}' does not exist.")

        return self.jobs[job_id]
    
    def check_cluster_health(self):
        """
        Check node health and recover jobs from failed nodes.
        """
        offline_nodes = self.registry.check_node_health()

        for node_id in offline_nodes:
            print(f"Node '{node_id}' marked OFFLINE")
            self.recover_jobs(node_id)
    
    def recover_jobs(self, node_id):
        """
        Recover unfinished jobs assigned to a failed node.
        """

        print(f"Recovering jobs from '{node_id}'...")

        for job_id, job in self.jobs.items():

            if job["node_id"] == node_id and job["status"] == "running":

                print(f"Recovering job '{job_id}'")

            # Reset the job
                job["status"] = "pending"
                job["node_id"] = None

                
            # Reassign the job
                try:
                    self.assign_job(job_id)
                    print(f"Job '{job_id}' reassigned successfully.")
                except RuntimeError as e:
                     print(f"Recovery failed for '{job_id}': {e}")
    
    
    def receive_heartbeat(self, heartbeat):
        """
        Process a heartbeat received from a node.
        """
        node_id = heartbeat["node_id"]
        status = heartbeat["status"]

        self.registry.heartbeat(
            heartbeat["node_id"],
            heartbeat["status"],
            heartbeat["timestamp"]
        )

        print(f"Heartbeat received from {node_id} ({status})")
    
    def receive_resource_report(self, resources):
        """
        Process a resource report received from a node.
        """

        node_id = resources["node_id"]

        self.registry.update_node_resources(node_id, resources)

        print(
            f"Resources updated for {node_id} "
            f"(CPU: {resources['cpu']}%, "
            f"Memory: {resources['memory']}%, "
            f"Jobs: {resources['running_jobs']})"
    )
        
    def dashboard(self):
        dashboard = Dashboard(self)

        self.event_bus.subscribe(dashboard.handle_event)

        dashboard.refresh()
        dashboard.display()

        return dashboard
    
    def _run_job(self, node, job_id):
        """
        Execute a job in a background thread.
        """

        job = self.jobs[job_id]

        try:
            response = self.communication.send_job(
                node.node_id,
                job_id,
                job["command"]
            )

            result = response["result"]

        except Exception as e:
            # Anything going wrong here (network error, agent crash,
            # bad response shape, etc.) must NOT leave the job
            # "running" and the node "busy" forever.
            print(f"[ERROR] _run_job failed for '{job_id}' on "
                  f"'{node.node_id}': {e}")

            job["status"] = "failed"
            job["completed_at"] = datetime.now(UTC).isoformat()
            job["result"] = {"status": "error", "message": str(e)}

            node.status = "idle"
            self.registry.heartbeat(
                node.node_id,
                "idle",
                node.heartbeat()["timestamp"]
            )

            self.event_bus.publish(
                Event(
                    timestamp=datetime.now(UTC),
                    event_type="JOB_FAILED",
                    source="Coordinator",
                    payload={
                        "job_id": job_id,
                        "node_id": node.node_id,
                        "error": str(e),
                    },
                )
            )
            return

        node.status = "idle"

        self.registry.heartbeat(
            node.node_id,
            "idle",
        node.heartbeat()["timestamp"]
)

        heartbeat = node.heartbeat()
        self.receive_heartbeat(heartbeat)

        resources = node.report_resources()
        self.receive_resource_report(resources)

        if result["status"] == "success":
            job["status"] = "completed"
            job["completed_at"] = datetime.now(UTC).isoformat()

            self.event_bus.publish(
                Event(
                    timestamp=datetime.now(UTC),
                    event_type="JOB_COMPLETED",
                    source="Coordinator",
                    payload={
                        "job_id": job_id,
                        "node_id": node.node_id,
        },
    )
)
        else:
            job["status"] = "failed"
            job["completed_at"] = datetime.now(UTC).isoformat()
            self.event_bus.publish(
                Event(
                    timestamp=datetime.now(UTC),
                    event_type="JOB_FAILED",
                    source="Coordinator",
                    payload={
                        "job_id": job_id,
                        "node_id": node.node_id,
        },
    )
)           
        job["result"] = result
          
    
    def scheduler_loop(self):
        """
        Continuously assign waiting jobs to idle nodes.
        """

        while True:

            if self.job_queue.empty():
                time.sleep(0.1)
                continue
            
            if not self.scheduler.has_available_node():
                time.sleep(0.1)
                continue

            job_id = self.job_queue.get()

            print(f"[QUEUE] Dispatching {job_id}")
            print(f"[QUEUE] Remaining jobs: {self.job_queue.qsize()}")


            try:
                self.assign_job(job_id)
            except RuntimeError:
                self.job_queue.put(job_id)
                
            time.sleep(0.05)    
            
    def queue_job(self, job_id):
        """Add a job to the pending queue."""
        self.job_queue.put(job_id)   
        
    
    def deregister_agent(self, node_id):
        """
        Remove an agent from the running cluster.
        """

        node = self.registry.get_node(node_id)

        self.event_bus.publish(
            Event(
                timestamp=datetime.now(UTC),
                event_type=EventType.NODE_DEREGISTERED,
                source="Coordinator",
                payload={
                    "node_id":node.node_id,
                    "status": node.status
        },
    )
)
        self.registry.remove(node_id)

        print(f"Node '{node_id}' deregistered successfully.")
        
    def get_pending_job_count(self):
        """
        Return the number of jobs waiting in the queue.
        """
        return self.job_queue.qsize()


    def get_idle_node_count(self):
        """
        Return the number of idle nodes currently available.
        """
        return len(self.registry.available_nodes())


    def get_total_node_count(self):
        """
        Return the total number of registered nodes.
        """
        return len(self.registry.list_nodes())


    def get_registered_nodes(self):
        """
        Return a list of registered node IDs.
        """
        return self.registry.list_nodes()
    
    def get_idle_nodes(self):
        """
        Return all currently idle node objects.
        """
        idle_nodes = []

        for info in self.registry.nodes.values():
            if info["status"] == "idle":
                idle_nodes.append(info["node"])

        return idle_nodes
    
    def register_job_artifact(self, job_id, node_id, filepath):
        """
        Store and register an artifact produced by a completed job.

        Returns:
            artifact_id
        """

        stored_path = self.storage_manager.store_artifact(
            node_id,
            filepath
    )

        artifact_id = self.artifact_registry.register_artifact(
             stored_path
    )

        job = self.jobs.get(job_id)

        if job is not None:
            job.setdefault("artifacts", []).append(artifact_id)
            
        self.event_bus.publish(
            Event(
                timestamp=datetime.now(),
                event_type="ARTIFACT_REGISTERED",
                source="StorageManager",
                payload={
                    "artifact_id": artifact_id
        },
    )
)

        return artifact_id
    
    def get_cluster_state(self):
        """
        Return a snapshot of the current cluster state, in the flat
        shape expected by the dashboard.
        """

        return {
            "total_nodes": self.get_total_node_count(),
            "idle_nodes": self.get_idle_node_count(),
            "registered_nodes": self.get_registered_nodes(),
            "running_jobs": sum(
                1 for job in self.jobs.values()
                if job["status"] == "running"
            ),
            "completed_jobs": sum(
                1 for job in self.jobs.values()
                if job["status"] == "completed"
            ),
            "failed_jobs": sum(
                1 for job in self.jobs.values()
                if job["status"] == "failed"
            ),
        }

    def get_events(self,limit=20):
        """
        Return recent cluster events.
        """
        return self.event_bus.get_recent_events(limit)
    
    def get_all_events(self):
        """
        Return the full event history (used by analytics/diagnostics).
        """
        return self.event_bus.get_events()

    def get_nodes(self):
        """
        Return a list of dicts describing every registered node, for
        use by presentation/dashboard clients. Uses getattr/get with
        defaults so a missing field never crashes the endpoint.
        """
        nodes = []
        
        for node_id, info in self.registry.nodes.items():
            node = info.get("node")

            nodes.append({
                "node_id": node_id,
                "status": info.get("status", "unknown"),
                "cpu": getattr(node, "cpu", "N/A"),
                "memory": getattr(node, "memory", "N/A"),
                "running_jobs": getattr(node, "running_jobs", 0),
                "last_seen": info.get("last_seen", "N/A"),
            })

        return nodes

    def get_jobs(self):
        """
        Return a dashboard summary about all jobs.
        """
        jobs = []

        for job_id, job in self.jobs.items():
            jobs.append({
                "job_id": job_id,
                "status": job["status"],
                "node_id": job.get("node_id"),
                "created_at": job.get("created_at"),
                "completed_at": job.get("completed_at"),
                "receipt_id": job_id if job_id in self.receipts else None,
                "artifacts": len(job.get("artifacts", [])),
            })
        return jobs

    def get_storage(self):
        """
        Return storage information.
        """
        return {
            "artifacts": self.artifact_registry.artifacts
        }

    def get_metrics(self):
        """
        Return cluster metrics.
        """
        collector = MetricsCollector(self)

        return {
            "nodes": collector.collect_node_metrics(),
            "jobs": collector.collect_job_metrics(),
        }
        
    def get_receipts(self):
        """
        Return a dashboard-friendly summary of all receipts.
        """
        receipts = []

        for receipt_id, receipt in self.receipts.items():

            receipts.append({
                "receipt_id": receipt_id,
                "job_id": receipt.get("job_id"),
                "status": receipt.get("status", "verified"),
                "created_at": receipt.get("created_at", "N/A")
        })

        return receipts
    
    def get_artifacts(self):
        """
        Return a dashboard-friendly summary of all artifacts.
        """

        artifacts = []

        for artifact in self.artifact_registry.list_artifacts():
            artifacts.append({
                "artifact_id": artifact.artifact_id,
                "filename": artifact.filename,
                "sha256": artifact.sha256,
                "size": artifact.size,
                "uploaded_at": artifact.uploaded_at,
        })

        return artifacts
    def get_cluster_status(self):
        """
        Return an overall summary of the cluster.
        """
        jobs = self.get_jobs()
        nodes = self.get_nodes()
        receipts = self.get_receipts()
        artifacts = self.get_artifacts()

        return {
            "total_nodes": len(nodes),
            "online_nodes": sum(
                1 for node in nodes
                if node["status"] in ("idle", "running", "online")
        ),
            "total_jobs": len(jobs),
            "running_jobs": sum(
                1 for job in jobs
                if job["status"] == "running"
        ),
            "completed_jobs": sum(
                1 for job in jobs
                if job["status"] == "completed"
        ),
            "total_receipts": len(receipts),
            "total_artifacts": len(artifacts),
    }