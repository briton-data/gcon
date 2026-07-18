import time
import threading
from queue import Queue
from datetime import datetime, UTC

from .Noderegistry import NodeRegistry
from .scheduler import Scheduler
from .communication import CommunicationManager

from gcon.execution.verifier import ExecutionVerifier
from gcon.execution.artifact_registry import ArtifactRegistry
from gcon.storage.storage_manager import StorageManager
from gcon.events.event import Event
from gcon.events.event_types import EventType
from gcon.events.event_bus import EventBus
from gcon.workflow.workflow_engine import WorkflowEngine
from gcon.monitoring.health_service import HealthService
from gcon.monitoring.metrics import MetricsCollector
from gcon.dashboard.dashboard import Dashboard

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
        self.jobs_lock = threading.RLock()
        self.job_queue = Queue()
        
        self.receipts = {}
        self.artifact_registry = ArtifactRegistry() 
        self.storage_manager = StorageManager()
        self.workflow_engine = WorkflowEngine(self)
        self.health_service = HealthService(self)
        self.verifier = ExecutionVerifier()
        self.scheduler_paused = False
        
        self.scheduler_thread = threading.Thread(
                target=self.scheduler_loop,                       
                daemon=True
        )       
        self.scheduler_thread.start()

        self.health_check_thread = threading.Thread(
                target=self.health_check_loop,
                daemon=True
        )
        self.health_check_thread.start()
        
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
             
            
        
        with self.jobs_lock:
            if job_id in self.jobs:
                raise ValueError(f"Job '{job_id}' already exists.")
        artifact_ids = []

        for filepath in artifacts:
            artifact_id = self.artifact_registry.register_artifact(filepath)
            artifact_ids.append(artifact_id)
        
        
        with self.jobs_lock:    
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
                timestamp=datetime.now(UTC),
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
        
        if job["status"] != "pending":
    # Already assigned/running/completed -- most likely a race
    # with the background scheduler_loop thread, which also
    # consumes the job queue. Assigning twice would run the
    # same job on two nodes.
            print(
                f"[QUEUE] Job {job_id} is already '{job['status']}', "
                "skipping re-assignment."
    )
            return

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
                timestamp=datetime.now(UTC),
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
            self.event_bus.publish(Event(
                timestamp=datetime.now(UTC),
                event_type="NODE_OFFLINE",
                source="Coordinator",
                payload={"node_id": node_id},
            ))
            self.recover_jobs(node_id)
    
    def recover_jobs(self, node_id):
        """
        Recover unfinished jobs assigned to a failed node.
        """

        print(f"Recovering jobs from '{node_id}'...")
        with self.jobs_lock:

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

            cancelled = job.get("cancel_requested", False)
            job["status"] = "cancelled" if cancelled else "failed"
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
                    event_type="JOB_CANCELLED" if cancelled else "JOB_FAILED",
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

            # Generate a real, cryptographically signed receipt for
            # this execution using the coordinator's shared verifier
            # instance (so later verification uses the same key).
            try:
                input_hash = self.verifier.hash_data(job["command"])
                output_hash = self.verifier.hash_data(result.get("stdout", ""))
                receipt = self.verifier.create_receipt(
                    job_id, node.node_id, result, input_hash, output_hash
                )
                self.receive_receipt(job_id, receipt)

                self.event_bus.publish(Event(
                    timestamp=datetime.now(UTC),
                    event_type="RECEIPT_GENERATED",
                    source="Coordinator",
                    payload={"job_id": job_id, "node_id": node.node_id},
                ))
            except Exception as e:
                print(f"[WARN] Receipt generation failed for '{job_id}': {e}")

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
            cancelled = job.get("cancel_requested", False)
            job["status"] = "cancelled" if cancelled else "failed"
            job["completed_at"] = datetime.now(UTC).isoformat()
            self.event_bus.publish(
                Event(
                    timestamp=datetime.now(UTC),
                    event_type="JOB_CANCELLED" if cancelled else "JOB_FAILED",
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

            if self.scheduler_paused:
                time.sleep(0.2)
                continue

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
            
    def health_check_loop(self):
        """
        Periodically check for nodes that have gone silent (missed
        their heartbeat window) and recover any jobs they were
        running. Runs continuously in the background so heartbeat
        loss is detected in real time, not just when a button is
        clicked.
        """
        while True:
            time.sleep(3)
            try:
                self.check_cluster_health()
            except Exception as e:
                print(f"[HEALTH] Health check loop error: {e}")

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

    # ------------------------------------------------------------
    # Scheduler control
    # ------------------------------------------------------------

    def pause_scheduler(self):
        """
        Stop assigning new jobs to nodes. Already-running jobs are
        unaffected and continue to completion.
        """
        self.scheduler_paused = True
        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="SCHEDULER_PAUSED",
            source="Coordinator", payload={},
        ))
        print("[SCHEDULER] Paused.")

    def resume_scheduler(self):
        """
        Resume assigning queued jobs to idle nodes.
        """
        self.scheduler_paused = False
        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="SCHEDULER_RESUMED",
            source="Coordinator", payload={},
        ))
        print("[SCHEDULER] Resumed.")

    # ------------------------------------------------------------
    # Node lifecycle control
    # ------------------------------------------------------------

    def drain_node(self, node_id):
        """
        Stop assigning new jobs to a node. Any job it's currently
        running is left to finish naturally.
        """
        self.registry.get_node(node_id)  # raises if missing
        self.registry.set_draining(node_id, True)
        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="NODE_DRAINING",
            source="Coordinator", payload={"node_id": node_id},
        ))
        print(f"[NODE] '{node_id}' is draining — no new jobs will be assigned.")

    def restart_worker(self, node_id):
        """
        Restart a worker in place: cancel any job it's currently
        running, then reset it to idle. The node keeps its identity
        and stays registered (unlike stop_worker, which removes it).
        """
        node = self.registry.get_node(node_id)
        info = self.registry.get_node_info(node_id)

        was_running = info["status"] == "busy"
        if was_running:
            self._cancel_node_job(node_id)

        self.registry.set_draining(node_id, False)
        node.status = "idle"
        self.registry.heartbeat(node_id, "idle", node.heartbeat()["timestamp"])

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="NODE_RESTARTED",
            source="Coordinator", payload={"node_id": node_id, "had_running_job": was_running},
        ))
        print(f"[NODE] '{node_id}' restarted.")

    def stop_worker(self, node_id):
        """
        Forcibly stop and remove a worker: cancel any job it's
        currently running, then deregister it from the cluster.
        """
        info = self.registry.get_node_info(node_id)
        if info["status"] == "busy":
            self._cancel_node_job(node_id)

        self.deregister_agent(node_id)
        print(f"[NODE] '{node_id}' stopped and removed.")

    def _cancel_node_job(self, node_id):
        """
        Find whatever job is currently running on a node and cancel
        it (kills the underlying subprocess).
        """
        with self.jobs_lock:
        
            for job_id, job in self.jobs.items():
                if job["node_id"] == node_id and job["status"] == "running":
                    job["cancel_requested"] = True
                    node = self.registry.get_node(node_id)
                    node.cancel()
                    return job_id
        return None

    # ------------------------------------------------------------
    # Job control
    # ------------------------------------------------------------

    def cancel_job(self, job_id):
        """
        Cancel a specific running job by killing its process.
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job '{job_id}' does not exist.")

        job = self.jobs[job_id]
        if job["status"] != "running":
            raise ValueError(f"Job '{job_id}' is not running (status: {job['status']}).")

        job["cancel_requested"] = True
        node = self.registry.get_node(job["node_id"])
        killed = node.cancel()

        print(f"[JOB] Cancel requested for '{job_id}' (process killed: {killed}).")
        return killed

    def clear_queue(self):
        """
        Remove every job still waiting in the queue and mark them
        cancelled. Jobs already running are unaffected.
        """
        cleared = []
        while not self.job_queue.empty():
            job_id = self.job_queue.get()
            job = self.jobs.get(job_id)
            if job and job["status"] == "pending":
                job["status"] = "cancelled"
                job["completed_at"] = datetime.now(UTC).isoformat()
                cleared.append(job_id)

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="QUEUE_CLEARED",
            source="Coordinator", payload={"cleared_job_ids": cleared},
        ))
        print(f"[QUEUE] Cleared {len(cleared)} pending job(s).")
        return cleared

    def retry_failed_jobs(self):
        """
        Re-queue every currently failed job for another attempt.
        """
        retried = []

        with self.jobs_lock:
            for job_id, job in self.jobs.items():
                if job["status"] == "failed":
                    job["status"] = "pending"
                    job["node_id"] = None
                    job["completed_at"] = None
                    job.pop("cancel_requested", None)
                    self.queue_job(job_id)
                    retried.append(job_id)

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="FAILED_JOBS_RETRIED",
            source="Coordinator", payload={"job_ids": retried},
        ))
        print(f"[QUEUE] Retrying {len(retried)} failed job(s).")
        return retried

    def clear_completed_jobs(self):
        """
        Remove completed jobs from the working set to declutter the
        dashboard. Running/pending/failed jobs are left alone.
        """
        with self.jobs_lock:
            cleared = [jid for jid, j in self.jobs.items() if j["status"] == "completed"]
            for job_id in cleared:
                del self.jobs[job_id]

        print(f"[JOBS] Cleared {len(cleared)} completed job(s).")
        return cleared
    
     
    def rediscover_nodes(self):
        """
        Re-check every node's heartbeat freshness right now (rather
        than waiting for the next periodic health check), marking
        any that have gone silent as offline and recovering their
        in-flight jobs.
        """
        offline_nodes = self.registry.check_node_health()
        for node_id in offline_nodes:
            print(f"Node '{node_id}' marked OFFLINE")
            self.recover_jobs(node_id)

        print(f"[DISCOVERY] Rediscovery complete. {len(offline_nodes)} node(s) newly offline.")
        return {
            "checked": len(self.registry.nodes),
            "newly_offline": offline_nodes,
        }

    # ------------------------------------------------------------
    # Receipts, snapshots, emergency control
    # ------------------------------------------------------------

    def verify_all_receipts(self):
        """
        Cryptographically verify every stored receipt's signed proof
        against the coordinator's verifier, using the real HMAC
        signature check (not a stub).
        """
        results = []
        for receipt_id, receipt in self.receipts.items():
            proof = receipt.get("proof", {})
            is_valid, message = self.verifier.validate_proof(proof)
            results.append({
                "receipt_id": receipt_id,
                "job_id": receipt.get("job_id"),
                "valid": is_valid,
                "message": message,
            })

        print(f"[VERIFY] Checked {len(results)} receipt(s).")
        return results

    def get_cluster_snapshot(self):
        """
        Return a full point-in-time dump of cluster state, for the
        "Snapshot Cluster" export.
        """
        return {
            "taken_at": datetime.now(UTC).isoformat(),
            "cluster_state": self.get_cluster_state(),
            "nodes": self.get_nodes(),
            "jobs": self.get_jobs(),
            "receipts": self.get_receipts(),
            "artifacts": self.get_artifacts(),
            "events": [
                {
                    "timestamp": e.timestamp.isoformat(),
                    "event_type": e.event_type,
                    "source": e.source,
                    "payload": e.payload,
                }
                for e in self.get_all_events()
            ],
        }

    def export_logs(self):
        """
        Collect stdout/stderr for every job that has actually run,
        for the "Export Logs" download.
        """
        lines = []
         
        with self.jobs_lock:
            jobs_snapshot = list(self.jobs.items())
        for job_id, job in jobs_snapshot:
            result = job.get("result")
            if not result:
                continue
            lines.append(f"===== {job_id} ({job['status']}) =====")
            lines.append(f"command: {job.get('command')}")
            lines.append(f"node: {job.get('node_id')}")
            if "stdout" in result:
                lines.append("--- stdout ---")
                lines.append(result.get("stdout") or "(empty)")
            if "stderr" in result:
                lines.append("--- stderr ---")
                lines.append(result.get("stderr") or "(empty)")
            if "message" in result:
                lines.append(f"error: {result['message']}")
            lines.append("")

        return "\n".join(lines) if lines else "No job output recorded yet."

    def emergency_stop(self):
        """
        Pause the scheduler and cancel every currently running job.
        Registered nodes are left in place (this is a stop, not a
        teardown) but no new work will be assigned until resumed.
        """
        self.pause_scheduler()

        cancelled = []
        with self.jobs_lock:
            jobs_snapshot = list(self.jobs.items())
        for job_id, job in self.jobs.items():
            if job["status"] == "running":
                try:
                    self.cancel_job(job_id)
                    cancelled.append(job_id)
                except ValueError:
                    pass

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="EMERGENCY_STOP",
            source="Coordinator", payload={"cancelled_job_ids": cancelled},
        ))
        print(f"[EMERGENCY] Stopped. Cancelled {len(cancelled)} running job(s).")
        return cancelled
        
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
                timestamp=datetime.now(UTC),
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
        with self.jobs_lock:
            jobs_snapshot = list(self.jobs.values())
 
        return {
            "total_nodes": self.get_total_node_count(),
            "idle_nodes": self.get_idle_node_count(),
            "registered_node_count": len(self.get_registered_nodes()),
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

    def submit_workflow(self, workflow):
        """
        Submit a workflow DAG for execution via the workflow engine.
        """
        return self.workflow_engine.submit_workflow(workflow)


    def get_workflows(self):
        """
        Return a summary of every workflow the engine knows about.
        Empty until a workflow has actually been submitted.
        """
        return [
            state.summary()
            for state in self.workflow_engine.states.values()
    ]


    def get_nodes(self):
        """
        Return a list of dicts describing every registered node, for
        use by presentation/dashboard clients. Uses getattr/get with
        defaults so a missing field never crashes the endpoint.
        """
        nodes = []
        
        for node_id, info in self.registry.nodes.items():
            nodes.append({
                "node_id": node_id,
                "status": info.get("status", "unknown"),
                "cpu": info.get("cpu", "N/A"),
                "memory": info.get("memory", "N/A"),
                "running_jobs": info.get("running_jobs", 0),
                "last_seen": (
                    info["last_seen"].isoformat()
                    if isinstance(info.get("last_seen"), datetime)
                    else info.get("last_seen", "N/A")
                ),
                "draining": info.get("draining", False),
            })

        return nodes

    def get_jobs(self):
        """
        Return a dashboard summary about all jobs.
        """
        jobs = []
        
        with self.jobs_lock:
            jobs_snapshot = list(self.jobs.items())
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
                "status": receipt.get("status", "unknown"),
                "created_at": receipt.get("issued_at", "N/A")
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
        
        
    def get_cluster_health(self):
        """
        Return overall cluster health, computed from real subsystem
        state (coordinator queue, node registry, receipts, storage
        disk, API latency) rather than a bare percentage. See
        HealthService for how each branch is derived.
        """
        health = self.health_service.compute()
        cluster = self.get_cluster_state()

        checks = health["checks"]

        return {
            # Overall cluster health
            "state": health["state"],
            "score": health["score"],
            "reason": health["reason"],
            "reasons": health["reasons"],
            "computed_at": health["computed_at"],

            # Per-branch detail, for the Health Inspector drill-down
            "checks": checks,

            # Kept for callers of the old shape (navbar badge, etc.)
            "services": {
                "coordinator": "online" if checks["coordinator"]["healthy"] else "degraded",
                "cluster": health["state"],
                "event_system": "running",
                "storage": "connected" if checks["storage"]["healthy"] else "degraded",
            },

            # Useful summary metrics
            "metrics": {
                "total_nodes": cluster["total_nodes"],
                "running_jobs": cluster["running_jobs"],
                "completed_jobs": cluster["completed_jobs"],
                "failed_jobs": cluster["failed_jobs"],
            },
        }

    def get_health_details(self):
        """
        Return the full health source-tree for the Health Inspector
        drill-down view (one entry per branch, each with its own
        metrics and explanation).
        """
        return self.health_service.compute()
    