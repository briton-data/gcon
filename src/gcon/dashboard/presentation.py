"""
GCON Presentation Layer

Acts as the boundary between the GCON Core Engine and any user
interface (web, terminal, CLI, or future clients).

The Presentation Layer never owns cluster state or business logic.
It simply provides a unified interface to the coordinator.
"""

from datetime import datetime, UTC

from gcon.cluster.autoscaler import AutoScaler


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
            "JOB_CANCELLED": f"Job {job_id} cancelled",
            
            "ARTIFACT_REGISTERED": f"Artifact registered for job {job_id}",
            "RECEIPT_GENERATED": f"Receipt generated for job {job_id}",
            "CLUSTER_STARTED": "Cluster started",
            "CLUSTER_STOPPED": "Cluster stopped",
            
            "SCHEDULER_PAUSED": "Scheduler paused",
            "SCHEDULER_RESUMED": "Scheduler resumed",
            "NODE_DRAINING": f"Node {node_id} draining — no new jobs will be assigned",
            "NODE_RESTARTED": f"Node {node_id} restarted",
            "QUEUE_CLEARED": f"Queue cleared ({len(payload.get('cleared_job_ids', []))} job(s))",
            "FAILED_JOBS_RETRIED": f"Retried {len(payload.get('job_ids', []))} failed job(s)",
            "FAILED_JOBS_CLEARED": f"Cleared {len(payload.get('job_ids', []))} failed job(s)",
            "EMERGENCY_STOP": f"Emergency stop — cancelled {len(payload.get('cancelled_job_ids', []))} running job(s)",
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
        Return all data required by the Home Dashboard, including the
        live-derived widgets (health, alerts, summaries) it needs to
        answer "is this trustworthy and does it need my attention?"
        within a glance.

        Health is computed once here and threaded into the widgets
        that depend on it, rather than recomputing the same live
        health-service pass multiple times per request.
        """
        health = self.get_cluster_health()
        trust = self.get_trust_score()

        return {
            "metrics": self.get_dashboard_metrics(),
            "nodes": self.get_nodes(),
            "jobs": self.get_jobs(),
            "events": self.get_events(),
            "storage": self.get_storage(),
            "workflows": self.get_workflows(),
            "receipts_count": len(self.coordinator.get_receipts()),
            "health": health,
            "trust": trust,
            "hero": self.get_hero_status(health, trust),
            "global_status": self.get_global_status(health),
            "node_summary": self.get_node_summary(),
            "receipts_summary": self.get_receipts_summary(),
            "storage_summary": self.get_storage_summary(health),
            "critical_alerts": self.get_critical_alerts(health),
            "execution_timeline": self.get_execution_timeline(),
    }

    # ------------------------------------------------------------------
    # Home Dashboard widgets
    # ------------------------------------------------------------------

    def get_node_summary(self):
        """
        Return a live breakdown of registered nodes by status, for the
        Node Summary widget.
        """
        return self.coordinator.get_node_summary()

    def get_receipts_summary(self):
        """
        Return live verification counts across all receipts, for the
        Receipt Summary widget. Verification is recomputed against
        each receipt's real signed proof (see Coordinator.get_receipts),
        never cached as a stale flag.
        """
        receipts = self.coordinator.get_receipts()
        verified = sum(1 for r in receipts if r.get("verified"))

        return {
            "total": len(receipts),
            "verified": verified,
            "unverified": len(receipts) - verified,
        }

    def get_storage_summary(self, health=None):
        """
        Return artifact counts/size alongside live disk capacity, for
        the Storage Summary widget. Disk figures are reused from the
        health service's storage branch rather than recomputed here.
        """
        health = health or self.get_cluster_health()
        artifacts = self.get_artifacts()
        total_bytes = sum(a.get("size") or 0 for a in artifacts)
        disk = health.get("checks", {}).get("storage", {}).get("metrics", {})

        return {
            "artifact_count": len(artifacts),
            "artifacts_total_bytes": total_bytes,
            "disk_free_bytes": disk.get("free_bytes", 0),
            "disk_total_bytes": disk.get("total_bytes", 0),
            "disk_remaining_pct": disk.get("remaining_capacity_pct", 0),
        }

    def get_critical_alerts(self, health=None):
        """
        Return operator-actionable alerts. This is a re-shaping of
        data already computed by the health service and job state —
        not a separate alerting engine — so it can never disagree
        with what Cluster Health and Executions already show.
        """
        health = health or self.get_cluster_health()
        alerts = []

        for reason in health.get("reasons", []):
            if not reason["healthy"]:
                alerts.append({
                    "id": f"health:{reason['check']}",
                    "severity": "critical" if health["state"] == "critical" else "warning",
                    "source": reason["label"],
                    "message": reason["detail"],
                })

        failed_jobs = [j for j in self.get_jobs() if j.get("status") == "failed"]
        if failed_jobs:
            alerts.append({
                "id": "jobs:failed",
                "severity": "warning",
                "source": "Executions",
                "message": f"{len(failed_jobs)} job(s) failed and may need attention.",
            })

        return alerts

    def get_execution_timeline(self, limit=8):
        """
        Return the most recently created jobs, newest first, for the
        Home Dashboard's Execution Timeline widget.
        """
        jobs = self.get_jobs()
        jobs_sorted = sorted(jobs, key=lambda j: j.get("created_at") or "", reverse=True)
        return jobs_sorted[:limit]

    def get_global_status(self, health=None):
        """
        Return the top-line system statuses shown in the Global Status
        Bar — each sourced from the same live health checks used
        elsewhere, never re-derived or hardcoded. Heartbeat age is the
        longest time since any active node last reported in.
        """
        health = health or self.get_cluster_health()
        checks = health.get("checks", {})
        coordinator_check = checks.get("coordinator", {})

        now = datetime.now(UTC)
        heartbeat_age_seconds = None
        for node in self.get_nodes():
            last_seen = node.get("last_seen")
            if not last_seen or last_seen == "N/A":
                continue
            try:
                seen_at = datetime.fromisoformat(last_seen)
            except ValueError:
                continue
            age = (now - seen_at).total_seconds()
            if heartbeat_age_seconds is None or age > heartbeat_age_seconds:
                heartbeat_age_seconds = age

        return {
            "coordinator_id": self.coordinator.coordinator_id,
            "cluster_state": health.get("state"),
            "coordinator_online": coordinator_check.get("healthy", False),
            "scheduler_running": coordinator_check.get("metrics", {}).get("running", False),
            "storage_online": checks.get("storage", {}).get("healthy", False),
            "receipt_engine_online": checks.get("receipt_service", {}).get("healthy", False),
            "heartbeat_age_seconds": (
                round(heartbeat_age_seconds) if heartbeat_age_seconds is not None else None
            ),
        }

    def get_trust_score(self):
        """
        Return the live execution-trust score (receipt verification
        integrity + node reliability + operational health). See
        HealthService.compute_trust — always freshly computed.
        """
        return self.coordinator.get_trust_score()

    def get_trust_history(self, limit=100):
        """
        Return the recorded trust-score time series for the Trust
        Center's history chart, newest last.
        """
        return self.coordinator.get_trust_history(limit=limit)

    def get_hero_status(self, health=None, trust=None):
        """
        Return the live figures shown in the dashboard's hero header:
        connected nodes, running executions, verified receipts, trust
        score, and coordinator status. Accepts already-computed
        health/trust so get_dashboard() doesn't pay for the same
        computation twice.
        """
        health = health or self.get_cluster_health()
        trust = trust or self.get_trust_score()
        global_status = self.get_global_status(health)
        nodes = self.get_nodes()
        jobs = self.get_jobs()
        receipts = self.get_receipts()

        return {
            "product_name": "GCON",
            "tagline": "Execution Verification Platform",
            "connected_nodes": sum(1 for n in nodes if n.get("status") != "offline"),
            "total_nodes": len(nodes),
            "running_executions": sum(1 for j in jobs if j.get("status") == "running"),
            "verified_receipts": sum(1 for r in receipts if r.get("verified")),
            "total_receipts": len(receipts),
            "trust_score": trust["trust_score"],
            "coordinator_id": global_status["coordinator_id"],
            "coordinator_online": global_status["coordinator_online"],
            "cluster_state": global_status["cluster_state"],
        }

    def get_trust_center(self):
        """
        Return everything the Trust Center page needs: the live trust
        score and its history, receipt verification statistics and
        the receipts currently failing verification, per-node trust
        status, and the underlying health breakdown — all sourced
        from the same live coordinator state used elsewhere, so this
        view can never disagree with the Home Dashboard.
        """
        health = self.get_cluster_health()
        trust = self.get_trust_score()
        receipts = self.get_receipts()
        failures = [r for r in receipts if not r.get("verified")]
        nodes = self.get_nodes()

        node_trust = [
            {
                "node_id": n["node_id"],
                "status": n["status"],
                "trusted": n["status"] != "offline",
                "last_seen": n["last_seen"],
            }
            for n in nodes
        ]

        return {
            "trust": trust,
            "history": self.get_trust_history(),
            "receipts_summary": self.get_receipts_summary(),
            "verification_failures": failures,
            "node_trust": node_trust,
            "health": health,
            "verification_timeline": self.get_events(limit=30),
        }

    # ------------------------------------------------------------------
    # Cluster Visualization
    # ------------------------------------------------------------------

    def get_topology(self):
        """
        Return a coordinator/node graph describing the current cluster
        shape, for the live topology view. Includes enough detail per
        node (cpu/memory/heartbeat/draining) to power click-to-inspect
        without a second round-trip.
        """
        nodes = self.coordinator.get_nodes()
        global_status = self.get_global_status()

        return {
            "coordinator": {
                "id": self.coordinator.coordinator_id,
                "online": global_status["coordinator_online"],
                "scheduler_running": global_status["scheduler_running"],
                "started_at": self.coordinator.started_at.isoformat(),
                "total_nodes": len(nodes),
                "running_jobs": sum(1 for j in self.get_jobs() if j.get("status") == "running"),
            },
            "nodes": [
                {
                    "node_id": node["node_id"],
                    "status": node["status"],
                    "cpu": node["cpu"],
                    "memory": node["memory"],
                    "running_jobs": node["running_jobs"],
                    "last_seen": node["last_seen"],
                    "draining": node["draining"],
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

    def get_receipt_detail(self, receipt_id):
        """
        Return the full record for one receipt (proof, live
        verification, execution details, artifacts) for the Receipt
        Explorer's detail view.
        """
        return self.coordinator.get_receipt_detail(receipt_id)

    def get_execution_detail(self, job_id):
        """
        Return the full lifecycle record for one execution (status,
        timestamps, artifacts, and its receipt's live verification if
        one exists) for the Executions page's detail view.
        """
        return self.coordinator.get_execution_detail(job_id)

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
    
    
    # ------------------------------------------------------------------
# Operations Panel — scheduler control
# ------------------------------------------------------------------

    def pause_scheduler(self):
        """
        Stop assigning new jobs to nodes. Already-running jobs finish
        normally.
        """
        self.coordinator.pause_scheduler()
        return {"scheduler_paused": True}

    def resume_scheduler(self):
        """
        Resume assigning queued jobs to idle nodes.
        """
        self.coordinator.resume_scheduler()
        return {"scheduler_paused": False}
    
    
    # ------------------------------------------------------------------
# Operations Panel — node lifecycle control
# ------------------------------------------------------------------

    def drain_node(self, node_id):
        """
        Stop assigning new jobs to a node without interrupting the
    job it may currently be running.
        """
        self.coordinator.drain_node(node_id)
        return {"node_id": node_id, "draining": True}

    def restart_worker(self, node_id):
        """
        Cancel a node's in-flight job (if any) and reset it to idle,
        keeping it registered.
        """
        self.coordinator.restart_worker(node_id)
        return {"node_id": node_id, "restarted": True}

    def stop_worker(self, node_id):
        """
        Cancel a node's in-flight job (if any) and remove it from
    the cluster entirely.
        """
        self.coordinator.stop_worker(node_id)
        return {"node_id": node_id, "stopped": True}

    def rediscover_nodes(self):
        """
        Force an immediate heartbeat freshness check across every
        registered node, recovering jobs from any that have gone
        silent.
        """
        return self.coordinator.rediscover_nodes()
    
    # ------------------------------------------------------------------
# Operations Panel — job control
# ------------------------------------------------------------------

    def cancel_job(self, job_id):
        """
        Cancel a specific running job by killing its process.
        """
        killed = self.coordinator.cancel_job(job_id)
        return {"job_id": job_id, "cancelled": True, "process_killed": killed}

    def clear_queue(self):
        """
        Remove every job still waiting in the queue.
        """
        return {"cleared_job_ids": self.coordinator.clear_queue()}

    def retry_failed_jobs(self):
        """
        Re-queue every currently failed job for another attempt.
        """
        return {"retried_job_ids": self.coordinator.retry_failed_jobs()}

    def clear_failed_jobs(self):
        """
        Permanently drop every currently failed job.
        """
        return {"cleared_job_ids": self.coordinator.clear_failed_jobs()}
    
    # ------------------------------------------------------------------
# Operations Panel — receipts, snapshots, export, emergency control
# ------------------------------------------------------------------

    def verify_all_receipts(self):
        """
        Cryptographically verify every stored receipt.
        """
        return self.coordinator.verify_all_receipts()

    def get_cluster_snapshot(self):
        """
        Return a full point-in-time dump of cluster state.
        """
        return self.coordinator.get_cluster_snapshot()

    def export_logs(self):
        """
        Return collected stdout/stderr for every job that has run.
        """
        return self.coordinator.export_logs()

    def emergency_stop(self):
        """
        Pause the scheduler and cancel every currently running job.
        """
        return {"cancelled_job_ids": self.coordinator.emergency_stop()}