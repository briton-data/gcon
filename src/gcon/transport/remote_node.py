"""
RemoteNodeProxy — makes a node connected over GrpcTransport look like
a local GCONNode/GCONAgent to the (untouched) scheduler and
NodeRegistry. NodeRegistry.register() only needs `.node_id` and
`.status`; the scheduler's dispatch path needs `.execute_job(job_id,
command, timeout=)`, `.cancel()`, and `.heartbeat()`. This class
supplies all of these -- `execute_job`/`cancel` delegate to the
transport over the network, while `heartbeat` is a purely local,
synchronous status snapshot (see below).
"""
from datetime import datetime, UTC


class RemoteNodeProxy:
    def __init__(self, node_id, transport, org_id=None):
        self.node_id = node_id
        self.transport = transport
        self.org_id = org_id
        self.status = "idle"

    def execute_job(self, job_id, command, timeout=None):
        self.status = "busy"
        try:
            response = self.transport.send_job(self.node_id, job_id, command, timeout=timeout)
            return response["result"]
        finally:
            self.status = "idle"

    def cancel(self):
        return self.transport.cancel_job(self.node_id, "")

    def heartbeat(self):
        """
        Return a local, synchronous heartbeat snapshot, matching
        GCONAgent.heartbeat()'s shape ({node_id, status, timestamp}).

        This is NOT a network round-trip to the remote agent -- the
        real periodic network heartbeat is the one the agent sends
        itself via AgentDaemon, delivered to the coordinator through
        the on_heartbeat callback wired up in run_coordinator.py and
        applied via receive_heartbeat()/NodeRegistry.heartbeat().
        This local method exists only so that
        GCONCoordinator.assign_job() -- which needs to optimistically
        stamp the registry with a fresh "busy" timestamp at the
        instant of dispatch, before the next real network heartbeat
        arrives -- can call `.heartbeat()` on ANY node it selected,
        whether that node is a local GCONAgent or a RemoteNodeProxy,
        without needing to know which. Without this method,
        RemoteNodeProxy raised AttributeError here, which is not a
        RuntimeError and therefore was NOT treated as scheduler_loop's
        expected "no node available, retry" case -- it propagated and
        killed the scheduler thread on the very first real (gRPC)
        job dispatch, permanently halting all further scheduling.
        """
        return {
            "node_id": self.node_id,
            "status": self.status,
            "timestamp": datetime.now(UTC),
        }

    def report_resources(self):
        """
        Return a resource snapshot in the same shape
        GCONAgent.report_resources() (-> ResourceMonitor.collect())
        produces: {node_id, cpu, memory, running_jobs, status,
        timestamp}. GCONCoordinator._run_job() calls this
        unconditionally right after a dispatch finishes, for every
        node type -- without it, RemoteNodeProxy raised
        AttributeError here, which _run_job does not catch (only the
        earlier `communication.send_job()` call is wrapped), silently
        killing that job's worker thread after the real remote
        execution had already completed successfully. The job was
        left stuck "running" forever: never marked completed, no
        receipt ever generated, and the node's registry entry never
        confirmed idle again for real work past that point.

        cpu/memory are reported as 0.0 here, NOT fabricated. The real
        agent process already measures and sends its own live
        cpu_percent/memory_percent on every periodic gRPC heartbeat
        (see AgentDaemon._heartbeat_loop), but that data currently
        only reaches receive_heartbeat() (status/timestamp), not
        receive_resource_report() -- wiring that through is a
        separate, tracked follow-up (see run_coordinator.py's
        on_heartbeat), not something this method should paper over by
        inventing numbers it doesn't actually have.
        """
        return {
            "node_id": self.node_id,
            "cpu": 0.0,
            "memory": 0.0,
            "running_jobs": 1 if self.status == "busy" else 0,
            "status": self.status,
            "timestamp": datetime.now(UTC).isoformat(),
        }