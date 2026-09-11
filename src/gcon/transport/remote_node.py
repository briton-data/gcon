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
    def __init__(self, node_id, transport, org_id=None, address=None):
        self.node_id = node_id
        self.transport = transport
        self.status = "idle"
        # Which company this (dedicated) node belongs to, if any --
        # read by NodeRegistry.register() into the live registry
        # entry, same as GCONNode.org_id.
        self.org_id = org_id
        # The node's real remote IP, as seen by the gRPC server at
        # registration time (see grpc_transport._peer_address) -- not
        # self-reported, so it can't be spoofed by the connecting
        # agent. Also read by NodeRegistry.register().
        self.address = address

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
        produces. GCONCoordinator._run_job() calls this
        unconditionally right after a dispatch finishes, for every
        node type -- without it, RemoteNodeProxy raised
        AttributeError here, which _run_job does not catch (only the
        earlier `communication.send_job()` call is wrapped), silently
        killing that job's worker thread after the real remote
        execution had already completed successfully. The job was
        left stuck "running" forever: never marked completed, no
        receipt ever generated, and the node's registry entry never
        confirmed idle again for real work past that point. That's
        why this method must still return a dict, not raise or return
        None, even though it has nothing new to report.

        cpu/memory/gpu_* are deliberately OMITTED, not returned as
        0.0 -- update_node_resources() (registry.py) treats an absent
        key as "leave the last known reading in place," never a
        fabricated zero. This proxy object genuinely has no resource
        data of its own; the real numbers arrive independently via
        the agent's periodic gRPC heartbeat (AgentDaemon._heartbeat_loop
        -> grpc_transport.py's heartbeat handling -> run_coordinator.py's
        on_heartbeat -> receive_resource_report). This method used to
        return cpu=0.0/memory=0.0 literally, on the mistaken belief
        that heartbeat data didn't reach receive_resource_report() yet
        (it already did) -- that meant every remote job's completion
        silently stomped the real, live heartbeat-reported cpu/memory
        back to zero a moment later, every single time. Returning
        `status`/`timestamp` (which this proxy DOES know for certain,
        from its own local state) is correct and unaffected by this.
        """
        return {
            "node_id": self.node_id,
            "running_jobs": 1 if self.status == "busy" else 0,
            "status": self.status,
            "timestamp": datetime.now(UTC).isoformat(),
        }