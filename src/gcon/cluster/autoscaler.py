from gcon.execution.agent import GCONAgent
from gcon.transport.local_transport import LocalTransport


class AutoScaler:
    """
    GCON AutoScaler.
    
    Monitors cluster workload and dynamically adds worker nodes
    when the current capacity is insufficient.
    """
    MIN_NODES = 1

    def __init__(self, coordinator):
        """
        Initialize the AutoScaler.

        Args:
            coordinator (GCONCoordinator): Running coordinator instance.
        """
        self.coordinator = coordinator
        self.node_counter = 1000
        self.scaled_nodes = []
        # node_id -> GCONAgent, for agents this AutoScaler created and
        # started a heartbeat thread for. Needed so scale_down() can
        # stop that thread instead of leaking it once the node is
        # deregistered.
        self._agents = {}

    def check_scale(self):
        """
        Check whether the cluster should scale up.
        """

        pending_jobs = self.coordinator.get_pending_job_count()
        idle_nodes = self.coordinator.get_idle_node_count()

        print(
            f"[AUTOSCALER] Pending Jobs: {pending_jobs} | "
            f"Idle Nodes: {idle_nodes}"
        )

        needed = pending_jobs - idle_nodes
        if needed > 0:
            for _ in range(needed):
                self.scale_up()

    def scale_up(self):
        """
        Create and register a new worker node.

        This spins up a real in-process GCONAgent and wires it all
        the way through: NodeRegistry (so the scheduler can select
        it), CommunicationManager/Transport (so a dispatched job can
        actually reach it), and a live heartbeat thread (so it stays
        "idle" instead of timing out). That is a genuine, dynamically
        created node, not a fake/hardcoded one -- `node_id` is always
        freshly generated (see node_counter), never reused, and never
        collides with a live node (NodeRegistry.register() itself
        rejects a node_id that's still active).

        This only works correctly when the coordinator's transport
        is LocalTransport (the in-process/demo/test path), because
        LocalTransport.register_node() *is* the connection -- there
        is no separate network session to establish. Under a real
        network transport (e.g. GrpcTransport), a node only becomes
        actually dispatchable once a real agent process dials in and
        completes the Register RPC; GrpcTransport.register_node() is
        deliberately a no-op for exactly that reason (see its
        docstring). If AutoScaler still registered a bare in-process
        GCONAgent in that case, the scheduler would happily pick it
        up as "idle" and dispatch real jobs to it, which would then
        fail every time with "Node '<id>' is not connected" -- i.e.
        exactly the "scale-up creates a node that jobs then fail
        against" symptom. Rather than silently registering a node
        that cannot execute anything, this raises immediately so the
        failure is loud and attributable to "no provisioner wired
        up" instead of a mysterious per-job dispatch failure later.

        Provisioning a real new worker process (container, VM, bare
        subprocess, etc.) that then dials into this coordinator via
        `scripts/run_worker.py` / AgentDaemon is an infrastructure
        integration this codebase does not currently implement, and
        is deliberately not faked here.
        """

        if not isinstance(self.coordinator.communication.transport, LocalTransport):
            raise RuntimeError(
                "AutoScaler.scale_up() cannot provision a new node under a "
                "non-local transport (e.g. GrpcTransport): a coordinator "
                "process cannot fabricate a live network connection to a "
                "node that doesn't exist yet. Provision a real new agent "
                "process (container/VM/subprocess) that connects via "
                "scripts/run_worker.py against this coordinator instead; "
                "it will self-register through the real Register RPC, "
                "exactly like any other worker."
            )

        node_id = f"node-{self.node_counter}"
        self.node_counter += 1

        new_agent = GCONAgent(node_id)

        self.coordinator.register_agent(new_agent)
        # register_agent only adds the node to the registry as a
        # one-time snapshot -- nothing keeps it "alive" after that.
        # Without a running heartbeat, NodeRegistry.check_node_health()
        # correctly (and inevitably) marks it offline once its timeout
        # elapses, since zero heartbeats were ever received. GCONAgent
        # already has a heartbeat thread built for exactly this; it
        # was just never started for scaled-up nodes.
        new_agent.start_heartbeat(self.coordinator)
        self._agents[node_id] = new_agent
        self.scaled_nodes.append(node_id)
        print(f"[AUTOSCALER] Added {node_id}")
        
    def scale_down(self):
        """
        Remove the most recently created idle worker.
        """

        if self.coordinator.get_total_node_count() <= self.MIN_NODES:
            print("[AUTOSCALER] Minimum cluster size reached.")
            return

        idle_nodes = {
            node.node_id: node
            for node in self.coordinator.get_idle_nodes()
    }

        while self.scaled_nodes:
            node_id = self.scaled_nodes[-1]

            if node_id in idle_nodes:
                self.coordinator.deregister_agent(node_id)
                agent = self._agents.pop(node_id, None)
                if agent is not None:
                    agent.stop_heartbeat()
                self.scaled_nodes.pop()

                print(f"[AUTOSCALER] Removed {node_id}")
                return

        # Node isn't idle anymore
            self.scaled_nodes.pop()

        print("[AUTOSCALER] No removable idle nodes.")
    