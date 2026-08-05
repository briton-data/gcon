from gcon.execution.agent import GCONAgent


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
        """

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
    