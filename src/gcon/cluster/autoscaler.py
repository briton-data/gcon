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
        self.scaled_nodes= [] 

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
                self.scaled_nodes.pop()

                print(f"[AUTOSCALER] Removed {node_id}")
                return

        # Node isn't idle anymore
            self.scaled_nodes.pop()

        print("[AUTOSCALER] No removable idle nodes.")
    