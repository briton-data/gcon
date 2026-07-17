class Scheduler:
    """
    GCON Job Scheduler.

    Selects an available node from the NodeRegistry.
    """

    def __init__(self, registry):
        """
        Initialize the scheduler.

        Args:
            registry (NodeRegistry): The node registry.
        """
        self.registry = registry

    def select_node(self):
        """
        Select the least-loaded idle node.
        """

        best_node = None
        lowest_score = float("inf")

        # Snapshot rather than iterate the live registry dict: another
        # thread can concurrently register()/remove() a node, which
        # would otherwise raise "dictionary changed size during
        # iteration" mid-scan.
        for info in self.registry.snapshot().values():

            if info["status"] != "idle" or info.get("draining"):
                continue

            score = (
                info["cpu"] * 0.5 +
                info["memory"] * 0.3 +
                info["running_jobs"] * 20
        )
        
            if score < lowest_score:
                lowest_score = score
                best_node = info["node"]

        return best_node
    

    def has_available_node(self):
        """
        Check whether an idle node exists.

        Returns:
            bool
        """

        return len(self.registry.available_nodes()) > 0

    def node_count(self):
        """
        Return the number of registered nodes.
        """

        return len(self.registry.nodes)