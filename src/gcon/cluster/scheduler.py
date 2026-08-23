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
        Select the least-loaded idle node and atomically claim it
        (marks it busy in the registry) in one locked operation, so
        two concurrent callers can never both walk away with the same
        node (AUDIT_REPORT.md 2.3 / audit finding C-1) -- see
        NodeRegistry.claim_best_idle_node() for how the race is
        closed.
        """

        def score(info):
            return (
                info["cpu"] * 0.5 +
                info["memory"] * 0.3 +
                info["running_jobs"] * 20
            )

        return self.registry.claim_best_idle_node(score)
    

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