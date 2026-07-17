class JobDispatcher:
    """
    Dispatches jobs to available GCON nodes.
    """

    def __init__(self, registry):
        self.registry = registry

    def dispatch(self, command):
        """
        Dispatch a job to the first available node.
        """

        available = self.registry.available_nodes()

        if not available:
            raise RuntimeError("No available nodes.")

        node = available[0]

        return node.execute_job(command)
    
    
