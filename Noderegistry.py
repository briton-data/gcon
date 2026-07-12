from datetime import datetime, UTC, timedelta
class NodeRegistry:
    """
    Stores and manages GCON nodes.
    """

    def __init__(self):
        self.nodes = {}
        self.timeout = timedelta(seconds=10)
        
    def register(self, node):
        """
        Register a new node.
        """

        if node.node_id in self.nodes:
            raise ValueError(f"Node '{node.node_id}' already exists.")

        from datetime import datetime, UTC

        self.nodes[node.node_id] = {
            "node": node,
            "last_seen": datetime.now(UTC),
            "status": node.status,
            
            "cpu": 0.0,
            "memory": 0.0,
            "running_jobs": 0,
            "resource_timestamp": None,
            "draining": False,
            
}
        
    def remove(self, node_id):
        """
        Remove a node from the registry.
        """

        if node_id not in self.nodes:
            raise ValueError(f"Node '{node_id}' does not exist.")

        del self.nodes[node_id] 

    def set_draining(self, node_id, draining):
        """
        Mark a node as draining (or not). A draining node keeps
        running any job it's currently executing, but the scheduler
        will not assign it new work.
        """

        if node_id not in self.nodes:
            raise ValueError(f"Node '{node_id}' does not exist.")

        self.nodes[node_id]["draining"] = draining
        
    def get_node(self, node_id):
        """
        Return a node by ID.
        """

        if node_id not in self.nodes:
             raise ValueError(f"Node '{node_id}' does not exist.")

        return self.nodes[node_id]["node"]
    
    def list_nodes(self):
        """
        Return all registered node IDs.
        """

        return list(self.nodes.keys())
    
    def available_nodes(self):
        """
        Return all idle, non-draining nodes.
        """

        return [     
        
        info["node"]
        for info in self.nodes.values()
        if info["status"] == "idle" and not info.get("draining")
]
        
    def get_node_info(self, node_id):
        """
        Return the complete registry information for a node.
        """

        if node_id not in self.nodes:
            raise ValueError(f"Node '{node_id}' does not exist.")

        return self.nodes[node_id]
    
    

    def heartbeat(self, node_id, status, timestamp):
        """
        Update heartbeat information for a node.
        """

        if node_id not in self.nodes:
            raise ValueError(f"Node '{node_id}' does not exist.")

        info = self.nodes[node_id]
        info["last_seen"] = timestamp
        info["status"] = status
        
        
        
    def check_node_health(self):
        """
        Mark nodes as offline if they have not sent
        a heartbeat within the timeout.

        Returns:
        list: IDs of nodes that became offline.
        """

        now = datetime.now(UTC)
        offline_nodes = []

        for node_id, info in self.nodes.items():

            elapsed = now - info["last_seen"]

            if elapsed > self.timeout and info["status"] != "offline":

                info["status"] = "offline"
                offline_nodes.append(node_id)
         
        
        return offline_nodes
    
    
    def update_node_resources(self, node_id, resources):
        """
        Update the latest resource information for a node.
        """

        if node_id not in self.nodes:
            raise ValueError(f"Node '{node_id}' does not exist.")

        info = self.nodes[node_id]

        info["cpu"] = resources["cpu"]
        info["memory"] = resources["memory"]
        info["running_jobs"] = resources["running_jobs"]
        info["resource_timestamp"] = resources["timestamp"]
        info["status"] = resources["status"]
        
        print(
            f"[RESOURCE] {node_id}: "
            f"status={resources['status']}, "
            f"jobs={resources['running_jobs']}"
)