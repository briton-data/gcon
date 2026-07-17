class CommunicationManager:
    """
    Simulates communication between the coordinator and compute nodes.
    """

    def __init__(self):
        self.nodes = {}
        
    def register_node(self, node):
        """
        Register a compute node with the communication manager.
        """

        self.nodes[node.node_id] = node
        
    def get_node(self, node_id):
        """
        Retrieve a registered node by its ID.
        """

        if node_id not in self.nodes:
            raise ValueError(f"Node '{node_id}' is not registered.")

        return self.nodes[node_id]
    
    def send_job(self, node_id, job_id, command, timeout=None):
        """
        Send a job to a registered node for execution.

        Args:
            timeout: Maximum seconds to allow the job to run before
                it's killed. Previously accepted nowhere in this call
                chain, so every job ran with an unbounded timeout
                regardless of what the caller intended -- a hung job
                script would block the coordinator's per-job worker
                thread (and the node it's "running" on) forever.
        """

        packet = {
        "node_id": node_id,
        "job_id": job_id,
        "command": command,
        "timeout": timeout,
    }
        print(f"[COMM] Sending packet to {node_id}")
        node = self.get_node(packet["node_id"])

        result = node.execute_job(
            packet["job_id"],
            packet["command"],
            timeout=packet["timeout"],
)
        print(f"[COMM] Response received from {node_id}")
        
        response = {
            "status": "success",
            "result": result
    }

        return response