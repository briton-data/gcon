from gcon.execution.agent import GCONAgent


class GCONNode:
    """
    Represents a compute node in the GCON network.
    """

    def __init__(self, node_id, org_id=None):
        self.node_id = node_id
        self.agent = GCONAgent(node_id)
        self.status = "idle"
        self.current_job = None
        # Which company this (dedicated) node belongs to, if any --
        # read by NodeRegistry.register() into the live registry entry.
        # None for shared/unassigned nodes (e.g. most local dev/test
        # clusters), which is a legitimate value, not a placeholder.
        self.org_id = org_id
        
    def execute_job(self, command):
        """
        Execute a job using the underlying agent.
        """

        self.status = "busy"
        self.current_job = command

        result = self.agent.execute_job(command)

        self.status = "idle"
        self.current_job = None

        return result

    def get_status(self):
        """
        Return the current status of the node.
        """

        return {
            "node_id": self.node_id,
            "status": self.status,
            "current_job": self.current_job
        }
        