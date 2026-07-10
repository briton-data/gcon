import psutil
from datetime import datetime, UTC


class ResourceMonitor:
    """
    Collect live resource usage for a GCON node.
    """

    def __init__(self, agent):
        self.agent = agent

    def collect(self):
        return {
            "node_id": self.agent.node_id,
            "cpu": psutil.cpu_percent(interval=0.1),
            "memory": psutil.virtual_memory().percent,
            "running_jobs": 1 if self.agent.status == "busy" else 0,
            "status": self.agent.status,
            "timestamp": datetime.now(UTC).isoformat()
        }