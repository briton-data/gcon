import sys
sys.path.insert(0, "src")

from gcon.execution.agent import GCONAgent
from gcon.transport.agent_daemon import AgentDaemon

NODE_ID = "worker-01"
agent = GCONAgent(node_id=NODE_ID)
daemon = AgentDaemon(
    node_id=NODE_ID,
    coordinator_address="coordinator.internal:50051",
    cert_dir="/etc/gcon/certs",
    agent=agent,
    capabilities={"gpu": "A100"},
)
daemon.run_forever()