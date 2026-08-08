"""
GCON local dev entrypoint.

Boots a real coordinator, registers a small set of local worker
agents, and starts the dashboard web server. This does NOT submit
any illustrative/demo jobs -- the queue starts empty and is
populated only by real submissions (via the dashboard, the public
/api/v1 REST API, or a workflow), same as a production deployment.
Node count is configurable via GCON_LOCAL_NODE_COUNT so this script
can double as a lightweight local cluster for manual testing without
hardcoding a fixed topology.
"""

import os

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent

from .presentation import PresentationLayer
from .web_server import WebServer


def main():
    coordinator = GCONCoordinator()

    node_count = int(os.environ.get("GCON_LOCAL_NODE_COUNT", "3"))
    for i in range(1, node_count + 1):
        agent = GCONAgent(f"node-{i:03d}")
        coordinator.registry.register(agent)
        agent.start_heartbeat(coordinator)

    presentation = PresentationLayer(coordinator)
    server = WebServer(presentation)
    server.start()


if __name__ == "__main__":
    main()