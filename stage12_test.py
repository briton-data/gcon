from coordinator import GCONCoordinator
from agent import GCONAgent
coordinator = GCONCoordinator()

node1 = GCONAgent("node-001")
node2 = GCONAgent("node-002")
node3 = GCONAgent("node-003")

coordinator.register_agent(node1)
coordinator.register_agent(node2)
coordinator.register_agent(node3)

for i in range(1, 11):
    coordinator.submit_job(
        f"job-{i:03}",
        'python -c "import time; time.sleep(5)"'
    )

import time
time.sleep(25)