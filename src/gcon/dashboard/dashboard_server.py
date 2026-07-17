from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent

from .presentation import PresentationLayer
from .web_server import WebServer



def main():
    coordinator = GCONCoordinator()
    agent1 = GCONAgent("node-001")
    agent2 = GCONAgent("node-002")
    agent3 = GCONAgent("node-003")

    coordinator.registry.register(agent1)
    agent1.start_heartbeat(coordinator)
    
    coordinator.registry.register(agent2)
    agent2.start_heartbeat(coordinator)
    
    coordinator.registry.register(agent3)
    agent3.start_heartbeat(coordinator)
    
    
    coordinator.submit_job(
    "job-001",
    "echo Hello from GCON"
)

    coordinator.submit_job(
    "job-002",
    "python worker.py"
)

    coordinator.submit_job(
    "job-003",
    "backup_database"
)
    
    presentation = PresentationLayer(coordinator)

    server = WebServer(presentation)

    server.start()
    

if __name__ == "__main__":
    main()