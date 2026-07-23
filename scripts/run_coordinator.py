import sys
sys.path.insert(0, "src")

from gcon.persistence.control_plane import ControlPlane
from gcon.transport.config import TransportConfig
from gcon.transport.grpc_transport import GrpcTransport
from gcon.cluster.coordinator import GCONCoordinator

control_plane = ControlPlane(path="data/gcon_control_plane.db")
config = TransportConfig.load(control_plane)
coordinator = GCONCoordinator(transport=None)

def on_heartbeat(node_id, payload):
    coordinator.receive_heartbeat({"node_id": node_id, "status": payload["status"],  "timestamp": datetime.now(UTC), })
    
def on_node_registered(node_id, capabilities):
    proxy = RemoteNodeProxy(node_id, transport)
    coordinator.register_agent(proxy)
    print(f"[bridge] '{node_id}' registered with scheduler, capabilities={capabilities}")


def on_node_disconnected(node_id):
    print(f"[coordinator] node disconnected: {node_id}")

transport = GrpcTransport(
    control_plane=control_plane,
    config=config,
    on_heartbeat=on_heartbeat,
    on_node_disconnected=on_node_disconnected,
)
from gcon.cluster.communication import CommunicationManager
coordinator.communication = CommunicationManager(transport=transport)

transport.start()

coordinator = GCONCoordinator(transport=transport)  # requires edit #1 above

try:
    transport.wait_for_termination()
except KeyboardInterrupt:
    coordinator.shutdown()
    transport.shutdown()
    control_plane.close()