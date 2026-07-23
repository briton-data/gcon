"""
CommunicationManager — the coordinator's single point of contact
with node transport, now backed by a swappable `Transport`
(`gcon.transport.interfaces.Transport`) instead of doing in-process
dict lookups + direct method calls inline.

`CommunicationManager` itself never imports `grpc`, never touches a
socket, and never knows whether a given node is local or remote --
it depends only on the `Transport` interface. By default it is
constructed with `LocalTransport`, which reproduces the exact
behavior this class always had, so every existing caller
(`GCONCoordinator()`, which does `CommunicationManager()` with no
arguments) is unaffected. To run against real, remote agents, a
coordinator process constructs `CommunicationManager(transport=
GrpcTransport(...))` instead -- see `gcon.transport.grpc_transport`.
"""

from typing import Optional

from gcon.transport.interfaces import Transport
from gcon.transport.local_transport import LocalTransport


class CommunicationManager:
    """
    Coordinates communication between the coordinator and compute
    nodes via an injected `Transport`.
    """

    def __init__(self, transport: Optional[Transport] = None):
        self.transport: Transport = transport or LocalTransport()

    def register_node(self, node):
        """
        Register a compute node with the communication manager.
        """
        self.transport.register_node(node)

    def unregister_node(self, node_id):
        """
        Remove a compute node from the communication manager.
        """
        self.transport.unregister_node(node_id)

    def get_node(self, node_id):
        """
        Retrieve a registered node by its ID.
        """
        return self.transport.get_node(node_id)

    def list_nodes(self):
        """
        Return the IDs of all currently registered nodes.
        """
        return self.transport.list_node_ids()

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
        return self.transport.send_job(node_id, job_id, command, timeout=timeout)

    def cancel_job(self, node_id, job_id):
        """
        Request cancellation of a job currently running on a node.
        """
        return self.transport.cancel_job(node_id, job_id)

    def shutdown(self, grace_period=None):
        """
        Gracefully shut down the underlying transport.
        """
        self.transport.shutdown(grace_period=grace_period)
