"""
GCON Transport Layer.

`gcon.cluster.communication.CommunicationManager` depends only on the
`Transport` interface defined in `gcon.transport.interfaces` -- it
has no idea whether a given node is reached via an in-process object
reference (`LocalTransport`, used by every existing test and by
default so nothing outside this package changes behavior) or a real
network connection (`GrpcTransport`, used when a coordinator is
deployed with remote agents).
"""

from gcon.transport.interfaces import Transport, TransportError
from gcon.transport.local_transport import LocalTransport

__all__ = ["Transport", "TransportError", "LocalTransport"]
