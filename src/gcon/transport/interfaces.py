"""
The `Transport` abstraction.

`gcon.cluster.communication.CommunicationManager` is written against
this interface only. It never imports `grpc`, never knows a node's
network address, and never knows whether "sending a job" means
calling a Python method on an in-process object or making a network
round trip to a remote machine. Two implementations exist:

  * `LocalTransport` (`local_transport.py`) -- preserves today's
    in-process behavior exactly (a registered "node" is just a
    Python object with `.execute_job()` / `.cancel()`, called
    directly). This is the default, so every existing test and every
    existing call site (`GCONCoordinator()`, which constructs
    `CommunicationManager()` with no arguments) is completely
    unaffected by this change.
  * `GrpcTransport` (`grpc_transport.py`) -- a real network transport.
    A "node" registered with this transport is a live gRPC control
    stream opened *by* a remote `AgentDaemon` process; the transport
    correlates outbound job dispatches with inbound results over
    that stream.

Both raise the same `TransportError` subclasses (`errors.py`) so
callers can handle failures uniformly regardless of which transport
is in use.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from gcon.transport.errors import TransportError

__all__ = ["Transport", "TransportError"]


class Transport(ABC):
    @abstractmethod
    def register_node(self, node: Any) -> None:
        """Register a node so it can receive jobs. `node` is whatever
        the concrete transport needs: a local object reference for
        `LocalTransport`, a `node_id` string that must already have an
        open control stream for `GrpcTransport`."""

    @abstractmethod
    def unregister_node(self, node_id: str) -> None:
        """Remove a node. Safe to call on an already-absent node_id."""

    @abstractmethod
    def get_node(self, node_id: str) -> Any:
        """Return the registered node handle, or raise NodeUnavailableError."""

    @abstractmethod
    def list_node_ids(self) -> List[str]:
        ...

    @abstractmethod
    def send_job(
        self,
        node_id: str,
        job_id: str,
        command: str,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Dispatch a job to `node_id` and block for its result (or
        raise on failure/timeout). Returns
        `{"status": "success", "result": {...}}` on success, matching
        the shape the coordinator has always received from
        `CommunicationManager.send_job`."""

    @abstractmethod
    def cancel_job(self, node_id: str, job_id: str) -> bool:
        """Request cancellation of a running job on `node_id`. Returns
        True if a cancellation request was actually delivered to a
        live node."""

    @abstractmethod
    def shutdown(self, grace_period: Optional[float] = None) -> None:
        """Gracefully stop the transport: stop accepting new
        registrations/dispatches, allow in-flight jobs up to
        `grace_period` seconds to finish, then tear down connections."""
