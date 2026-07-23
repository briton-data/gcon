"""
LocalTransport — the default `Transport` implementation.

This is a straight extraction of what `CommunicationManager` always
did: hold a dict of node objects and call methods on them directly,
in-process. It exists so that:

  1. `CommunicationManager` can depend on the `Transport` interface
     instead of doing this inline, without changing behavior for any
     existing caller.
  2. Every existing test that constructs `GCONCoordinator()` /
     `CommunicationManager()` with no arguments keeps working exactly
     as before -- `LocalTransport` is the default transport.
"""

from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from gcon.transport.errors import NodeUnavailableError
from gcon.transport.interfaces import Transport


class LocalTransport(Transport):
    def __init__(self):
        self.nodes: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def register_node(self, node: Any) -> None:
        with self._lock:
            self.nodes[node.node_id] = node

    def unregister_node(self, node_id: str) -> None:
        with self._lock:
            self.nodes.pop(node_id, None)

    def get_node(self, node_id: str) -> Any:
        with self._lock:
            if node_id not in self.nodes:
                raise NodeUnavailableError(f"Node '{node_id}' is not registered.")
            return self.nodes[node_id]

    def list_node_ids(self) -> List[str]:
        with self._lock:
            return list(self.nodes.keys())

    def send_job(
        self,
        node_id: str,
        job_id: str,
        command: str,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        node = self.get_node(node_id)

        result = node.execute_job(job_id, command, timeout=timeout)

        return {"status": "success", "result": result}

    def cancel_job(self, node_id: str, job_id: str) -> bool:
        node = self.get_node(node_id)
        if hasattr(node, "cancel"):
            return bool(node.cancel())
        return False

    def shutdown(self, grace_period: Optional[float] = None) -> None:
        # In-process nodes have no connection to tear down; nothing to do.
        with self._lock:
            self.nodes.clear()
