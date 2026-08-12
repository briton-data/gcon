import threading
from datetime import datetime, UTC, timedelta
class NodeRegistry:
    """
    Stores and manages GCON nodes.
    """

    def __init__(self, timeout_seconds=10):
        """
        Args:
            timeout_seconds: How long a node can go without a
                heartbeat before check_node_health() marks it
                offline. Callers should derive this from the
                transport's configured heartbeat_interval_seconds *
                heartbeat_miss_threshold (see GCONCoordinator.__init__)
                rather than relying on the 10s default here -- a
                fixed 10s window is fine for a handful of nodes on a
                fast local heartbeat, but under load (many nodes
                registering at once, a busier gRPC thread pool, or an
                operator-configured slower heartbeat interval) it
                flips healthy-but-slow-to-report nodes to "offline"
                even though they're still alive.
        """
        self.nodes = {}
        self.timeout = timedelta(seconds=timeout_seconds)
        self._lock = threading.RLock()

    def register(self, node):
        """
        Register a new node, or re-register one whose previous
        session has already timed out (status "offline").

        A node reconnecting after its own process crashed/restarted
        is the normal case (e.g. an agent process dying and coming
        back up) and must succeed -- the coordinator has no way to
        tell "the old process is gone" apart from the heartbeat
        timeout already having flipped it offline, so that's exactly
        the signal used here. Only a node_id that is still actively
        alive (heartbeating within the timeout) is rejected, since
        that's the genuine conflict case: two live processes
        claiming the same node_id at once.
        """
        with self._lock:
            existing = self.nodes.get(node.node_id)
            if existing is not None and existing["status"] != "offline":
                raise ValueError(
                    f"Node '{node.node_id}' already exists and is still active."
                )

            self.nodes[node.node_id] = {
                "node": node,
                "last_seen": datetime.now(UTC),
                "status": node.status,

                "cpu": 0.0,
                "memory": 0.0,
                "running_jobs": 0,
                "resource_timestamp": None,
                "draining": False,
                # Which company this (dedicated) node belongs to, if
                # any -- read from the node object itself (set by
                # RemoteNodeProxy/GCONNode at construction) rather than
                # passed as a separate register() argument, so every
                # existing caller of register(node) keeps working
                # unchanged. getattr with a default: not every node
                # type is required to have this attribute.
                "org_id": getattr(node, "org_id", None),
            }

    def remove(self, node_id):
        """
        Remove a node from the registry.
        """
        with self._lock:
            if node_id not in self.nodes:
                raise ValueError(f"Node '{node_id}' does not exist.")

            del self.nodes[node_id]

    def set_draining(self, node_id, draining):
        """
        Mark a node as draining (or not). A draining node keeps
        running any job it's currently executing, but the scheduler
        will not assign it new work.
        """
        with self._lock:
            if node_id not in self.nodes:
                raise ValueError(f"Node '{node_id}' does not exist.")

            self.nodes[node_id]["draining"] = draining

    def get_node(self, node_id):
        """
        Return a node by ID.
        """
        with self._lock:
            if node_id not in self.nodes:
                raise ValueError(f"Node '{node_id}' does not exist.")

            return self.nodes[node_id]["node"]

    def list_nodes(self):
        """
        Return all registered node IDs.
        """
        with self._lock:
            return list(self.nodes.keys())

    def available_nodes(self):
        """
        Return all idle, non-draining nodes.
        """
        with self._lock:
            return [
                info["node"]
                for info in self.nodes.values()
                if info["status"] == "idle" and not info.get("draining")
            ]

    def get_node_info(self, node_id):
        """
        Return the complete registry information for a node.
        """
        with self._lock:
            if node_id not in self.nodes:
                raise ValueError(f"Node '{node_id}' does not exist.")

            return self.nodes[node_id]

    def snapshot(self):
        """
        Return a shallow copy of (node_id -> info) safe to iterate
        over without holding the registry lock.
        """
        with self._lock:
            return dict(self.nodes)

    def heartbeat(self, node_id, status, timestamp):
        """
        Update heartbeat information for a node.
        """
        with self._lock:
            if node_id not in self.nodes:
                raise ValueError(f"Node '{node_id}' does not exist.")
            
            info = self.nodes[node_id]
            current = info.get("last_seen")
            if current is None or timestamp >= current:
                info["last_seen"] = timestamp
                info["status"] = status

    def mark_offline(self, node_id):
        """
        Immediately mark a node offline outside the normal heartbeat-
        timeout sweep (check_node_health), e.g. when the transport
        layer reports the node's connection dropped. Thread-safe,
        unlike poking self.nodes[node_id] directly. No-op if the node
        is unknown or already offline; returns True if it changed the
        status.
        """
        with self._lock:
            info = self.nodes.get(node_id)
            if info is None or info["status"] == "offline":
                return False
            info["status"] = "offline"
            return True

    def check_node_health(self):
        """
        Mark nodes as offline if they have not sent
        a heartbeat within the timeout.

        Returns:
        list: IDs of nodes that became offline.
        """
        now = datetime.now(UTC)
        offline_nodes = []

        with self._lock:
            for node_id, info in list(self.nodes.items()):

                elapsed = now - info["last_seen"]

                if elapsed > self.timeout and info["status"] != "offline":

                    info["status"] = "offline"
                    offline_nodes.append(node_id)

        return offline_nodes

    def update_node_resources(self, node_id, resources):
        """
        Update the latest resource information for a node.
        """
        with self._lock:
            if node_id not in self.nodes:
                raise ValueError(f"Node '{node_id}' does not exist.")

            info = self.nodes[node_id]

            info["cpu"] = resources["cpu"]
            info["memory"] = resources["memory"]
            info["running_jobs"] = resources["running_jobs"]
            info["resource_timestamp"] = resources["timestamp"]
            info["status"] = resources["status"]

        print(
            f"[RESOURCE] {node_id}: "
            f"status={resources['status']}, "
            f"jobs={resources['running_jobs']}"
        )