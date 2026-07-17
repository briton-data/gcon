"""
mock_network.py — network-condition simulation for GCON tests.

IMPORTANT (see AUDIT_REPORT.md, section 4): GCON's current
CommunicationManager (communication.py) does not use a real network at
all — `send_job` is a direct in-process Python method call onto a
`GCONAgent` object living in the same process and memory space as the
coordinator. There is nothing today for latency, packet loss, or a
partition to happen *to*.

This module solves that by providing a drop-in replacement,
`SimulatedNetworkCommunicationManager`, that implements the exact same
interface as `communication.CommunicationManager` (`register_node`,
`get_node`, `send_job`) but routes every call through a configurable
`NetworkConditions` profile first — so today's tests can exercise
latency/loss/duplication/reordering/partition behavior *at the seam*
where a real RPC client will eventually sit, without waiting for that
client to be built. When CommunicationManager is replaced with a real
transport, this harness continues to work by wrapping the new client
instead of `GCONAgent` directly.

Usage:
    coordinator.communication = SimulatedNetworkCommunicationManager()
    coordinator.communication.conditions.packet_loss_rate = 0.1
    coordinator.communication.conditions.latency_ms = (50, 400)  # jittered range
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set


class NetworkPartitionError(Exception):
    """Raised when a call is attempted across a simulated partition."""


class PacketLostError(Exception):
    """Raised when a simulated packet is dropped in transit."""


@dataclass
class NetworkConditions:
    """
    Tunable network-fault profile. All rates are probabilities in
    [0, 1] evaluated independently per call unless noted otherwise.
    """
    latency_ms: tuple = (0, 0)          # (min, max) — randomized delay per call
    jitter_ms: float = 0.0              # extra uniform jitter added on top
    packet_loss_rate: float = 0.0       # probability the call never "arrives"
    duplicate_rate: float = 0.0         # probability the call is executed twice
    max_duplicates: int = 1             # extra executions beyond the first, if duplicated
    reorder_rate: float = 0.0           # probability of being queued for delayed/out-of-order delivery
    reorder_delay_ms: tuple = (100, 500)
    corrupt_rate: float = 0.0           # probability the response payload is corrupted
    partitioned_nodes: Set[str] = field(default_factory=set)  # node_ids unreachable right now

    def sample_latency_seconds(self) -> float:
        lo, hi = self.latency_ms
        base = random.uniform(lo, hi) if hi > lo else lo
        jitter = random.uniform(0, self.jitter_ms)
        return (base + jitter) / 1000.0


class NetworkSimulator:
    """
    Shared fault-injection engine. Owns the NetworkConditions profile
    and applies it around an arbitrary callable, so it can be reused by
    both the CommunicationManager wrapper below and by heartbeat/
    resource-report call sites if those are wrapped too.
    """

    def __init__(self, conditions: Optional[NetworkConditions] = None):
        self.conditions = conditions or NetworkConditions()
        self._lock = threading.Lock()
        self._reorder_queue: List[Callable[[], None]] = []
        self._stats = {
            "calls": 0, "dropped": 0, "duplicated": 0,
            "reordered": 0, "partitioned": 0, "corrupted": 0,
        }

    # -- partition control -------------------------------------------------

    def partition(self, node_id: str) -> None:
        with self._lock:
            self.conditions.partitioned_nodes.add(node_id)

    def heal(self, node_id: str) -> None:
        with self._lock:
            self.conditions.partitioned_nodes.discard(node_id)

    def heal_all(self) -> None:
        with self._lock:
            self.conditions.partitioned_nodes.clear()

    def is_partitioned(self, node_id: str) -> bool:
        with self._lock:
            return node_id in self.conditions.partitioned_nodes

    # -- fault injection wrapper --------------------------------------------

    def call(self, node_id: str, fn: Callable[[], Dict], label: str = "") -> Dict:
        """
        Execute `fn()` (a zero-arg closure performing the actual RPC/
        method call) subject to the current NetworkConditions, and
        return its result. Raises NetworkPartitionError or
        PacketLostError to simulate the corresponding real-world
        failure instead of returning a result.
        """
        with self._lock:
            self._stats["calls"] += 1

        if self.is_partitioned(node_id):
            with self._lock:
                self._stats["partitioned"] += 1
            raise NetworkPartitionError(
                f"node '{node_id}' is unreachable (simulated partition)"
            )

        if random.random() < self.conditions.packet_loss_rate:
            with self._lock:
                self._stats["dropped"] += 1
            raise PacketLostError(
                f"packet to '{node_id}' lost in transit (simulated, label={label!r})"
            )

        delay = self.conditions.sample_latency_seconds()
        if delay > 0:
            time.sleep(delay)

        if random.random() < self.conditions.reorder_rate:
            # Simulate out-of-order delivery: this call is delayed
            # significantly longer than a "normal" one, so if the
            # caller issued a second call right after this one, that
            # second call's response can arrive first.
            with self._lock:
                self._stats["reordered"] += 1
            extra_delay = random.uniform(*self.conditions.reorder_delay_ms) / 1000.0
            time.sleep(extra_delay)

        result = fn()

        if random.random() < self.conditions.duplicate_rate:
            with self._lock:
                self._stats["duplicated"] += 1
            # Re-invoke fn() up to max_duplicates extra times to simulate
            # the receiver processing the same message more than once —
            # this is the exact scenario AUDIT_REPORT.md 4.4 flags as
            # entirely unhandled (no idempotency key anywhere in the
            # heartbeat/resource-report/job-dispatch path).
            for _ in range(self.conditions.max_duplicates):
                try:
                    fn()
                except Exception:
                    pass  # duplicate executions failing is itself an interesting signal

        if random.random() < self.conditions.corrupt_rate and isinstance(result, dict):
            result = dict(result)
            result["_corrupted"] = True
            result.pop("status", None)  # drop a field to simulate malformed payload

        return result

    def stats(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._stats)


class SimulatedNetworkCommunicationManager:
    """
    Drop-in replacement for communication.CommunicationManager that
    routes every send_job through a NetworkSimulator. Same public
    surface (register_node, get_node, send_job) so it can be assigned
    directly onto `coordinator.communication` in a test without any
    other coordinator code changing.
    """

    def __init__(self, conditions: Optional[NetworkConditions] = None):
        self.nodes: Dict[str, object] = {}
        self.simulator = NetworkSimulator(conditions)
        self.conditions = self.simulator.conditions  # convenience alias

    def register_node(self, node) -> None:
        self.nodes[node.node_id] = node

    def get_node(self, node_id: str):
        if node_id not in self.nodes:
            raise ValueError(f"Node '{node_id}' is not registered.")
        return self.nodes[node_id]

    def send_job(self, node_id: str, job_id: str, command: str,
                 timeout: Optional[int] = None) -> Dict:
        node = self.get_node(node_id)

        def do_call():
            result = node.execute_job(job_id, command, timeout=timeout)
            return {"status": "success", "result": result}

        return self.simulator.call(node_id, do_call, label=f"send_job:{job_id}")


# ---------------------------------------------------------------------
# Convenience context managers for common partition scenarios
# ---------------------------------------------------------------------

class temporary_partition:
    """
    Context manager: partition `node_ids` from the network for the
    duration of the `with` block, then heal automatically (even on
    exception) so one failing test can't leave later tests running
    against a permanently-broken simulator instance.
    """

    def __init__(self, simulator: NetworkSimulator, node_ids: List[str]):
        self.simulator = simulator
        self.node_ids = node_ids

    def __enter__(self):
        for node_id in self.node_ids:
            self.simulator.partition(node_id)
        return self

    def __exit__(self, exc_type, exc, tb):
        for node_id in self.node_ids:
            self.simulator.heal(node_id)
        return False


class split_brain_partition:
    """
    Partition the cluster into two disjoint groups that cannot reach
    each other (from the coordinator's point of view, both groups are
    simply flagged unreachable while the coordinator itself keeps
    running — this exercises the coordinator's SINGLE-node-partition
    handling; it cannot exercise true multi-coordinator split-brain
    since GCON has no multi-coordinator support today, per
    AUDIT_REPORT.md 4.5).
    """

    def __init__(self, simulator: NetworkSimulator,
                 group_a: List[str], group_b: List[str]):
        self.simulator = simulator
        self.all_nodes = list(group_a) + list(group_b)

    def __enter__(self):
        for node_id in self.all_nodes:
            self.simulator.partition(node_id)
        return self

    def __exit__(self, exc_type, exc, tb):
        for node_id in self.all_nodes:
            self.simulator.heal(node_id)
        return False


# ---------------------------------------------------------------------
# Presets matching common real-world profiles, for quick test setup
# ---------------------------------------------------------------------

PRESETS = {
    "ideal": NetworkConditions(),
    "datacenter_lan": NetworkConditions(latency_ms=(0.2, 1.5), jitter_ms=0.5),
    "cross_region": NetworkConditions(latency_ms=(30, 120), jitter_ms=20,
                                       packet_loss_rate=0.001),
    "flaky_wifi": NetworkConditions(latency_ms=(20, 300), jitter_ms=100,
                                     packet_loss_rate=0.05, duplicate_rate=0.02,
                                     reorder_rate=0.05),
    "degraded_congested": NetworkConditions(latency_ms=(200, 2000), jitter_ms=500,
                                             packet_loss_rate=0.15, duplicate_rate=0.05,
                                             reorder_rate=0.1),
    "hostile": NetworkConditions(latency_ms=(0, 5000), jitter_ms=1000,
                                  packet_loss_rate=0.3, duplicate_rate=0.15,
                                  reorder_rate=0.2, corrupt_rate=0.05),
}


def preset(name: str) -> NetworkConditions:
    if name not in PRESETS:
        raise ValueError(f"Unknown network preset '{name}'. Options: {list(PRESETS)}")
    import copy
    return copy.deepcopy(PRESETS[name])
