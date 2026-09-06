import os
import psutil
from datetime import datetime, UTC


class ResourceMonitor:
    """
    Collect live resource usage for a GCON node.

    IMPORTANT: this reports metrics for the *current OS process*
    (via psutil.Process), not the whole host. The previous
    implementation used psutil.cpu_percent()/virtual_memory(), which
    are whole-machine statistics -- when multiple simulated
    GCONAgent nodes run inside a single test/dev process (as they do
    throughout this codebase's test suite), every node reported the
    exact same host-wide numbers, making the scheduler's load-based
    node selection (Scheduler.select_node, which weights cpu/memory)
    effectively noise: two "different" nodes always looked equally
    loaded regardless of which one was actually doing work.

    In a real deployment where each GCONAgent runs as its own OS
    process, psutil.Process(os.getpid()) correctly reflects that
    node's own usage. Process-level cpu_percent() also has its own
    caveat: the first call after process start (or after a long gap)
    returns 0.0/None because psutil needs two samples to compute a
    delta -- callers should not treat an initial 0.0 reading as
    "idle", only later readings.
    """

    def __init__(self, agent):
        self.agent = agent
        self._process = psutil.Process(os.getpid())
        # Prime the internal sample so the first real collect() call
        # returns a meaningful (non-zero-by-construction) delta
        # instead of always reporting 0.0 for the very first reading.
        self._process.cpu_percent(interval=None)

    def collect(self):
        # GPU data is intentionally NOT gathered every collect() call
        # (heartbeats fire frequently -- see AgentDaemon's heartbeat
        # loop) -- detect_gpu() shells out to GPUtil/nvidia-smi under
        # the hood, and polling that on every heartbeat tick would add
        # real overhead for data that changes on a much slower cadence
        # than cpu/memory does. Reuses the agent's own detect_gpu(),
        # the same call execute_job's periodic sampling uses, so the
        # live node-status reading and a job's own receipt draw from
        # one code path, not two that could silently disagree.
        gpu_info = self.agent.detect_gpu()
        load = gpu_info.get("load", 0) or 0
        return {
            "node_id": self.agent.node_id,
            "cpu": self._process.cpu_percent(interval=None),
            "memory": self._process.memory_percent(),
            "running_jobs": 1 if self.agent.status == "busy" else 0,
            "status": self.agent.status,
            "timestamp": datetime.now(UTC).isoformat(),
            "gpu_name": gpu_info.get("gpu_name", "Unknown"),
            "gpu_memory_total": gpu_info.get("memory_total", 0),
            "gpu_memory_used": gpu_info.get("memory_used", 0),
            "gpu_utilization_percent": round(load * 100, 2),
        }