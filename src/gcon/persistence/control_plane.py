"""
ControlPlane — single entry point that wires a `ControlPlaneDatabase`
to every repository. This is the object the transport layer (and,
optionally, the coordinator) depends on via dependency injection;
nothing outside `gcon.persistence` talks to `sqlite3` directly.
"""

from __future__ import annotations

from typing import Optional

from gcon.persistence.db import ControlPlaneDatabase, Dialect
from gcon.persistence.repositories import (
    NodeRepository,
    NodeCapabilityRepository,
    JobRepository,
    JobAttemptRepository,
    ReceiptRepository,
    HeartbeatRepository,
    ClusterEventRepository,
    ExecutionLogRepository,
    SettingsRepository,
)


class ControlPlane:
    def __init__(self, path: Optional[str] = None, dialect: Optional[Dialect] = None):
        self.db = ControlPlaneDatabase(path=path, dialect=dialect)

        self.nodes = NodeRepository(self.db)
        self.node_capabilities = NodeCapabilityRepository(self.db)
        self.jobs = JobRepository(self.db)
        self.job_attempts = JobAttemptRepository(self.db)
        self.receipts = ReceiptRepository(self.db)
        self.heartbeats = HeartbeatRepository(self.db)
        self.cluster_events = ClusterEventRepository(self.db)
        self.execution_logs = ExecutionLogRepository(self.db)
        self.settings = SettingsRepository(self.db)

    def close(self) -> None:
        self.db.close()

    def __enter__(self) -> "ControlPlane":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
