"""
ControlPlane — single entry point that wires a `ControlPlaneDatabase`
to every repository. This is the object the transport layer (and,
optionally, the coordinator) depends on via dependency injection;
nothing outside `gcon.persistence` talks to `sqlite3` directly.
"""

from __future__ import annotations

from typing import Optional

from gcon.config import resolve_control_plane_postgres_dsn
from gcon.persistence.db import ControlPlaneDatabase, Dialect, PostgresDialect
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
    StakeRepository,
    InvoiceRepository,
    WebhookRepository,
    LeaseRepository,
    EnrollTokenRepository,
    TelemetryRepository,
    NodeEnrollmentAuditRepository,
    IdempotencyKeyRepository,
    IncidentRepository,
    MetricSnapshotRepository,
    ObservabilityQueries,
    WorkflowRepository,
)


class ControlPlane:
    def __init__(self, path: Optional[str] = None, dialect: Optional[Dialect] = None):
        # An explicit `dialect` (or an explicit `path` with no
        # dialect override -- unchanged existing behavior, defaults to
        # SQLite) always wins. Only when neither is given do we check
        # GCON_CONTROL_PLANE_DATABASE_URL and, if it's set, connect to
        # that Postgres server instead -- this is opt-in, so every
        # existing single-host deployment that's never heard of this
        # keeps working exactly as before with zero config changes.
        if dialect is None and path is None:
            dsn = resolve_control_plane_postgres_dsn()
            if dsn:
                dialect = PostgresDialect()
                path = dsn
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
        self.stakes = StakeRepository(self.db)
        self.invoices = InvoiceRepository(self.db)
        self.webhooks = WebhookRepository(self.db)
        self.leases = LeaseRepository(self.db)
        self.enroll_tokens = EnrollTokenRepository(self.db)
        self.telemetry_events = TelemetryRepository(self.db)
        self.node_enrollment_audit = NodeEnrollmentAuditRepository(self.db)
        self.idempotency_keys = IdempotencyKeyRepository(self.db)
        self.metric_snapshots = MetricSnapshotRepository(self.db)
        self.incidents = IncidentRepository(self.db)
        self.workflows = WorkflowRepository(self.db)
        self.obs_queries = ObservabilityQueries(self.db)

    def close(self) -> None:
        self.db.close()

    def __enter__(self) -> "ControlPlane":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
