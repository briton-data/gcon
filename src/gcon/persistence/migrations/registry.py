"""
Versioned migrations for GCON's control-plane database.

Each `Migration` is applied at most once (tracked in
`schema_migrations`, see `db.py`), in ascending `version` order,
inside its own transaction. To change the schema in the future, add
a new `Migration` to `MIGRATIONS` with the next version number —
never edit a migration that has already shipped, and never reorder
this list.

Every statement here is portable SQL (see the dialect notes in
`db.py`); the only engine-specific fragment is the `{{PK}}` token,
expanded by `render_migration_sql()` per dialect.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    up_sql: List[str] = field(default_factory=list)


MIGRATIONS: List[Migration] = [
    Migration(
        version=1,
        name="initial_control_plane_schema",
        up_sql=[
            # -------------------------------------------------- nodes
            """
            CREATE TABLE nodes (
                node_id             TEXT PRIMARY KEY,
                hostname            TEXT NOT NULL,
                status              TEXT NOT NULL DEFAULT 'unknown',
                transport_endpoint  TEXT,
                agent_version       TEXT,
                auth_fingerprint    TEXT,
                registered_at       TEXT NOT NULL,
                last_seen_at        TEXT,
                draining            INTEGER NOT NULL DEFAULT 0,
                metadata_json       TEXT,
                UNIQUE (auth_fingerprint)
            )
            """,
            "CREATE INDEX idx_nodes_status ON nodes (status)",
            "CREATE INDEX idx_nodes_last_seen ON nodes (last_seen_at)",
            # ---------------------------------------- node_capabilities
            """
            CREATE TABLE node_capabilities (
                capability_id     TEXT PRIMARY KEY,
                node_id           TEXT NOT NULL REFERENCES nodes (node_id) ON DELETE CASCADE,
                capability_key    TEXT NOT NULL,
                capability_value  TEXT NOT NULL,
                updated_at        TEXT NOT NULL,
                UNIQUE (node_id, capability_key)
            )
            """,
            "CREATE INDEX idx_node_capabilities_node ON node_capabilities (node_id)",
            # -------------------------------------------------- jobs
            """
            CREATE TABLE jobs (
                job_id           TEXT PRIMARY KEY,
                command          TEXT NOT NULL,
                status           TEXT NOT NULL DEFAULT 'pending',
                priority         INTEGER NOT NULL DEFAULT 0,
                workflow_id      TEXT,
                created_by       TEXT,
                timeout_seconds  INTEGER,
                submitted_at     TEXT NOT NULL,
                completed_at     TEXT,
                result_json      TEXT
            )
            """,
            "CREATE INDEX idx_jobs_status ON jobs (status)",
            "CREATE INDEX idx_jobs_workflow ON jobs (workflow_id)",
            "CREATE INDEX idx_jobs_submitted_at ON jobs (submitted_at)",
            # ----------------------------------------------- job_attempts
            """
            CREATE TABLE job_attempts (
                attempt_id          TEXT PRIMARY KEY,
                job_id              TEXT NOT NULL REFERENCES jobs (job_id) ON DELETE CASCADE,
                node_id             TEXT REFERENCES nodes (node_id) ON DELETE SET NULL,
                attempt_number      INTEGER NOT NULL,
                status              TEXT NOT NULL DEFAULT 'dispatched',
                request_message_id  TEXT,
                dispatched_at       TEXT NOT NULL,
                completed_at        TEXT,
                error               TEXT,
                UNIQUE (job_id, attempt_number),
                UNIQUE (request_message_id)
            )
            """,
            "CREATE INDEX idx_job_attempts_node ON job_attempts (node_id)",
            "CREATE INDEX idx_job_attempts_status ON job_attempts (status)",
            "CREATE INDEX idx_job_attempts_job ON job_attempts (job_id)",
            # -------------------------------------------------- receipts
            """
            CREATE TABLE receipts (
                receipt_id    TEXT PRIMARY KEY,
                job_id        TEXT NOT NULL REFERENCES jobs (job_id) ON DELETE CASCADE,
                attempt_id    TEXT REFERENCES job_attempts (attempt_id) ON DELETE SET NULL,
                node_id       TEXT REFERENCES nodes (node_id) ON DELETE SET NULL,
                receipt_hash  TEXT NOT NULL,
                signature     TEXT,
                payload_json  TEXT NOT NULL,
                uploaded_at   TEXT NOT NULL,
                verified      INTEGER NOT NULL DEFAULT 0,
                UNIQUE (receipt_hash)
            )
            """,
            "CREATE INDEX idx_receipts_job ON receipts (job_id)",
            "CREATE INDEX idx_receipts_node ON receipts (node_id)",
            # ------------------------------------------------- heartbeats
            """
            CREATE TABLE heartbeats (
                id             {{PK}},
                node_id        TEXT NOT NULL REFERENCES nodes (node_id) ON DELETE CASCADE,
                sequence       INTEGER NOT NULL,
                status         TEXT NOT NULL,
                cpu_percent    REAL,
                memory_percent REAL,
                running_jobs   INTEGER,
                received_at    TEXT NOT NULL,
                UNIQUE (node_id, sequence)
            )
            """,
            "CREATE INDEX idx_heartbeats_node_time ON heartbeats (node_id, received_at)",
            # ---------------------------------------------- cluster_events
            """
            CREATE TABLE cluster_events (
                id           {{PK}},
                event_type   TEXT NOT NULL,
                node_id      TEXT REFERENCES nodes (node_id) ON DELETE SET NULL,
                job_id       TEXT REFERENCES jobs (job_id) ON DELETE SET NULL,
                payload_json TEXT,
                created_at   TEXT NOT NULL
            )
            """,
            "CREATE INDEX idx_cluster_events_type_time ON cluster_events (event_type, created_at)",
            "CREATE INDEX idx_cluster_events_node ON cluster_events (node_id)",
            # --------------------------------------------- execution_logs
            """
            CREATE TABLE execution_logs (
                id          {{PK}},
                job_id      TEXT NOT NULL REFERENCES jobs (job_id) ON DELETE CASCADE,
                attempt_id  TEXT REFERENCES job_attempts (attempt_id) ON DELETE CASCADE,
                node_id     TEXT REFERENCES nodes (node_id) ON DELETE SET NULL,
                stream      TEXT NOT NULL DEFAULT 'stdout',
                sequence    INTEGER NOT NULL,
                content     TEXT NOT NULL,
                logged_at   TEXT NOT NULL,
                UNIQUE (attempt_id, stream, sequence)
            )
            """,
            "CREATE INDEX idx_execution_logs_job ON execution_logs (job_id)",
            "CREATE INDEX idx_execution_logs_attempt ON execution_logs (attempt_id)",
            # -------------------------------------------------- settings
            """
            CREATE TABLE settings (
                key         TEXT PRIMARY KEY,
                value       TEXT NOT NULL,
                updated_at  TEXT NOT NULL,
                updated_by  TEXT
            )
            """,
        ],
    ),
]
