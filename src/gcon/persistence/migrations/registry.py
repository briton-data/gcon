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
    Migration(
        version=2,
        name="add_org_id_to_nodes_and_jobs",
        up_sql=[
            # Dedicated-node multi-tenancy: which company a node belongs
            # to (set at agent registration, see AgentDaemon/run_worker.py
            # --org-id and grpc_transport.py's Register handler) and
            # which company a job was submitted for (set at submission
            # time, derived from the submitting API key's owner's
            # organization -- see api_v1.py's submit_job route).
            # NULL is a legitimate value: pre-migration rows, and any
            # node/job that genuinely isn't associated with a company
            # (e.g. local dev clusters), aren't backfilled or guessed at.
            "ALTER TABLE nodes ADD COLUMN org_id TEXT",
            "CREATE INDEX idx_nodes_org ON nodes (org_id)",
            "ALTER TABLE jobs ADD COLUMN org_id TEXT",
            "CREATE INDEX idx_jobs_org ON jobs (org_id)",
        ],
    ),
    Migration(
        version=3,
        name="billing_staking_webhooks_ha_retention",
        up_sql=[
            # ------------------------------------------------- billing
            # Invoices are generated from get_org_usage_summary() metering
            # data (see gcon.billing.invoicing) -- this table stores the
            # generated line items and status, not a payment integration
            # itself. No card/bank data ever lands here; a real charge is
            # made (or not) via gcon.billing.providers.PaymentProvider,
            # referenced by provider + provider_charge_id once attempted.
            """
            CREATE TABLE invoices (
                invoice_id          TEXT PRIMARY KEY,
                org_id              TEXT NOT NULL,
                period_start        TEXT NOT NULL,
                period_end          TEXT NOT NULL,
                currency            TEXT NOT NULL DEFAULT 'usd',
                amount_cents        INTEGER NOT NULL DEFAULT 0,
                status              TEXT NOT NULL DEFAULT 'draft',
                provider            TEXT,
                provider_charge_id  TEXT,
                provider_error      TEXT,
                created_at          TEXT NOT NULL,
                finalized_at        TEXT,
                paid_at             TEXT,
                UNIQUE (org_id, period_start, period_end)
            )
            """,
            "CREATE INDEX idx_invoices_org ON invoices (org_id)",
            "CREATE INDEX idx_invoices_status ON invoices (status)",
            """
            CREATE TABLE invoice_line_items (
                line_item_id   TEXT PRIMARY KEY,
                invoice_id     TEXT NOT NULL REFERENCES invoices (invoice_id) ON DELETE CASCADE,
                description    TEXT NOT NULL,
                quantity       REAL NOT NULL,
                unit           TEXT NOT NULL,
                unit_price_cents INTEGER NOT NULL,
                amount_cents   INTEGER NOT NULL
            )
            """,
            "CREATE INDEX idx_invoice_line_items_invoice ON invoice_line_items (invoice_id)",
            # ------------------------------------------------- staking
            # One bonded-deposit ledger row per node. Balances are GCON's
            # own accounting units (see gcon.execution.staking module
            # docstring) -- there is no on-chain token here, this is the
            # scaffolding a real bonded-stake system would sit behind.
            """
            CREATE TABLE node_stakes (
                node_id              TEXT PRIMARY KEY REFERENCES nodes (node_id) ON DELETE CASCADE,
                bonded_amount        INTEGER NOT NULL DEFAULT 0,
                unbonding_amount     INTEGER NOT NULL DEFAULT 0,
                unbonding_release_at TEXT,
                slashed_total        INTEGER NOT NULL DEFAULT 0,
                updated_at           TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE stake_events (
                event_id     TEXT PRIMARY KEY,
                node_id      TEXT NOT NULL,
                event_type   TEXT NOT NULL,
                amount       INTEGER NOT NULL,
                reason       TEXT,
                job_id       TEXT,
                receipt_id   TEXT,
                created_at   TEXT NOT NULL
            )
            """,
            "CREATE INDEX idx_stake_events_node ON stake_events (node_id)",
            "CREATE INDEX idx_stake_events_type ON stake_events (event_type)",
            # ------------------------------------------------- webhooks
            """
            CREATE TABLE webhook_subscriptions (
                subscription_id  TEXT PRIMARY KEY,
                org_id           TEXT NOT NULL,
                url              TEXT NOT NULL,
                secret           TEXT NOT NULL,
                event_types_json TEXT NOT NULL,
                active           INTEGER NOT NULL DEFAULT 1,
                created_at       TEXT NOT NULL
            )
            """,
            "CREATE INDEX idx_webhook_subscriptions_org ON webhook_subscriptions (org_id)",
            """
            CREATE TABLE webhook_deliveries (
                delivery_id      TEXT PRIMARY KEY,
                subscription_id  TEXT REFERENCES webhook_subscriptions (subscription_id) ON DELETE CASCADE,
                org_id           TEXT,
                url              TEXT NOT NULL,
                secret           TEXT NOT NULL,
                job_id           TEXT,
                event_type       TEXT NOT NULL,
                payload_json     TEXT NOT NULL,
                status           TEXT NOT NULL DEFAULT 'pending',
                attempt_count    INTEGER NOT NULL DEFAULT 0,
                response_code    INTEGER,
                last_error       TEXT,
                created_at       TEXT NOT NULL,
                last_attempt_at  TEXT,
                next_attempt_at  TEXT
            )
            """,
            "CREATE INDEX idx_webhook_deliveries_status ON webhook_deliveries (status)",
            "CREATE INDEX idx_webhook_deliveries_subscription ON webhook_deliveries (subscription_id)",
            # A job can also register a one-off callback URL at submit
            # time without a standing subscription (the common case for
            # a single batch of jobs) -- see JobSubmitRequest.callback_url.
            "ALTER TABLE jobs ADD COLUMN callback_url TEXT",
            # --------------------------------------------- HA / failover
            # Single shared row per lease name, contended by every
            # coordinator process pointed at this same control-plane DB.
            # SQLite's own locking (a write takes the one process-level
            # RLock plus the file lock) makes acquisition atomic without
            # needing a separate distributed-lock service. See
            # gcon.cluster.leader_election.
            """
            CREATE TABLE coordinator_leases (
                lease_name   TEXT PRIMARY KEY,
                holder_id    TEXT NOT NULL,
                term         INTEGER NOT NULL DEFAULT 0,
                acquired_at  TEXT NOT NULL,
                expires_at   TEXT NOT NULL,
                updated_at   TEXT NOT NULL
            )
            """,
        ],
    ),
    Migration(
        version=4,
        name="receipt_verification_and_uploaded_at_indexes",
        up_sql=[
            # The `verified` column has existed since the initial
            # schema but was never actually written by anything --
            # ReceiptRepository.mark_verified() had no caller, so it
            # always sat at its DEFAULT 0. Coordinator._commit_receipt_
            # verification now persists real results into it (see that
            # method), which is what makes the two indexes below useful
            # for real server-side filtering (WHERE verified = ?)
            # instead of always false-negative-matching every row.
            "CREATE INDEX idx_receipts_verified ON receipts (verified)",
            # Backs ORDER BY uploaded_at DESC LIMIT ? OFFSET ? --
            # ReceiptRepository.search_paginated's primary access
            # pattern, and list_recent's before it.
            "CREATE INDEX idx_receipts_uploaded_at ON receipts (uploaded_at)",
        ],
    ),
    Migration(
        version=5,
        name="org_scoped_enroll_tokens",
        up_sql=[
            # Per-org bootstrap secrets for the Enroll RPC (see
            # transport/grpc_transport.py and
            # persistence/repositories/enroll_tokens.py). Replaces the
            # single shared GCON_ENROLL_TOKEN env var as the source of
            # truth for which org a newly-enrolling worker belongs to.
            # token_hash only -- the plaintext token is never stored,
            # only returned once at creation time (see
            # EnrollTokenRepository.create_token). revoked_at NULL
            # means active; a token is never deleted outright so
            # audit history of who could once enroll survives revocation.
            """
            CREATE TABLE enroll_tokens (
                token_id     TEXT PRIMARY KEY,
                org_id       TEXT NOT NULL,
                token_hash   TEXT NOT NULL,
                label        TEXT,
                created_at   TEXT NOT NULL,
                revoked_at   TEXT,
                UNIQUE (token_hash)
            )
            """,
            "CREATE INDEX idx_enroll_tokens_org ON enroll_tokens (org_id)",
        ],
    ),
]
