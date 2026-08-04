"""
Versioned migrations for GCON's identity/platform database
(gcon.storage.database.Database), brought up to the same pattern as
gcon.persistence.db's control-plane migrations: each Migration is
applied at most once, tracked in a `schema_migrations` table, in
ascending version order. To change this schema in the future, add a
new Migration with the next version number -- never edit one that has
already shipped.

Migration 1 is exactly the CREATE TABLE IF NOT EXISTS schema this
module already had before it had a migration registry at all: wrapping
it as version 1 does not touch or lose any existing database. A
database that already has these tables (created by the old
`_conn.executescript(SCHEMA)` path) simply has migration 1 recorded as
applied on next boot and every statement in it no-ops (`IF NOT
EXISTS`); a genuinely fresh database creates them for the first time.
Every subsequent migration is free to assume they exist.
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
        name="initial_identity_schema",
        up_sql=[
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id         TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                email           TEXT NOT NULL,
                role            TEXT NOT NULL,
                organization_id TEXT,
                status          TEXT NOT NULL,
                avatar_initials TEXT,
                created_at      TEXT NOT NULL,
                last_active     TEXT NOT NULL,
                password_hash   TEXT,
                stats_json       TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)",
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                key_id        TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                owner_user_id TEXT NOT NULL,
                scopes_json   TEXT NOT NULL,
                secret        TEXT NOT NULL,
                created_at    TEXT NOT NULL,
                expires_at    TEXT,
                last_used_at  TEXT,
                usage_count   INTEGER NOT NULL DEFAULT 0,
                status        TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_api_keys_secret ON api_keys (secret)",
            """
            CREATE TABLE IF NOT EXISTS organizations (
                org_id          TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                plan            TEXT NOT NULL,
                created_at      TEXT NOT NULL,
                storage_used_gb REAL NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS teams (
                team_id       TEXT PRIMARY KEY,
                org_id        TEXT NOT NULL,
                name          TEXT NOT NULL,
                admin_user_id TEXT,
                member_ids_json TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_teams_org ON teams (org_id)",
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                entry_id  TEXT PRIMARY KEY,
                actor     TEXT NOT NULL,
                action    TEXT NOT NULL,
                target    TEXT,
                timestamp TEXT NOT NULL,
                seq       INTEGER
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_audit_seq ON audit_log (seq)",
            """
            CREATE TABLE IF NOT EXISTS notifications (
                notification_id TEXT PRIMARY KEY,
                type            TEXT NOT NULL,
                message         TEXT NOT NULL,
                severity        TEXT NOT NULL,
                category        TEXT NOT NULL,
                timestamp       TEXT NOT NULL,
                read            INTEGER NOT NULL DEFAULT 0,
                seq             INTEGER
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_notif_seq ON notifications (seq)",
        ],
    ),
    Migration(
        version=2,
        name="sessions_and_login_rate_limit",
        up_sql=[
            # Backs auth.SessionManager -- was purely in-memory, so a
            # restart force-logged-out every user. token is the opaque
            # session cookie value itself.
            """
            CREATE TABLE IF NOT EXISTS sessions (
                token      TEXT PRIMARY KEY,
                user_id    TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions (user_id)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions (expires_at)",
            # Backs rate_limit.LoginRateLimiter -- was purely
            # in-memory, so a restart cleared every lockout. One row
            # per attempt (pruned by window on read/write); `key` is
            # LoginRateLimiter._key(email, client_ip).
            """
            CREATE TABLE IF NOT EXISTS login_attempts (
                key          TEXT NOT NULL,
                attempted_at TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_login_attempts_key ON login_attempts (key)",
            """
            CREATE TABLE IF NOT EXISTS login_lockouts (
                key          TEXT PRIMARY KEY,
                locked_until TEXT NOT NULL
            )
            """,
        ],
    ),
]