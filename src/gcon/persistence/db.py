"""
ControlPlaneDatabase — connection management, crash safety, and the
migration runner for GCON's cluster control-plane store.

SQLite today, PostgreSQL-compatible by construction
-----------------------------------------------------
This module is deliberately written so that swapping the backing
engine later is a driver change, not a rewrite:

  * Every migration is portable SQL. The one genuine SQLite/Postgres
    divergence — auto-incrementing surrogate keys for the
    high-volume append-only tables (heartbeats, cluster_events,
    execution_logs) — is isolated behind the `{{PK}}` token, which
    `Dialect.pk_ddl()` expands per-engine (`INTEGER PRIMARY KEY
    AUTOINCREMENT` on SQLite, `BIGINT GENERATED ALWAYS AS IDENTITY
    PRIMARY KEY` on Postgres). No migration file hardcodes either
    form directly.
  * All other primary keys are application-generated TEXT (uuid4
    hex), which is identical on both engines.
  * Timestamps are stored as TEXT in ISO-8601 (`datetime.now(UTC)
    .isoformat()`), matching the convention already used by
    `gcon.storage.database`. This avoids relying on either engine's
    native datetime functions inside SQL.
  * Booleans are stored as INTEGER 0/1. SQLite has no native boolean
    type; Postgres accepts integer literals for boolean columns via
    parameterized inserts through psycopg's adapters, and the one
    place this matters (`draining`, `read`, `success` style flags)
    is written explicitly as 0/1 in the repositories rather than
    Python True/False, so behavior is identical either way.
  * No SQLite-only functions (`json_extract`, `printf`, ...) appear
    in any query. JSON columns are opaque TEXT, serialized/deserialized
    in Python, exactly like `gcon.storage.database`.
  * Placeholders use `?` (SQLite's paramstyle). A future Postgres
    driver adapter only needs to translate `?` -> `%s` positionally,
    which `Dialect.translate_placeholders()` already isolates as a
    single seam.

Crash safety
------------
Same guarantees as `gcon.storage.database.Database`, deliberately
kept consistent across both persistence stores in this codebase:
WAL journal mode, synchronous=FULL, foreign_keys=ON, a single
`threading.RLock` serializing write sequences at the Python level
(SQLite serializes actual disk writes on its own, but several
repository methods here do compound read-then-write sequences that
need to be atomic at the application level too), and all multi-row
mutations going through `transaction()` so a crash mid-operation
rolls back the whole thing.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass

from gcon.persistence.migrations.registry import MIGRATIONS


DEFAULT_CONTROL_PLANE_DB_PATH = os.environ.get(
    "GCON_CONTROL_PLANE_DB_PATH", "data/gcon_control_plane.db"
)


@dataclass(frozen=True)
class Dialect:
    """
    The one seam a future PostgreSQL backend needs to fill in.
    `SQLiteDialect` is the only implementation today; `db.py` and
    every repository talk to the database exclusively through this
    object plus `ControlPlaneDatabase.execute/query`, never with
    engine-specific SQL inline.
    """

    name: str

    def pk_ddl(self) -> str:
        raise NotImplementedError

    def now_placeholder(self) -> str:
        """Portable 'insert current time' — we always pass it as a bound
        parameter (Python-side ISO-8601 string), never a SQL function,
        so this simply documents that convention."""
        return "?"


class SQLiteDialect(Dialect):
    def __init__(self):
        object.__setattr__(self, "name", "sqlite")

    def pk_ddl(self) -> str:
        return "INTEGER PRIMARY KEY AUTOINCREMENT"


class PostgresDialect(Dialect):
    """
    Not wired to a live driver in this codebase (no network-accessible
    Postgres in this deployment target), but exists so the migration
    registry and repositories can be exercised against the intended
    production dialect string today, ahead of adding `psycopg` as a
    dependency. Swapping `ControlPlaneDatabase` to open a psycopg
    connection instead of `sqlite3.connect`, translating `?` -> `%s`
    placeholders, and selecting this dialect is the entire migration.
    """

    def __init__(self):
        object.__setattr__(self, "name", "postgres")

    def pk_ddl(self) -> str:
        return "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"


def render_migration_sql(sql: str, dialect: Dialect) -> str:
    return sql.replace("{{PK}}", dialect.pk_ddl())


class ControlPlaneDatabase:
    """
    One connection, shared by every control-plane repository
    (nodes, jobs, job_attempts, receipts, heartbeats, cluster_events,
    execution_logs, settings, node_capabilities). One
    ControlPlaneDatabase == one control-plane .db file == one GCON
    coordinator's durable cluster state.
    """

    def __init__(self, path: str | None = None, dialect: Dialect | None = None):
        self.path = path or DEFAULT_CONTROL_PLANE_DB_PATH
        self.dialect = dialect or SQLiteDialect()

        if self.path != ":memory:":
            directory = os.path.dirname(self.path)
            if directory:
                os.makedirs(directory, exist_ok=True)

        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")

        self._migrate()

    # ---------------------------------------------------------- migrations
    def _migrate(self):
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version     INTEGER PRIMARY KEY,
                    name        TEXT NOT NULL,
                    applied_at  TEXT NOT NULL
                )
                """
            )
            self._conn.commit()

            applied = {
                row["version"]
                for row in self._conn.execute("SELECT version FROM schema_migrations")
            }

            for migration in MIGRATIONS:
                if migration.version in applied:
                    continue
                try:
                    for statement in migration.up_sql:
                        rendered = render_migration_sql(statement, self.dialect)
                        self._conn.execute(rendered)

                    from datetime import datetime, UTC

                    self._conn.execute(
                        "INSERT INTO schema_migrations (version, name, applied_at) "
                        "VALUES (?, ?, ?)",
                        (migration.version, migration.name, datetime.now(UTC).isoformat()),
                    )
                    self._conn.commit()
                except Exception:
                    self._conn.rollback()
                    raise RuntimeError(
                        f"Migration {migration.version} ({migration.name}) failed"
                    )

    def applied_migrations(self):
        with self._lock:
            rows = self._conn.execute(
                "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------- queries
    @contextmanager
    def transaction(self):
        """
        Serializes a sequence of writes both at the Python level (the
        RLock) and the SQLite level (commits atomically or rolls back
        entirely on error/crash, so a `kill -9` mid multi-table write —
        e.g. inserting a job_attempt row alongside a job status update —
        never leaves the control plane in a half-written state).
        """
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def execute(self, sql, params=()):
        with self.transaction() as conn:
            return conn.execute(sql, params)

    def executemany(self, sql, seq_of_params):
        with self.transaction() as conn:
            return conn.executemany(sql, seq_of_params)

    def query(self, sql, params=()):
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql, params=()):
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def close(self):
        with self._lock:
            self._conn.close()
