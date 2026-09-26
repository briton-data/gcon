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
    `gcon.storage.database`.
  * Booleans are stored as INTEGER 0/1.
  * No SQLite-only functions (`json_extract`, `printf`, ...) appear
    in any query.
  * Placeholders use `?` (SQLite's paramstyle).

Crash safety
------------
Same guarantees as `gcon.storage.database.Database`: WAL journal
mode, synchronous=FULL, foreign_keys=ON, a single `threading.RLock`
serializing write sequences at the Python level, and all multi-row
mutations going through `transaction()`.
"""

from __future__ import annotations

import os
import sqlite3
import threading

# Portable "this violated a UNIQUE/PK/FK constraint" exception type,
# for repositories that intentionally race an INSERT against a
# concurrent duplicate and need to catch just that (see
# JobAttemptRepository.record_attempt's docstring for why: losing
# that race is expected and handled, not a real error). sqlite3 and
# psycopg raise different, unrelated exception classes for the same
# underlying condition; repositories that only ever caught
# sqlite3.IntegrityError directly would silently stop catching this
# at all the moment a coordinator is Postgres-backed (see
# PostgresDialect), letting a real, expected race propagate as an
# unhandled error instead of falling back to "read back what the
# winner wrote". psycopg is an optional dependency (only installed
# for `pip install gcon[postgres]`), so this degrades gracefully to
# just sqlite3.IntegrityError when it isn't present -- exactly the
# set of engines actually in play for that process either way.
try:
    import psycopg.errors as _psycopg_errors
    IntegrityError: "tuple" = (sqlite3.IntegrityError, _psycopg_errors.IntegrityError)
except ImportError:
    IntegrityError: "tuple" = (sqlite3.IntegrityError,)
from contextlib import contextmanager
from dataclasses import dataclass

from gcon.config import resolve_control_plane_db_path
from gcon.persistence.migrations.registry import MIGRATIONS


def _default_control_plane_db_path() -> str:
    """Resolved lazily (see gcon.config) so GCON_DATA_DIR /
    GCON_CONTROL_PLANE_DB_PATH set after import (e.g. by tests) still
    take effect. Kept as a module-level function rather than a frozen
    constant for that reason; existing callers that imported
    DEFAULT_CONTROL_PLANE_DB_PATH directly still get a valid string
    below, just resolved once at import time as before."""
    return resolve_control_plane_db_path()


# Backwards-compatible constant for any existing external callers.
DEFAULT_CONTROL_PLANE_DB_PATH = _default_control_plane_db_path()


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
    Wired to a live driver (psycopg 3, `pip install gcon[postgres]`)
    by ControlPlaneDatabase.__init__ -- see its docstring for how a
    ControlPlaneDatabase is told to use this dialect. This is the
    real, network-shared backend genuine cross-host coordinator HA
    depends on: leader election and job state can only be shared
    across separate machines through a database those machines can
    all actually reach over the network, which a local SQLite file
    never can be.
    """

    def __init__(self):
        object.__setattr__(self, "name", "postgres")

    def pk_ddl(self) -> str:
        return "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"


def render_migration_sql(sql: str, dialect: Dialect) -> str:
    return sql.replace("{{PK}}", dialect.pk_ddl())


class _PsycopgConnectionShim:
    """
    Makes a psycopg3 Connection behave like sqlite3.Connection's
    convenience API: sqlite3.Connection.execute()/.executemany() are
    shortcuts that implicitly open a cursor and hand it back;
    psycopg3's Connection has no such shortcut at all (you get a
    cursor from conn.cursor(), then call .execute() on *that*). Every
    repository file (there are about a dozen) and ControlPlaneDatabase
    itself were written entirely against the sqlite3 shortcut --
    `db.execute(...)`, `with db.transaction() as conn: conn.execute(...)`
    -- since SQLite was the only backend that existed. Rewriting every
    one of those call sites to open its own cursor is exactly the
    "rewrite, not a driver change" this module's docstring already
    said this migration should not be; this shim is the one seam that
    keeps it a driver change: everything above ControlPlaneDatabase
    keeps calling conn.execute(sql, params) exactly as before,
    unaware the connection underneath is psycopg for a Postgres-backed
    coordinator.

    Also handles the other two real SQLite/psycopg differences that
    would otherwise leak into every repository:
      * Placeholders: SQLite uses `?`, psycopg uses `%s`. Translated
        here, once, so no repository's SQL string needs to change.
        Safe as a blind replace in this codebase specifically --
        checked (grep) that no query anywhere embeds a literal `?`
        inside a string constant; every `?` in every query here is a
        bound-parameter placeholder.
      * Row shape: connected with row_factory=dict_row (see
        ControlPlaneDatabase.__init__), so fetchone()/fetchall() hand
        back real dicts -- both `row["col"]` and `dict(row)`, the two
        access patterns already used throughout the repositories
        (written against sqlite3.Row, which supports both), keep
        working unchanged.
    """

    def __init__(self, raw_conn):
        self._raw = raw_conn

    @staticmethod
    def _translate(sql):
        return sql.replace("?", "%s")

    def execute(self, sql, params=()):
        cur = self._raw.cursor()
        cur.execute(self._translate(sql), params)
        return cur

    def executemany(self, sql, seq_of_params):
        cur = self._raw.cursor()
        cur.executemany(self._translate(sql), seq_of_params)
        return cur

    def commit(self):
        self._raw.commit()

    def rollback(self):
        self._raw.rollback()

    def close(self):
        self._raw.close()


class ControlPlaneDatabase:
    """
    One connection, shared by every control-plane repository
    (nodes, jobs, job_attempts, receipts, heartbeats, cluster_events,
    execution_logs, settings, node_capabilities). One
    ControlPlaneDatabase == one control-plane .db file == one GCON
    coordinator's durable cluster state.
    """

    # Exposed here (not just importable from this module) so a
    # repository holding `self.db` can write `except self.db.
    # IntegrityError:` without a separate import -- see the module-
    # level IntegrityError's own comment for why this needs to be
    # dialect-portable rather than sqlite3.IntegrityError directly.
    IntegrityError = IntegrityError

    def __init__(self, path: str | None = None, dialect: Dialect | None = None):
        self.dialect = dialect or SQLiteDialect()
        self._lock = threading.RLock()

        if self.dialect.name == "postgres":
            # `path` is repurposed as a libpq connection string/DSN
            # here (e.g. "postgresql://user:pass@host:5432/dbname")
            # rather than a filesystem path -- see
            # gcon.config.resolve_control_plane_db_dsn, which is what
            # actually decides whether a coordinator uses SQLite or
            # Postgres and supplies this value. Multiple
            # ControlPlaneDatabase instances (on separate hosts, in
            # separate coordinator processes) pointed at the same DSN
            # is the entire point: unlike a local SQLite file, this is
            # the one thing they can all genuinely share over the
            # network, which is what real cross-host leader
            # election/job-state HA depends on.
            if path is None:
                raise ValueError(
                    "PostgresDialect requires `path` to be a libpq "
                    "connection string (e.g. "
                    "'postgresql://user:pass@host:5432/dbname'), not None."
                )
            self.path = path
            try:
                import psycopg
                from psycopg.rows import dict_row
            except ImportError as e:
                raise RuntimeError(
                    "PostgresDialect was selected but the `psycopg` "
                    "driver isn't installed. Install it with "
                    "`pip install gcon[postgres]` (or `pip install "
                    "'psycopg[binary]>=3.1'` directly)."
                ) from e
            raw_conn = psycopg.connect(self.path, autocommit=False, row_factory=dict_row)
            self._conn = _PsycopgConnectionShim(raw_conn)
            # Postgres always enforces foreign keys and has its own
            # durability guarantees (WAL-based, fsync by default) --
            # there's no PRAGMA equivalent to set, and none of the
            # SQLite-specific ones below apply.
        else:
            self.path = resolve_control_plane_db_path(path)
            if self.path != ":memory:":
                directory = os.path.dirname(self.path)
                if directory:
                    os.makedirs(directory, exist_ok=True)
            self._conn = sqlite3.connect(self.path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")

        self._migrate()

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

    @contextmanager
    def transaction(self):
        """
        Serializes a sequence of writes both at the Python level (the
        RLock) and the SQLite level (commits atomically or rolls back
        entirely on error/crash).
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