"""
GCON Database — the persistence layer for platform/identity state.

Backed by SQLite (stdlib `sqlite3`, no new dependency). This is a
deliberate choice for GCON's current single-process architecture,
not a placeholder for "a real database later": SQLite in WAL mode is
fully ACID, crash-safe, and the entire store is one portable file —
appropriate for a single coordinator process. If/when GCON grows a
real multi-machine network transport (today `CommunicationManager`
is in-process method calls — see communication.py), THAT is the
point to migrate this to a client-server database like PostgreSQL,
because that's when there would actually be multiple independent
writers. Doing that migration now, before the thing that requires
it, would just be an ops dependency for its own sake.

What is (and isn't) persisted here, deliberately:
  - Users, API keys, organizations/teams, audit log, and
    notifications: yes. This is exactly the state that was living in
    plain Python dicts/lists and vanishing on every restart (see
    auth.py's own comment: "persistence... the next piece to add").
  - Login sessions (auth.py SessionManager) and login rate-limit /
    lockout state (rate_limit.py LoginRateLimiter): also yes, as of
    the `sessions` / `login_attempts` / `login_lockouts` tables added
    in migration 2 (storage/migrations.py). This reverses an earlier
    documented decision to leave sessions in-memory-only "because
    losing them just means logging in again" -- in practice that also
    means a restart transparently logs out every active user and
    wipes every brute-force lockout, which is worse than the modest
    cost of persisting two small tables. SessionManager and
    LoginRateLimiter both still work purely in-memory (no behavior
    change for existing callers) when constructed without a `db=`.
  - Node registry / running jobs / workflow state: NOT persisted
    here. That's live, ephemeral cluster *scheduling* state tied to
    processes that are, themselves, not currently durable across a
    restart (agents reconnect and re-register). The durable side of
    cluster state (which nodes/jobs/receipts have ever existed) is
    persisted separately, in the control-plane database
    (gcon.persistence.db / gcon.cluster.coordinator.
    GCONCoordinator.restore_from_persistence) -- this module stays
    scoped to identity/security data.

Schema management:
  Uses the same versioned-migration pattern as
  gcon.persistence.db.ControlPlaneDatabase (see storage/migrations.py):
  a `schema_migrations` table tracks which migrations have run, and
  each Migration's SQL runs at most once, in order, inside its own
  transaction. Migration 1 is exactly the schema this module always
  had; it uses `CREATE TABLE IF NOT EXISTS` throughout specifically so
  running it against a database that already has these tables (every
  pre-existing deployment) is a safe no-op rather than a destructive
  operation.

Crash safety:
  - `PRAGMA journal_mode=WAL` — a crash or kill -9 mid-write leaves
    the WAL file intact; SQLite replays/rolls it back cleanly on the
    next open. You never get a half-written, corrupted main database
    file.
  - `PRAGMA synchronous=FULL` — every commit is durable to disk
    before returning, not just to the OS page cache. This is the
    "survive `deploy the whole system, crashed or not`" requirement:
    slower than NORMAL, but nothing acknowledged as committed can be
    lost to a power loss or OOM-kill.
  - All multi-row mutations go through `Database.transaction()`, so a
    crash mid-operation rolls back the whole thing rather than
    leaving partially-written state.
  - A single `threading.RLock` serializes write sequences at the
    Python level too. SQLite already serializes actual disk writes,
    but several call sites here do a compound "read current state,
    then act" sequence (e.g. audit log trimming) that needs to be
    atomic at the application level as well — this is the same class
    of lost-update race stress_test2.py found in the pure in-memory
    version of these classes, and this lock closes it.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, UTC

from gcon.config import resolve_legacy_db_path
from gcon.storage.migrations import MIGRATIONS


DEFAULT_DB_PATH = resolve_legacy_db_path()


class Database:
    """
    Thin wrapper around one SQLite connection, shared by every
    management-layer registry (users, API keys, organizations,
    audit log, notifications). One Database == one .db file == one
    GCON deployment's durable state.
    """

    def __init__(self, path=None):
        self.path = resolve_legacy_db_path(path)
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
        self._seq_counters = {"audit_log": self._max_seq("audit_log"), "notifications": self._max_seq("notifications")}

    def _migrate(self):
        """
        Versioned migration runner, same pattern/guarantees as
        gcon.persistence.db.ControlPlaneDatabase._migrate: each
        Migration in storage.migrations.MIGRATIONS is applied at most
        once (tracked in schema_migrations), in ascending version
        order, each inside its own transaction that rolls back
        cleanly on failure.
        """
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
                        self._conn.execute(statement)
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

    def _max_seq(self, table):
        row = self._conn.execute(f"SELECT MAX(seq) AS m FROM {table}").fetchone()
        return (row["m"] or 0)

    def next_seq(self, table):
        """Monotonic per-table counter, used to keep trim-by-age exact and race-free."""
        with self._lock:
            self._seq_counters[table] += 1
            return self._seq_counters[table]

    @contextmanager
    def transaction(self):
        """
        Serializes a sequence of writes both at the Python level (the
        RLock) and the SQLite level (an explicit transaction that
        commits atomically or rolls back entirely on error/crash).
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

    def query(self, sql, params=()):
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql, params=()):
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def close(self):
        with self._lock:
            self._conn.close()


def dumps(value):
    return json.dumps(value)


def loads(value, default=None):
    if value is None:
        return default
    return json.loads(value)