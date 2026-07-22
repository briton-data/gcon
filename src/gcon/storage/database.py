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
  - Login sessions (auth.py SessionManager): NOT persisted, on
    purpose. Sessions already expire after 24h; losing them on a
    restart just means an active user logs in again, which is normal
    behavior for most web apps and not a data-loss concern.
  - Node registry / running jobs / workflow state: NOT persisted
    here either. That's live, ephemeral cluster state tied to
    processes that are, themselves, not currently durable across a
    restart (agents reconnect and re-register). Persisting half of a
    distributed system's runtime state without the other half (real
    node processes surviving a coordinator restart) would create the
    illusion of durability without the substance. This is scoped to
    the identity/security data that a restart should never be allowed
    to destroy.

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


DEFAULT_DB_PATH = os.environ.get("GCON_DB_PATH", "data/gcon.db")

SCHEMA = """
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
);
CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);

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
);
CREATE INDEX IF NOT EXISTS idx_api_keys_secret ON api_keys (secret);

CREATE TABLE IF NOT EXISTS organizations (
    org_id          TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    plan            TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    storage_used_gb REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS teams (
    team_id       TEXT PRIMARY KEY,
    org_id        TEXT NOT NULL,
    name          TEXT NOT NULL,
    admin_user_id TEXT,
    member_ids_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_teams_org ON teams (org_id);

CREATE TABLE IF NOT EXISTS audit_log (
    entry_id  TEXT PRIMARY KEY,
    actor     TEXT NOT NULL,
    action    TEXT NOT NULL,
    target    TEXT,
    timestamp TEXT NOT NULL,
    seq       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_audit_seq ON audit_log (seq);

CREATE TABLE IF NOT EXISTS notifications (
    notification_id TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    message         TEXT NOT NULL,
    severity        TEXT NOT NULL,
    category        TEXT NOT NULL,
    timestamp       TEXT NOT NULL,
    read            INTEGER NOT NULL DEFAULT 0,
    seq             INTEGER
);
CREATE INDEX IF NOT EXISTS idx_notif_seq ON notifications (seq);
"""


class Database:
    """
    Thin wrapper around one SQLite connection, shared by every
    management-layer registry (users, API keys, organizations,
    audit log, notifications). One Database == one .db file == one
    GCON deployment's durable state.
    """

    def __init__(self, path=None):
        self.path = path or DEFAULT_DB_PATH
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
        self._conn.executescript(SCHEMA)
        self._conn.commit()
        self._seq_counters = {"audit_log": self._max_seq("audit_log"), "notifications": self._max_seq("notifications")}

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
