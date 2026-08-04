"""
Covers "unify the two databases": one shared config source
(GCON_DATA_DIR / --data-dir) for both gcon.persistence.db and
gcon.storage.database, and storage/database.py brought up to the same
versioned-migration pattern as persistence/db.py, without losing data
already in an existing gcon.db (created by the old
`executescript(SCHEMA)` path).
"""

import os
import sqlite3

import pytest

from gcon.config import resolve_control_plane_db_path, resolve_legacy_db_path
from gcon.persistence.db import ControlPlaneDatabase
from gcon.storage.database import Database


# --------------------------------------------------------------- config

def test_defaults_unchanged_when_nothing_is_set(monkeypatch):
    monkeypatch.delenv("GCON_DATA_DIR", raising=False)
    monkeypatch.delenv("GCON_CONTROL_PLANE_DB_PATH", raising=False)
    monkeypatch.delenv("GCON_DB_PATH", raising=False)

    assert resolve_control_plane_db_path() == os.path.join("data", "gcon_control_plane.db")
    assert resolve_legacy_db_path() == os.path.join("data", "gcon.db")


def test_shared_data_dir_moves_both_defaults(monkeypatch):
    monkeypatch.delenv("GCON_CONTROL_PLANE_DB_PATH", raising=False)
    monkeypatch.delenv("GCON_DB_PATH", raising=False)
    monkeypatch.setenv("GCON_DATA_DIR", "/var/lib/gcon")

    assert resolve_control_plane_db_path() == "/var/lib/gcon/gcon_control_plane.db"
    assert resolve_legacy_db_path() == "/var/lib/gcon/gcon.db"


def test_explicit_path_always_wins(monkeypatch):
    monkeypatch.setenv("GCON_DATA_DIR", "/var/lib/gcon")
    monkeypatch.setenv("GCON_CONTROL_PLANE_DB_PATH", "/other/cp.db")
    monkeypatch.setenv("GCON_DB_PATH", "/other/legacy.db")

    assert resolve_control_plane_db_path("/explicit/cp.db") == "/explicit/cp.db"
    assert resolve_legacy_db_path("/explicit/legacy.db") == "/explicit/legacy.db"


def test_specific_env_var_wins_over_shared_data_dir(monkeypatch):
    monkeypatch.setenv("GCON_DATA_DIR", "/var/lib/gcon")
    monkeypatch.setenv("GCON_CONTROL_PLANE_DB_PATH", "/other/cp.db")
    monkeypatch.setenv("GCON_DB_PATH", "/other/legacy.db")

    assert resolve_control_plane_db_path() == "/other/cp.db"
    assert resolve_legacy_db_path() == "/other/legacy.db"


def test_both_stores_land_in_the_same_directory_end_to_end(tmp_path, monkeypatch):
    monkeypatch.delenv("GCON_CONTROL_PLANE_DB_PATH", raising=False)
    monkeypatch.delenv("GCON_DB_PATH", raising=False)
    monkeypatch.setenv("GCON_DATA_DIR", str(tmp_path))

    control_plane_db = ControlPlaneDatabase()
    legacy_db = Database()
    try:
        assert os.path.dirname(control_plane_db.path) == str(tmp_path)
        assert os.path.dirname(legacy_db.path) == str(tmp_path)
        assert os.path.exists(control_plane_db.path)
        assert os.path.exists(legacy_db.path)
    finally:
        control_plane_db.close()


# --------------------------------------------------------------- migrations

def test_database_migrations_are_recorded():
    db = Database(":memory:")
    applied = db.applied_migrations()
    assert [m["version"] for m in applied] == [1, 2]
    assert applied[0]["name"] == "initial_identity_schema"
    assert applied[1]["name"] == "sessions_and_login_rate_limit"


def test_database_migrations_are_idempotent_across_restarts(tmp_path):
    path = str(tmp_path / "gcon.db")
    db1 = Database(path)
    assert len(db1.applied_migrations()) == 2

    # Reopening must not re-run / fail on already-applied migrations.
    db2 = Database(path)
    assert len(db2.applied_migrations()) == 2


def test_migration_2_tables_exist():
    db = Database(":memory:")
    tables = {row["name"] for row in db.query("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"sessions", "login_attempts", "login_lockouts"}.issubset(tables)


def test_upgrading_a_pre_migration_database_does_not_lose_data(tmp_path):
    """
    Simulates a real deployment's existing gcon.db, created by the old
    `_conn.executescript(SCHEMA)` path (no schema_migrations table,
    just the bare tables) with real data in it, then opens it with the
    new migration-based Database and confirms the data is intact and
    migration 1 is simply recorded as a no-op.
    """
    path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE users (
            user_id TEXT PRIMARY KEY, name TEXT, email TEXT, role TEXT,
            organization_id TEXT, status TEXT, avatar_initials TEXT,
            created_at TEXT, last_active TEXT, password_hash TEXT,
            stats_json TEXT
        );
        """
    )
    conn.execute(
        "INSERT INTO users (user_id, name, email, role, status, created_at, "
        "last_active, stats_json) VALUES ('u1','Old User','old@example.com',"
        "'owner','active','2025-01-01','2025-01-01','{}')"
    )
    conn.commit()
    conn.close()

    db = Database(path)
    row = db.query_one("SELECT * FROM users WHERE user_id = 'u1'")
    assert row is not None
    assert row["name"] == "Old User"

    applied = {m["version"] for m in db.applied_migrations()}
    assert applied == {1, 2}