import os
import sqlite3
import tempfile

import pytest

from gcon.persistence.control_plane import ControlPlane
from gcon.persistence.db import ControlPlaneDatabase


@pytest.fixture
def cp():
    plane = ControlPlane(path=":memory:")
    yield plane
    plane.close()


# --------------------------------------------------------------- migrations
def test_migrations_apply_and_are_recorded(cp):
    applied = cp.db.applied_migrations()
    assert len(applied) >= 1
    assert applied[0]["name"] == "initial_control_plane_schema"


def test_migrations_are_idempotent(tmp_path):
    path = str(tmp_path / "cp.db")
    plane1 = ControlPlane(path=path)
    plane1.close()
    # Reopening must not re-run / fail on already-applied migrations.
    plane2 = ControlPlane(path=path)
    assert len(plane2.db.applied_migrations()) >= 1
    plane2.close()


def test_all_required_tables_exist(cp):
    tables = {
        row["name"]
        for row in cp.db.query(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    required = {
        "nodes", "jobs", "job_attempts", "receipts", "heartbeats",
        "cluster_events", "execution_logs", "settings", "node_capabilities",
    }
    assert required.issubset(tables)


def test_foreign_keys_enforced(cp):
    with pytest.raises(sqlite3.IntegrityError):
        cp.db.execute(
            "INSERT INTO job_attempts (attempt_id, job_id, node_id, attempt_number, "
            "status, request_message_id, dispatched_at) VALUES "
            "('a1', 'nonexistent-job', NULL, 1, 'dispatched', 'r1', '2026-01-01T00:00:00')"
        )


def test_crash_safety_wal_and_foreign_keys_pragmas(tmp_path):
    path = str(tmp_path / "cp.db")
    plane = ControlPlane(path=path)
    mode = plane.db.query_one("PRAGMA journal_mode")[0]
    fk = plane.db.query_one("PRAGMA foreign_keys")[0]
    assert mode.lower() == "wal"
    assert fk == 1
    plane.close()


# ----------------------------------------------------------------- nodes
def test_node_upsert_is_idempotent_registration(cp):
    cp.nodes.upsert("node-1", "host-a", status="idle", auth_fingerprint="fp-1")
    cp.nodes.upsert("node-1", "host-a", status="busy", auth_fingerprint="fp-1")
    rows = cp.nodes.list_all()
    assert len(rows) == 1
    assert rows[0]["status"] == "busy"


def test_node_auth_fingerprint_unique(cp):
    cp.nodes.upsert("node-1", "host-a", auth_fingerprint="fp-shared")
    with pytest.raises(sqlite3.IntegrityError):
        cp.db.execute(
            "INSERT INTO nodes (node_id, hostname, status, registered_at, last_seen_at, "
            "draining, auth_fingerprint) VALUES "
            "('node-2', 'host-b', 'idle', '2026-01-01T00:00:00', '2026-01-01T00:00:00', 0, 'fp-shared')"
        )


def test_node_capabilities_upsert(cp):
    cp.nodes.upsert("node-1", "host-a")
    cp.node_capabilities.set_capabilities("node-1", {"gpu": "A100", "cpu_cores": "32"})
    cp.node_capabilities.set_capability("node-1", "gpu", "H100")
    caps = cp.node_capabilities.get_capabilities("node-1")
    assert caps == {"gpu": "H100", "cpu_cores": "32"}


# ------------------------------------------------------------- job_attempts
def test_job_attempt_idempotent_on_duplicate_request_id(cp):
    cp.jobs.create("job-1", "echo hi")
    cp.nodes.upsert("node-1", "host-a")
    a1 = cp.job_attempts.record_attempt("job-1", "node-1", "req-abc")
    a2 = cp.job_attempts.record_attempt("job-1", "node-1", "req-abc")
    assert a1["attempt_id"] == a2["attempt_id"]
    assert len(cp.job_attempts.list_for_job("job-1")) == 1


def test_job_attempt_numbers_increment_per_job(cp):
    cp.jobs.create("job-1", "echo hi")
    cp.nodes.upsert("node-1", "host-a")
    a1 = cp.job_attempts.record_attempt("job-1", "node-1", "req-1")
    a2 = cp.job_attempts.record_attempt("job-1", "node-1", "req-2")
    assert a1["attempt_number"] == 1
    assert a2["attempt_number"] == 2


def test_jobs_ensure_exists_is_idempotent(cp):
    cp.jobs.ensure_exists("job-x", "echo hi")
    cp.jobs.ensure_exists("job-x", "echo hi")
    assert cp.jobs.get("job-x") is not None
    assert len(cp.jobs.list_all()) == 1


# --------------------------------------------------------------- receipts
def test_receipt_upload_idempotent_on_hash(cp):
    cp.jobs.create("job-1", "echo hi")
    r1 = cp.receipts.upload("job-1", {"a": 1}, "hash-1")
    r2 = cp.receipts.upload("job-1", {"a": 1}, "hash-1")
    assert r1["receipt_id"] == r2["receipt_id"]
    assert len(cp.receipts.list_for_job("job-1")) == 1


# -------------------------------------------------------------- heartbeats
def test_heartbeat_dedup_on_sequence(cp):
    cp.nodes.upsert("node-1", "host-a")
    assert cp.heartbeats.record("node-1", 1, "idle") is True
    assert cp.heartbeats.record("node-1", 1, "idle") is False  # duplicate delivery
    assert cp.heartbeats.record("node-1", 2, "idle") is True
    assert cp.heartbeats.last_sequence("node-1") == 2


# ---------------------------------------------------------- execution_logs
def test_execution_log_dedup_on_sequence(cp):
    cp.jobs.create("job-1", "echo hi")
    cp.nodes.upsert("node-1", "host-a")
    attempt = cp.job_attempts.record_attempt("job-1", "node-1", "req-1")
    attempt_id = attempt["attempt_id"]

    assert cp.execution_logs.append("job-1", attempt_id, "node-1", "stdout", 1, "line one") is True
    assert cp.execution_logs.append("job-1", attempt_id, "node-1", "stdout", 1, "line one") is False
    assert len(cp.execution_logs.for_job("job-1")) == 1


def test_execution_log_null_attempt_id_is_not_deduplicated(cp):
    """
    SQL UNIQUE constraints treat NULL as distinct from every other
    NULL, so `(attempt_id, stream, sequence)` only deduplicates once a
    real attempt_id is attached -- documented here so this isn't
    mistaken for a bug later. Production log writers always have an
    attempt_id by the time they call this.
    """
    cp.jobs.create("job-1", "echo hi")
    cp.execution_logs.append("job-1", None, None, "stdout", 1, "line one")
    cp.execution_logs.append("job-1", None, None, "stdout", 1, "line one")
    assert len(cp.execution_logs.for_job("job-1")) == 2


# ------------------------------------------------------------- settings
def test_settings_roundtrip_and_update(cp):
    assert cp.settings.get("heartbeat_interval_seconds") is None
    cp.settings.set("heartbeat_interval_seconds", "10")
    assert cp.settings.get("heartbeat_interval_seconds") == "10"
    cp.settings.set("heartbeat_interval_seconds", "20")
    assert cp.settings.get("heartbeat_interval_seconds") == "20"


# ------------------------------------------------------------ thread safety
def test_concurrent_heartbeats_thread_safe(cp):
    import threading

    cp.nodes.upsert("node-1", "host-a")
    errors = []

    def record(seq):
        try:
            cp.heartbeats.record("node-1", seq, "idle")
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=record, args=(i,)) for i in range(1, 51)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert cp.heartbeats.last_sequence("node-1") == 50


def test_cluster_events_recorded(cp):
    cp.nodes.upsert("node-1", "host-a")
    cp.cluster_events.record("NODE_REGISTERED", node_id="node-1", payload={"hostname": "host-a"})
    events = cp.cluster_events.recent()
    assert events[0]["event_type"] == "NODE_REGISTERED"
    assert events[0]["payload"]["hostname"] == "host-a"
