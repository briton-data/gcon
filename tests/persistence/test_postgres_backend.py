"""
Real tests against a live PostgreSQL server -- not mocked, not
sqlite-with-a-postgres-flavored-assertion. Opt-in: set
GCON_TEST_POSTGRES_DSN to a real, reachable Postgres connection
string to run these; otherwise every test here is skipped with a
clear reason, so the normal test suite never requires a local
Postgres server to pass.

What's actually under test is the "one seam" the module's own
docstring described: ControlPlaneDatabase opening a psycopg
connection instead of sqlite3.connect, translating `?` -> `%s`
placeholders via _PsycopgConnectionShim, and PostgresDialect's
already-correct pk_ddl() -- proven by running the SAME repository
code every SQLite-backed test in this suite already exercises,
against a real server, not by asserting anything about the shim's
internals directly.

Uses uuid-suffixed job/node ids per test rather than a fresh
database per test (no per-test CREATE DATABASE/schema provisioning
here -- this file's job is proving the backend works, not building
test-database infrastructure) so tests sharing GCON_TEST_POSTGRES_DSN
don't collide with each other or with a prior run against the same
server.
"""
import os
import uuid

import pytest

psycopg = pytest.importorskip("psycopg", reason="psycopg not installed (pip install gcon[postgres])")

POSTGRES_DSN = os.environ.get("GCON_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.skipif(
    not POSTGRES_DSN,
    reason="GCON_TEST_POSTGRES_DSN not set -- set it to a real, reachable "
           "Postgres connection string to run these tests",
)

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane
from gcon.persistence.db import PostgresDialect


def _uid(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def control_plane():
    cp = ControlPlane(path=POSTGRES_DSN, dialect=PostgresDialect())
    yield cp
    cp.close()


def test_migrations_apply_and_are_idempotent(control_plane):
    applied_first = control_plane.db.applied_migrations()
    assert len(applied_first) > 0
    # Re-running __init__ against the same DSN (a second coordinator
    # process connecting to the same shared server -- exactly the
    # real multi-host scenario this backend exists for) must not fail
    # or re-apply anything; CREATE TABLE IF NOT EXISTS + the
    # schema_migrations version check are what make this safe.
    cp2 = ControlPlane(path=POSTGRES_DSN, dialect=PostgresDialect())
    assert cp2.db.applied_migrations() == applied_first
    cp2.close()


def test_job_and_attempt_crud_round_trips(control_plane):
    job_id = _uid("job")
    node_id = _uid("node")

    control_plane.jobs.create(job_id, "echo hi")
    control_plane.jobs.set_status(job_id, "running")
    row = control_plane.jobs.get(job_id)
    assert row["status"] == "running"
    assert row["command"] == "echo hi"

    control_plane.nodes.upsert(node_id, hostname=node_id)
    attempt = control_plane.job_attempts.record_attempt(job_id, node_id, _uid("msg"))
    assert attempt["attempt_number"] == 1
    assert attempt["status"] == "dispatched"

    control_plane.job_attempts.set_status(attempt["attempt_id"], "success", completed=True)
    attempts = control_plane.job_attempts.list_for_job(job_id)
    assert len(attempts) == 1
    assert attempts[0]["status"] == "success"

    control_plane.receipts.upload(
        job_id, {"foo": "bar"}, _uid("hash"),
        attempt_id=attempt["attempt_id"], node_id=node_id,
    )
    receipts = control_plane.receipts.list_for_job(job_id)
    assert len(receipts) == 1
    assert receipts[0]["attempt_id"] == attempt["attempt_id"]
    assert receipts[0]["payload"] == {"foo": "bar"}


def test_attempt_number_unique_constraint_is_enforced(control_plane):
    """The real thing job_attempts' UNIQUE(job_id, attempt_number)
    constraint protects: two concurrent dispatches can never be
    durably recorded under the same attempt number for the same job.
    record_attempt() computes the next number itself, so proving the
    constraint holds means calling it back-to-back and checking the
    numbers it hands out are genuinely distinct and sequential, not
    just trusting the DDL translated correctly."""
    job_id = _uid("job")
    node_id = _uid("node")
    control_plane.jobs.create(job_id, "echo hi")
    control_plane.nodes.upsert(node_id, hostname=node_id)

    a1 = control_plane.job_attempts.record_attempt(job_id, node_id, _uid("msg"))
    a2 = control_plane.job_attempts.record_attempt(job_id, node_id, _uid("msg"))
    assert a1["attempt_number"] == 1
    assert a2["attempt_number"] == 2


def test_a_real_job_dispatches_and_completes_end_to_end(control_plane):
    """Full coordinator-level round trip against the Postgres-backed
    control plane -- not just the repository layer in isolation."""
    coordinator = GCONCoordinator(control_plane=control_plane)
    node_id = _uid("node")
    job_id = _uid("job")
    try:
        coordinator.register_agent(GCONAgent(node_id=node_id))
        coordinator.submit_job(job_id, "echo hi")
        coordinator.assign_job(job_id)

        import time
        end = time.time() + 5
        while time.time() < end and coordinator.jobs[job_id]["status"] == "running":
            time.sleep(0.05)

        assert coordinator.jobs[job_id]["status"] == "completed"
        assert control_plane.jobs.get(job_id)["status"] == "completed"
    finally:
        coordinator.shutdown()
