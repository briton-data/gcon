"""
Two coordinators sharing one database must never both hold the leader lease.

Each coordinator has its OWN connection, so the tests give each side its own ControlPlane object on the same
database, and release both at the same instant. Before the compare-and-swap, both won most rounds of the
expired-lease race on PostgreSQL (and could on SQLite across processes).
"""
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, UTC

import pytest

from gcon.persistence.control_plane import ControlPlane

ROUNDS = 80
EXPIRED = (datetime.now(UTC) - timedelta(days=1)).isoformat()


def race(make_plane, lease_name, rounds, expire_each_round):
    a_plane, b_plane = make_plane(), make_plane()
    admin = make_plane()
    results = []
    for r in range(rounds):
        if expire_each_round:
            admin.db.execute("DELETE FROM coordinator_leases WHERE lease_name = ?", (lease_name,))
            admin.db.execute("INSERT INTO coordinator_leases (lease_name, holder_id, term, acquired_at, expires_at, updated_at) "
                             "VALUES (?, 'dead', 1, ?, ?, ?)", (lease_name, EXPIRED, EXPIRED, EXPIRED))
        else:
            admin.db.execute("DELETE FROM coordinator_leases WHERE lease_name = ?", (lease_name,))   # empty table
        barrier, got, errors = threading.Barrier(2), {}, []

        def go(name, plane):
            barrier.wait()
            try:
                got[name] = plane.leases.try_acquire_or_renew(lease_name, name, 30.0) is not None
            except Exception as e:                                   # a crash here is a failure too
                errors.append(repr(e))
        threads = [threading.Thread(target=go, args=(n, p)) for n, p in (("A", a_plane), ("B", b_plane))]
        [t.start() for t in threads]; [t.join() for t in threads]
        assert not errors, errors
        results.append(sum(got.values()))
    return results


class TestLeaseSemantics:
    def test_acquire_renew_contend_take_over(self, tmp_path):
        cp = ControlPlane(path=str(tmp_path / "l.db"))
        first = cp.leases.try_acquire_or_renew("L", "A", 30)
        assert first["term"] == 1
        assert cp.leases.try_acquire_or_renew("L", "B", 30) is None                  # live lease held by A
        assert cp.leases.try_acquire_or_renew("L", "A", 30)["term"] == 1             # A renews, same term
        cp.db.execute("UPDATE coordinator_leases SET expires_at = ? WHERE lease_name = 'L'", (EXPIRED,))
        taken = cp.leases.try_acquire_or_renew("L", "B", 30)
        assert taken["holder_id"] == "B" and taken["term"] == 2                       # takeover bumps the term
        assert cp.leases.try_acquire_or_renew("L", "A", 30) is None                  # the old holder is out
        assert cp.leases.release("L", "A") is False and cp.leases.release("L", "B") is True


class TestNeverTwoLeaders:
    def test_racing_for_an_expired_lease(self, tmp_path):
        path = str(tmp_path / "race.db")
        results = race(lambda: ControlPlane(path=path), "L", ROUNDS, expire_each_round=True)
        assert results == [1] * ROUNDS

    def test_racing_to_create_the_lease_on_an_empty_table(self, tmp_path):
        path = str(tmp_path / "first.db")
        results = race(lambda: ControlPlane(path=path), "L", ROUNDS, expire_each_round=False)
        assert results == [1] * ROUNDS                      # exactly one winner, and nobody crashed


POSTGRES_DSN = os.environ.get("GCON_TEST_POSTGRES_DSN")


@pytest.mark.skipif(not POSTGRES_DSN, reason="GCON_TEST_POSTGRES_DSN not set")
class TestNeverTwoLeadersOnPostgres:
    def _plane(self):
        from gcon.persistence.db import PostgresDialect
        return ControlPlane(dialect=PostgresDialect(), path=POSTGRES_DSN)

    def test_racing_for_an_expired_lease(self):
        name = f"race-{uuid.uuid4().hex[:8]}"
        assert race(self._plane, name, ROUNDS, expire_each_round=True) == [1] * ROUNDS

    def test_racing_to_create_the_lease_on_an_empty_table(self):
        name = f"first-{uuid.uuid4().hex[:8]}"
        assert race(self._plane, name, ROUNDS, expire_each_round=False) == [1] * ROUNDS
