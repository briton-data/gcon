from __future__ import annotations

from datetime import datetime, UTC
from typing import Any, Dict, Optional

from gcon.persistence.db import ControlPlaneDatabase


class LeaseRepository:
    """
    Single-row-per-lease-name compare-and-swap store, backing
    `gcon.cluster.leader_election.LeaderElector`. Every write here
    goes through `ControlPlaneDatabase.transaction()`, which holds
    both the process-level `threading.RLock` and SQLite's own write
    lock for the duration -- so two coordinator processes racing to
    acquire the same lease can never both succeed, even though they
    are different OS processes sharing only the database file (SQLite
    serializes writers at the file level; the read-modify-write below
    happens inside one such serialized write).
    """

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def read(self, lease_name: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT * FROM coordinator_leases WHERE lease_name = ?", (lease_name,)
        )
        return dict(row) if row else None

    def try_acquire_or_renew(
        self, lease_name: str, holder_id: str, ttl_seconds: float
    ) -> Optional[Dict[str, Any]]:
        """
        Returns the lease row if `holder_id` now holds (or continues
        to hold) the lease, or None if someone else holds an
        unexpired lease or won the race for it.

        Safe against another coordinator on ANOTHER connection or
        process (the database may be shared): the read happens before
        the write, so the write is a compare-and-swap. It only takes
        effect if the row is still exactly what was read (same holder,
        term and expiry); when two coordinators race for an expired
        lease, the database lets exactly one UPDATE match, and the
        other gets None. The first-ever acquisition uses
        INSERT ... ON CONFLICT DO NOTHING for the same reason.
        """
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        expires_at = now.timestamp() + ttl_seconds
        from datetime import timedelta
        expires_iso = (now + timedelta(seconds=ttl_seconds)).isoformat()

        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM coordinator_leases WHERE lease_name = ?", (lease_name,)
            ).fetchone()

            if row is None:
                created = conn.execute(
                    """
                    INSERT INTO coordinator_leases
                        (lease_name, holder_id, term, acquired_at, expires_at, updated_at)
                    VALUES (?, ?, 1, ?, ?, ?)
                    ON CONFLICT (lease_name) DO NOTHING
                    """,
                    (lease_name, holder_id, now_iso, expires_iso, now_iso),
                )
                if created.rowcount != 1:
                    return None  # another coordinator created it first
                return {
                    "lease_name": lease_name, "holder_id": holder_id, "term": 1,
                    "acquired_at": now_iso, "expires_at": expires_iso, "updated_at": now_iso,
                }

            row = dict(row)
            is_current_holder = row["holder_id"] == holder_id
            is_expired = row["expires_at"] <= now_iso

            if not is_current_holder and not is_expired:
                return None  # someone else holds a live lease

            new_term = row["term"] + (0 if is_current_holder else 1)
            acquired_at = row["acquired_at"] if is_current_holder else now_iso
            swapped = conn.execute(
                """
                UPDATE coordinator_leases
                SET holder_id = ?, term = ?, acquired_at = ?, expires_at = ?, updated_at = ?
                WHERE lease_name = ? AND holder_id = ? AND term = ? AND expires_at = ?
                """,
                (holder_id, new_term, acquired_at, expires_iso, now_iso,
                 lease_name, row["holder_id"], row["term"], row["expires_at"]),
            )
            if swapped.rowcount != 1:
                return None  # the lease changed hands between the read and the write
            return {
                "lease_name": lease_name, "holder_id": holder_id, "term": new_term,
                "acquired_at": acquired_at, "expires_at": expires_iso, "updated_at": now_iso,
            }

    def release(self, lease_name: str, holder_id: str) -> bool:
        """Voluntary early release (graceful shutdown) -- only the
        current holder can release its own lease. Returns True if a
        row was actually cleared."""
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT holder_id FROM coordinator_leases WHERE lease_name = ?", (lease_name,)
            ).fetchone()
            if row is None or row["holder_id"] != holder_id:
                return False
            conn.execute(
                "DELETE FROM coordinator_leases WHERE lease_name = ? AND holder_id = ?", (lease_name, holder_id)
            )
            return True
