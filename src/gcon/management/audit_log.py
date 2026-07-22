"""
GCON Audit Log — records important management actions.

Persistence: every entry is written through to the shared
`Database`, and the most recent `max_entries` are loaded back into
memory on construction, so the audit trail survives a restart
instead of starting empty every time.

The append + trim sequence now happens inside Database.transaction(),
closing the lost-update race stress_test2.py found in the pure
in-memory version: `self.entries.append(...)` followed by
`self.entries = self.entries[-max_entries:]` was two separate,
unguarded steps, so a concurrent log() landing between them could be
silently dropped. A monotonic `seq` column (rather than trusting
insertion order) makes "the most recent N" well-defined even when
writes arrive concurrently.
"""

from datetime import datetime, UTC
from uuid import uuid4

from ..storage.database import Database


class AuditLogger:
    def __init__(self, max_entries=500, db: Database = None):
        self.db = db or Database(":memory:")
        self.max_entries = max_entries
        rows = self.db.query(
            "SELECT * FROM audit_log ORDER BY seq DESC LIMIT ?", (max_entries,)
        )
        self.entries = [self._row_to_entry(r) for r in reversed(rows)]

    @staticmethod
    def _row_to_entry(row):
        return {
            "entry_id": row["entry_id"],
            "actor": row["actor"],
            "action": row["action"],
            "target": row["target"],
            "timestamp": row["timestamp"],
        }

    def log(self, actor, action, target=None):
        with self.db.transaction() as conn:
            seq = self.db.next_seq("audit_log")
            entry = {
                "entry_id": f"audit_{uuid4().hex[:8]}",
                "actor": actor,
                "action": action,
                "target": target,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            conn.execute(
                "INSERT INTO audit_log (entry_id, actor, action, target, timestamp, seq) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (entry["entry_id"], actor, action, target, entry["timestamp"], seq),
            )
            self.entries.append(entry)
            if len(self.entries) > self.max_entries:
                self.entries = self.entries[-self.max_entries:]
            # Keep the on-disk table capped too, so it doesn't grow
            # unbounded across a long-lived process — delete rows
            # older than the oldest one we're still keeping in memory.
            if self.entries:
                cutoff_seq = seq - self.max_entries + 1
                conn.execute("DELETE FROM audit_log WHERE seq < ?", (cutoff_seq,))
        return entry

    def list_entries(self, limit=100):
        return list(reversed(self.entries[-limit:]))
