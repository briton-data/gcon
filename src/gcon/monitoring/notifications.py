"""
GCON Notifications — platform-level event notifications.

Every notification carries two independent, live-computed
dimensions:

* severity  — how urgent it is (critical / warning / information / security)
* category  — what part of the system it's about (security /
  receipt_verification / cluster_events / account)

Both are derived from the notification `type_` via the maps below,
never chosen ad hoc at the call site, so the same event type always
renders the same way everywhere in the UI.

Persistence: every notification is written through to the shared
`Database`, and the most recent `max_entries` are loaded back on
construction. The append + trim sequence (and mark_read/mark_all_read)
now happen under the database's write lock, closing the same
lost-update race fixed in audit_log.py.
"""

from datetime import datetime, UTC
from uuid import uuid4

from ..storage.database import Database

NOTIFICATION_TYPES = [
    "user_registered", "invitation_accepted", "password_changed",
    "api_key_created", "workflow_completed", "node_failure",
    "node_registered", "job_failed", "receipt_generated",
    "receipt_verification_failed", "receipt_verification_recovered",
    "health_degraded", "health_critical", "health_recovered",
    "storage_warning",
]

SEVERITY_LEVELS = ["critical", "warning", "information", "security"]

# type -> severity
TYPE_SEVERITY = {
    "user_registered": "information",
    "invitation_accepted": "information",
    "password_changed": "security",
    "api_key_created": "security",
    "workflow_completed": "information",
    "node_failure": "critical",
    "node_registered": "information",
    "job_failed": "warning",
    "receipt_generated": "information",
    "receipt_verification_failed": "critical",
    "receipt_verification_recovered": "information",
    "health_degraded": "warning",
    "health_critical": "critical",
    "health_recovered": "information",
    "storage_warning": "warning",
}

# type -> category
TYPE_CATEGORY = {
    "user_registered": "account",
    "invitation_accepted": "account",
    "password_changed": "security",
    "api_key_created": "security",
    "workflow_completed": "cluster_events",
    "node_failure": "cluster_events",
    "node_registered": "cluster_events",
    "job_failed": "cluster_events",
    "receipt_generated": "receipt_verification",
    "receipt_verification_failed": "receipt_verification",
    "receipt_verification_recovered": "receipt_verification",
    "health_degraded": "cluster_events",
    "health_critical": "cluster_events",
    "health_recovered": "cluster_events",
    "storage_warning": "cluster_events",
}


class NotificationCenter:
    def __init__(self, max_entries=200, db: Database = None):
        self.db = db or Database(":memory:")
        self.max_entries = max_entries
        rows = self.db.query(
            "SELECT * FROM notifications ORDER BY seq DESC LIMIT ?", (max_entries,)
        )
        self.entries = [self._row_to_entry(r) for r in reversed(rows)]

    @staticmethod
    def _row_to_entry(row):
        return {
            "notification_id": row["notification_id"],
            "type": row["type"],
            "message": row["message"],
            "severity": row["severity"],
            "category": row["category"],
            "timestamp": row["timestamp"],
            "read": bool(row["read"]),
        }

    def notify(self, type_, message, severity=None, category=None):
        with self.db.transaction() as conn:
            seq = self.db.next_seq("notifications")
            entry = {
                "notification_id": f"notif_{uuid4().hex[:8]}",
                "type": type_,
                "message": message,
                "severity": severity or TYPE_SEVERITY.get(type_, "information"),
                "category": category or TYPE_CATEGORY.get(type_, "cluster_events"),
                "timestamp": datetime.now(UTC).isoformat(),
                "read": False,
            }
            conn.execute(
                "INSERT INTO notifications (notification_id, type, message, severity, "
                "category, timestamp, read, seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (entry["notification_id"], entry["type"], entry["message"],
                 entry["severity"], entry["category"], entry["timestamp"], 0, seq),
            )
            self.entries.append(entry)
            if len(self.entries) > self.max_entries:
                self.entries = self.entries[-self.max_entries:]
                cutoff_seq = seq - self.max_entries + 1
                conn.execute("DELETE FROM notifications WHERE seq < ?", (cutoff_seq,))
        return entry

    def mark_read(self, notification_id):
        with self.db.transaction() as conn:
            for entry in self.entries:
                if entry["notification_id"] == notification_id:
                    entry["read"] = True
                    conn.execute(
                        "UPDATE notifications SET read = 1 WHERE notification_id = ?",
                        (notification_id,),
                    )
                    return entry
        raise ValueError(f"Notification '{notification_id}' does not exist.")

    def mark_all_read(self):
        with self.db.transaction() as conn:
            for entry in self.entries:
                entry["read"] = True
            conn.execute("UPDATE notifications SET read = 1")
        return self.entries

    def list_entries(self, limit=50):
        return list(reversed(self.entries[-limit:]))

    def unread_count(self):
        return sum(1 for e in self.entries if not e["read"])

    def unread_count_by_severity(self):
        """
        Live breakdown of unread notifications by severity, for the
        navbar bell badge and the Notifications page filters.
        """
        counts = {level: 0 for level in SEVERITY_LEVELS}
        for e in self.entries:
            if not e["read"]:
                counts[e.get("severity", "information")] = (
                    counts.get(e.get("severity", "information"), 0) + 1
                )
        return counts
