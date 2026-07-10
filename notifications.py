"""
GCON Notifications — platform-level event notifications.
"""

from datetime import datetime, UTC
from uuid import uuid4

NOTIFICATION_TYPES = [
    "user_registered", "invitation_accepted", "password_changed",
    "api_key_created", "workflow_completed", "node_failure",
    "storage_warning",
]


class NotificationCenter:
    def __init__(self, max_entries=200):
        self.entries = []
        self.max_entries = max_entries

    def notify(self, type_, message):
        entry = {
            "notification_id": f"notif_{uuid4().hex[:8]}",
            "type": type_,
            "message": message,
            "timestamp": datetime.now(UTC).isoformat(),
            "read": False,
        }
        self.entries.append(entry)
        if len(self.entries) > self.max_entries:
            self.entries = self.entries[-self.max_entries:]
        return entry

    def mark_read(self, notification_id):
        for entry in self.entries:
            if entry["notification_id"] == notification_id:
                entry["read"] = True
                return entry
        raise ValueError(f"Notification '{notification_id}' does not exist.")

    def list_entries(self, limit=50):
        return list(reversed(self.entries[-limit:]))

    def unread_count(self):
        return sum(1 for e in self.entries if not e["read"])


def seed_notifications(center):
    """
    Populate the center with illustrative demo notifications.
    """
    center.notify("user_registered", "Ken Osei registered and is pending approval")
    center.notify("api_key_created", "Marcus Webb created API key 'CI/CD Pipeline'")
    center.notify("workflow_completed", "Workflow A completed successfully")
    center.notify("storage_warning", "Acme Compute is at 85% of its storage quota")
