"""
GCON Audit Log — records important management actions.
"""

from datetime import datetime, UTC
from uuid import uuid4


class AuditLogger:
    def __init__(self, max_entries=500):
        self.entries = []
        self.max_entries = max_entries

    def log(self, actor, action, target=None):
        entry = {
            "entry_id": f"audit_{uuid4().hex[:8]}",
            "actor": actor,
            "action": action,
            "target": target,
            "timestamp": datetime.now(UTC).isoformat(),
        }
        self.entries.append(entry)
        if len(self.entries) > self.max_entries:
            self.entries = self.entries[-self.max_entries:]
        return entry

    def list_entries(self, limit=100):
        return list(reversed(self.entries[-limit:]))
