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


def seed_audit_log(logger):
    """
    Populate the log with illustrative demo entries.
    """
    seed_entries = [
        ("Avery Chen", "created workflow", "Workflow A"),
        ("Priya Nair", "deregistered node", "node-004"),
        ("Marcus Webb", "generated API key", "CI/CD Pipeline"),
        ("Avery Chen", "changed permissions", "Sofia Ramirez"),
        ("System", "created organization", "Nimbus Labs"),
    ]
    for actor, action, target in seed_entries:
        logger.log(actor, action, target)
