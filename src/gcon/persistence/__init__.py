"""
GCON Persistence Layer — the durable control plane for the cluster.

This package is deliberately separate from `gcon.storage` (which
persists identity/security state such as users, API keys, and
organizations — see `gcon.storage.database`). This package persists
*cluster control-plane* state: node inventory, job/attempt history,
receipts, heartbeats, cluster events, execution logs, runtime
settings, and node capabilities.

Backed by SQLite today (stdlib `sqlite3`, WAL mode, foreign keys on),
but every query in every repository is written using the portable
subset of SQL supported by both SQLite and PostgreSQL (parameter
placeholders are abstracted via `ControlPlaneDatabase.execute`,
no SQLite-only functions in queries, no `INSERT OR REPLACE` outside
of a dedicated upsert helper, explicit column lists, explicit
transactions). See `db.py` for the compatibility notes and `schema.py`
migrations for the versioned schema history.
"""

from gcon.persistence.db import ControlPlaneDatabase
from gcon.persistence.control_plane import ControlPlane

__all__ = ["ControlPlaneDatabase", "ControlPlane"]
