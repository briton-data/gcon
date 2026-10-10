"""
Which of GCON's shared (operator-run) workers a customer's jobs may use.

A shared-pool worker serves any organization's jobs, so for those jobs the
sandbox is the ONLY isolation boundary (a dedicated worker adds the org
boundary on top). That is a tenancy decision, so it is the customer's:

  off    never run this organization's jobs on a shared worker
  basic  (default) only jobs that bring nothing of the customer's own beyond
         the command -- no artifacts, no datasets
  full   the customer has opted in: jobs with their artifacts/datasets too
"""
from datetime import UTC, datetime
from typing import Optional

POOL_MODES = ("off", "basic", "full")
DEFAULT_POOL_MODE = "basic"


class OrgPoolSettingsRepository:
    def __init__(self, db):
        self.db = db

    def get_mode(self, org_id: Optional[str]) -> str:
        if not org_id:
            return "off"
        row = self.db.query_one("SELECT mode FROM org_pool_settings WHERE org_id = ?", (org_id,))
        if row and row["mode"] in POOL_MODES:
            return row["mode"]
        return DEFAULT_POOL_MODE

    def set_mode(self, org_id: str, mode: str) -> str:
        if mode not in POOL_MODES:
            raise ValueError(f"mode must be one of {', '.join(POOL_MODES)}")
        if not org_id:
            raise ValueError("org_id is required")
        now = datetime.now(UTC).isoformat()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO org_pool_settings (org_id, mode, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT (org_id) DO UPDATE SET mode = excluded.mode, updated_at = excluded.updated_at",
                (org_id, mode, now),
            )
        return mode
