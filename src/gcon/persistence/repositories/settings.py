from __future__ import annotations

from datetime import datetime, UTC
from typing import Optional

from gcon.persistence.db import ControlPlaneDatabase


class SettingsRepository:
    """
    Durable operator-configurable settings, the middle tier of GCON's
    configuration precedence (environment variables > database
    settings > hardcoded defaults -- see `gcon.transport.config`).
    Plain key/value: values are always stored as TEXT and parsed by
    the caller, exactly like `gcon.storage.database`'s JSON columns.
    """

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def get(self, key: str) -> Optional[str]:
        row = self.db.query_one("SELECT value FROM settings WHERE key = ?", (key,))
        return row["value"] if row else None

    def set(self, key: str, value: str, updated_by: Optional[str] = None) -> None:
        now = datetime.now(UTC).isoformat()
        existing = self.get(key)
        with self.db.transaction() as conn:
            if existing is None:
                conn.execute(
                    "INSERT INTO settings (key, value, updated_at, updated_by) "
                    "VALUES (?, ?, ?, ?)",
                    (key, value, now, updated_by),
                )
            else:
                conn.execute(
                    "UPDATE settings SET value = ?, updated_at = ?, updated_by = ? "
                    "WHERE key = ?",
                    (value, now, updated_by, key),
                )

    def delete(self, key: str) -> None:
        self.db.execute("DELETE FROM settings WHERE key = ?", (key,))

    def all(self) -> dict:
        rows = self.db.query("SELECT key, value FROM settings")
        return {r["key"]: r["value"] for r in rows}
