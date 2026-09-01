"""
DB-level retention policy — the piece that was still missing after
the earlier in-memory eviction fix (see coordinator.py's
_evict_completed_if_over_capacity docstring): that fix bounds how
much history lives in the coordinator process's memory, but the
underlying control-plane DB tables (jobs, receipts) themselves still
grow forever, so `restore_from_persistence()` and any filtered/
unlimited get_jobs()/get_receipts() query were still O(total history)
by construction -- there was no history to *not* have.

This module actually deletes old rows, on two independent axes,
either or both of which can be enabled:

  * age-based: GCON_DB_RETENTION_DAYS (default: unset/disabled).
    Terminal jobs/receipts older than N days are purged.
  * row-count-based: GCON_DB_MAX_TERMINAL_JOBS /
    GCON_DB_MAX_RECEIPTS (defaults: unset/disabled). Keeps only the
    newest N rows regardless of age.

Both default to disabled -- unlike the in-memory cap (which only
affects a live process's RAM and defaults ON), physically deleting
rows is a one-way door for anyone who wanted that history for
auditing/compliance (receipts are the tamper-evident proof-of-work
record this whole system exists to produce), so it needs an explicit
opt-in. Pending/running jobs are never purged by either axis,
regardless of age -- see JobRepository.purge_terminal_older_than's
docstring.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, UTC
from typing import Dict

from gcon.persistence.control_plane import ControlPlane


class RetentionPolicy:
    def __init__(self):
        raw_days = os.environ.get("GCON_DB_RETENTION_DAYS")
        self.retention_days = float(raw_days) if raw_days else None

        raw_max_jobs = os.environ.get("GCON_DB_MAX_TERMINAL_JOBS")
        self.max_terminal_jobs = int(raw_max_jobs) if raw_max_jobs else None

        raw_max_receipts = os.environ.get("GCON_DB_MAX_RECEIPTS")
        self.max_receipts = int(raw_max_receipts) if raw_max_receipts else None

    @property
    def enabled(self) -> bool:
        return bool(self.retention_days or self.max_terminal_jobs or self.max_receipts)

    def sweep(self, control_plane: ControlPlane) -> Dict[str, int]:
        """Runs whichever purge axes are configured. Returns a dict of
        how many rows were removed by each, for logging/testing.
        Safe to call repeatedly (e.g. from a periodic background
        loop) -- each call is independently a no-op once nothing
        matches its condition."""
        removed = {"jobs_by_age": 0, "jobs_by_count": 0,
                   "receipts_by_age": 0, "receipts_by_count": 0}

        if self.retention_days:
            cutoff = (
                datetime.now(UTC) - timedelta(days=self.retention_days)
            ).isoformat()
            removed["jobs_by_age"] = control_plane.jobs.purge_terminal_older_than(cutoff)
            removed["receipts_by_age"] = control_plane.receipts.purge_older_than(cutoff)

        if self.max_terminal_jobs is not None:
            if control_plane.jobs.count_terminal() > self.max_terminal_jobs:
                removed["jobs_by_count"] = control_plane.jobs.purge_terminal_keep_newest(
                    self.max_terminal_jobs
                )

        if self.max_receipts is not None:
            if control_plane.receipts.count_all() > self.max_receipts:
                removed["receipts_by_count"] = control_plane.receipts.purge_keep_newest(
                    self.max_receipts
                )

        return removed
