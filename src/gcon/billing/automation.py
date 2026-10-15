"""
Automatic monthly invoicing.

Once a month has ended, every organization that ran completed jobs in it gets a DRAFT invoice for that month,
without anyone having to ask for it. What it does and doesn't do:

* It only ever creates drafts. Charging a customer (`finalize_invoice`) happens automatically only if an
  operator opts in with GCON_BILLING_AUTO_FINALIZE=1; otherwise staff review and finalize drafts themselves.
* It is safe to run again and again. A month that already has an invoice is left alone, and generating is
  idempotent underneath, so a restart, a second coordinator or a missed run can't bill anyone twice.
* It catches up: each run covers the last GCON_BILLING_CATCHUP_MONTHS complete months (default 3), so a month
  missed because the coordinator was down is picked up on the next run.
* It never invoices a month that hasn't ended, never creates an empty invoice, and skips (and reports) a month
  that would overlap an invoice staff created by hand with a different period.
* Only the leader coordinator runs it, from the health loop, in its own short-lived thread, so it can't delay
  scheduling.

Settings (environment):
  GCON_BILLING_AUTO_INVOICE      1 (default) / 0 -- the scheduled run. "Run now" in the dashboard works either way.
  GCON_BILLING_AUTO_FINALIZE     0 (default) / 1 -- also charge the drafts it creates.
  GCON_BILLING_INTERVAL_SECONDS  how often to run (default 3600).
  GCON_BILLING_CATCHUP_MONTHS    complete months covered per run (default 3).
  GCON_BILLING_FIRST_RUN_DELAY   seconds after startup before the first run when none has happened yet (default 120).
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Tuple

from gcon.billing.invoicing import (
    default_billing_period, finalize_invoice, generate_invoice, make_receipt_validator,
)
from gcon.persistence.repositories.billing import InvoiceOverlapError

_log = logging.getLogger("gcon.billing")
LAST_RUN_KEY = "billing.automation.last_run"
_MAX_ERRORS_KEPT = 20


def _flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    return default if raw is None else raw.strip().lower() in ("1", "true", "yes", "on")


def _number(name: str, default: float, minimum: float) -> float:
    try:
        return max(float(os.environ.get(name, default)), minimum)
    except ValueError:
        return default


def _shift_months(start: datetime, months: int) -> datetime:
    index = start.year * 12 + (start.month - 1) + months
    return start.replace(year=index // 12, month=index % 12 + 1)


def previous_months(now: datetime, count: int) -> List[Tuple[datetime, datetime]]:
    """The `count` complete UTC calendar months before the one `now` is in, oldest first, as (start, end)."""
    current = default_billing_period(now)[0]
    return [(_shift_months(current, -i), _shift_months(current, -i + 1)) for i in range(count, 0, -1)]


class BillingAutomation:
    def __init__(self, coordinator):
        self.c = coordinator
        self._gate = threading.Lock()        # guards _running / _next_due
        self._run_lock = threading.Lock()    # one invoicing run at a time, scheduled or manual
        self._running = False
        self._next_due: Optional[float] = None

    # ------------------------------------------------------------ settings
    @property
    def enabled(self) -> bool:
        return _flag("GCON_BILLING_AUTO_INVOICE", True)

    @property
    def auto_finalize(self) -> bool:
        return _flag("GCON_BILLING_AUTO_FINALIZE", False)

    @property
    def interval(self) -> float:
        return _number("GCON_BILLING_INTERVAL_SECONDS", 3600.0, 60.0)

    @property
    def catchup_months(self) -> int:
        return int(_number("GCON_BILLING_CATCHUP_MONTHS", 3, 1))

    def _plane(self):
        return getattr(self.c, "control_plane", None)

    def _is_leader(self) -> bool:
        elector = getattr(self.c, "leader_elector", None)
        return elector is None or bool(elector.is_leader)

    # ------------------------------------------------------------ scheduling
    def last_run(self) -> Optional[Dict[str, Any]]:
        plane = self._plane()
        raw = plane.settings.get(LAST_RUN_KEY) if plane is not None else None
        try:
            return json.loads(raw) if raw else None
        except (TypeError, ValueError):
            return None

    def _first_due(self, mono: float) -> float:
        last = self.last_run()
        if last and last.get("finished_at"):
            try:
                age = (datetime.now(UTC) - datetime.fromisoformat(last["finished_at"])).total_seconds()
                return mono + max(0.0, self.interval - age)
            except ValueError:
                pass
        return mono + _number("GCON_BILLING_FIRST_RUN_DELAY", 120.0, 0.0)

    def maybe_tick(self) -> bool:
        """Called every health-loop pass. Starts a run (in its own thread) only when one is due, on the leader."""
        if not self.enabled or self._plane() is None or not self._is_leader():
            return False
        mono = time.monotonic()
        with self._gate:
            if self._next_due is None:
                self._next_due = self._first_due(mono)
            if mono < self._next_due or self._running:
                return False
            self._running = True
            self._next_due = mono + self.interval
        threading.Thread(target=self._run_in_thread, name="billing-automation", daemon=True).start()
        return True

    def _run_in_thread(self) -> None:
        try:
            self.run_once(trigger="schedule")
        except Exception:
            _log.exception("automatic invoicing run failed")
        finally:
            with self._gate:
                self._running = False

    # ------------------------------------------------------------ the run
    def run_once(self, now: Optional[datetime] = None, months: Optional[int] = None,
                 trigger: str = "manual", finalize: Optional[bool] = None) -> Dict[str, Any]:
        """Invoice every complete month in the catch-up window for every organization with usage in it.
        Returns (and remembers) a summary. Raises ValueError if a run is already in progress."""
        plane = self._plane()
        if plane is None:
            raise ValueError("No control plane available to invoice from.")
        if not self._run_lock.acquire(blocking=False):
            raise ValueError("An invoicing run is already in progress.")
        try:
            started = datetime.now(UTC)
            now = (now or started).astimezone(UTC)
            do_finalize = self.auto_finalize if finalize is None else finalize
            verifier = getattr(self.c, "verifier", None)
            validator = make_receipt_validator(verifier) if verifier is not None else None
            summary: Dict[str, Any] = {
                "trigger": trigger, "started_at": started.isoformat(), "finished_at": None, "months": [],
                "generated": 0, "finalized": 0, "already_invoiced": 0, "overlap_skipped": 0, "failed": 0,
                "errors": [], "auto_finalize": do_finalize,
            }

            def note(message: str) -> None:
                if len(summary["errors"]) < _MAX_ERRORS_KEPT:
                    summary["errors"].append(message)

            for start, end in previous_months(now, months or self.catchup_months):
                s_iso, e_iso = start.isoformat(), end.isoformat()
                summary["months"].append(s_iso[:7])
                orgs = plane.db.query(
                    "SELECT DISTINCT org_id FROM jobs WHERE org_id IS NOT NULL AND status = 'completed' "
                    "AND completed_at >= ? AND completed_at < ?", (s_iso, e_iso))
                for row in orgs:
                    org_id = row["org_id"]
                    try:
                        invoice = plane.invoices.get_for_period(org_id, s_iso, e_iso)
                        if invoice is not None:
                            summary["already_invoiced"] += 1
                        else:
                            invoice = generate_invoice(plane, org_id, s_iso, e_iso, receipt_validator=validator,
                                                       now=now, skip_empty=True)
                            if invoice is None:
                                continue
                            summary["generated"] += 1
                        if do_finalize and invoice["status"] == "draft":
                            finalize_invoice(plane, invoice["invoice_id"])
                            summary["finalized"] += 1
                    except InvoiceOverlapError as e:
                        summary["overlap_skipped"] += 1
                        note(f"{org_id} {s_iso[:7]}: {e}")
                    except Exception as e:
                        summary["failed"] += 1
                        note(f"{org_id} {s_iso[:7]}: {e!r}")
                        _log.exception("invoicing %s for %s failed", s_iso[:7], org_id)
            summary["finished_at"] = datetime.now(UTC).isoformat()
            plane.settings.set(LAST_RUN_KEY, json.dumps(summary), updated_by="billing-automation")
            _log.info("invoicing run (%s): %s", trigger, {k: v for k, v in summary.items() if k not in ("errors",)})
            return summary
        finally:
            self._run_lock.release()

    # ------------------------------------------------------------ for the dashboard
    def status(self) -> Dict[str, Any]:
        provider_name, provider_ready, real_money = None, True, False
        try:
            from gcon.billing.providers import get_configured_provider
            provider_name = getattr(get_configured_provider(), "name", None)
            real_money = provider_name not in (None, "mock")
        except Exception as e:                      # e.g. a provider selected but not configured
            provider_name = os.environ.get("GCON_PAYMENT_PROVIDER", "mock")
            provider_ready = False
        next_run = None
        if self.enabled and self._next_due is not None:
            next_run = datetime.fromtimestamp(time.time() + (self._next_due - time.monotonic()), UTC).isoformat()
        return {
            "enabled": self.enabled, "auto_finalize": self.auto_finalize, "interval_seconds": self.interval,
            "catchup_months": self.catchup_months, "running": self._running or self._run_lock.locked(),
            "next_run_at": next_run, "last_run": self.last_run(),
            "payment_provider": provider_name, "payment_provider_ready": provider_ready,
            "moves_real_money": real_money,
        }
