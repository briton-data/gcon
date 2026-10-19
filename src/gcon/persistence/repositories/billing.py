from __future__ import annotations

import json
import uuid
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


def _instant(value: str) -> datetime:
    """A stored period boundary as an aware UTC datetime (no zone means UTC), so periods written with
    different spellings of the same instant ('...Z', '...+00:00', a bare date) still compare as equal."""
    parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


class InvoiceOverlapError(ValueError):
    """The requested period overlaps an invoice this organization already has."""


class InvoiceRepository:
    """
    Durable store for invoices generated from usage-metering data
    (see `gcon.billing.invoicing.generate_invoice`). `UNIQUE(org_id,
    period_start, period_end)` makes re-running invoice generation
    for a period idempotent -- the same billing period never produces
    two invoices for the same org.
    """

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def create(
        self,
        org_id: str,
        period_start: str,
        period_end: str,
        currency: str,
        line_items: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        existing = self.get_for_period(org_id, period_start, period_end)
        if existing is not None:
            return existing

        invoice_id = uuid.uuid4().hex
        amount_cents = sum(item["amount_cents"] for item in line_items)
        now = datetime.now(UTC).isoformat()
        try:
            with self.db.transaction() as conn:
                # Checked inside the transaction (which holds the database lock), so two callers racing for the
                # same organization cannot both pass it.
                found = self._find_same_or_overlapping(org_id, period_start, period_end)
                if found is not None and found[1] == "same":
                    return self.get(found[0]["invoice_id"])
                if found is not None:
                    clash = found[0]
                    raise InvoiceOverlapError(
                        f"This period overlaps invoice {clash['invoice_id'][:12]} for the same organization "
                        f"({clash['period_start']} to {clash['period_end']}, {clash['status']}). "
                        "An organization can't be billed twice for the same time."
                    )
                conn.execute(
                    """
                    INSERT INTO invoices (invoice_id, org_id, period_start, period_end,
                                           currency, amount_cents, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'draft', ?)
                    """,
                    (invoice_id, org_id, period_start, period_end, currency, amount_cents, now),
                )
                for position, item in enumerate(line_items):
                    conn.execute(
                        """
                        INSERT INTO invoice_line_items
                            (line_item_id, invoice_id, description, quantity, unit,
                             unit_price_cents, amount_cents, unit_price, position)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            uuid.uuid4().hex, invoice_id, item["description"],
                            item["quantity"], item["unit"], int(round(item["unit_price_cents"])),
                            item["amount_cents"], float(item["unit_price_cents"]), position,
                        ),
                    )
        except self.db.IntegrityError:
            existing = self.get_for_period(org_id, period_start, period_end)
            if existing is not None:
                return existing
            raise
        return self.get(invoice_id)

    def _find_same_or_overlapping(self, org_id, period_start, period_end):
        """(invoice_row, 'same' | 'overlap') for the first invoice of this organization that covers exactly the
        same instants, or part of them, else None. Compared as instants rather than strings."""
        start, end = _instant(period_start), _instant(period_end)
        overlap = None
        for row in self.db.query(
            "SELECT invoice_id, period_start, period_end, status FROM invoices WHERE org_id = ?", (org_id,)
        ):
            try:
                other_start, other_end = _instant(row["period_start"]), _instant(row["period_end"])
            except ValueError:
                continue
            if other_start == start and other_end == end:
                return dict(row), "same"
            if other_start < end and other_end > start and overlap is None:
                overlap = (dict(row), "overlap")
        return overlap

    def get(self, invoice_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one("SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,))
        if row is None:
            return None
        invoice = dict(row)
        invoice["line_items"] = [
            self._line_item(r) for r in self.db.query(
                "SELECT * FROM invoice_line_items WHERE invoice_id = ? ORDER BY position",
                (invoice_id,),
            )
        ]
        return invoice

    @staticmethod
    def _line_item(row) -> Dict[str, Any]:
        """A line item as the API presents it: the exact unit price when there is one (rows written before
        migration 16 only have the legacy column), without the storage-only columns."""
        item = dict(row)
        exact = item.pop("unit_price", None)
        item.pop("position", None)
        if exact is not None:
            item["unit_price_cents"] = exact
        return item

    def get_for_period(self, org_id: str, period_start: str, period_end: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT invoice_id FROM invoices WHERE org_id = ? AND period_start = ? AND period_end = ?",
            (org_id, period_start, period_end),
        )
        return self.get(row["invoice_id"]) if row else None

    def list_for_org(self, org_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT invoice_id FROM invoices WHERE org_id = ? ORDER BY period_start DESC LIMIT ?",
            (org_id, limit),
        )
        return [self.get(r["invoice_id"]) for r in rows]

    def list_issued_for_org(self, org_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """An org's invoices that have left `draft` (open/paid/failed/void),
        newest period first. Drafts are staff working copies and are never
        shown to the customer."""
        rows = self.db.query(
            "SELECT invoice_id FROM invoices WHERE org_id = ? AND status != 'draft' "
            "ORDER BY period_start DESC LIMIT ?",
            (org_id, limit),
        )
        return [self.get(r["invoice_id"]) for r in rows]

    def list_all(self, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        if status:
            rows = self.db.query(
                "SELECT invoice_id FROM invoices WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            )
        else:
            rows = self.db.query(
                "SELECT invoice_id FROM invoices ORDER BY created_at DESC LIMIT ?", (limit,)
            )
        return [self.get(r["invoice_id"]) for r in rows]

    def mark_status(
        self,
        invoice_id: str,
        status: str,
        provider: Optional[str] = None,
        provider_charge_id: Optional[str] = None,
        provider_error: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        now = datetime.now(UTC).isoformat()
        finalized_at = now if status in ("open", "paid", "failed", "void") else None
        paid_at = now if status == "paid" else None
        self.db.execute(
            """
            UPDATE invoices SET status = ?,
                provider = COALESCE(?, provider),
                provider_charge_id = COALESCE(?, provider_charge_id),
                provider_error = ?,
                finalized_at = COALESCE(finalized_at, ?),
                paid_at = COALESCE(paid_at, ?)
            WHERE invoice_id = ?
            """,
            (status, provider, provider_charge_id, provider_error, finalized_at, paid_at, invoice_id),
        )
        return self.get(invoice_id)
