from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


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
                conn.execute(
                    """
                    INSERT INTO invoices (invoice_id, org_id, period_start, period_end,
                                           currency, amount_cents, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'draft', ?)
                    """,
                    (invoice_id, org_id, period_start, period_end, currency, amount_cents, now),
                )
                for item in line_items:
                    conn.execute(
                        """
                        INSERT INTO invoice_line_items
                            (line_item_id, invoice_id, description, quantity, unit,
                             unit_price_cents, amount_cents)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            uuid.uuid4().hex, invoice_id, item["description"],
                            item["quantity"], item["unit"], item["unit_price_cents"],
                            item["amount_cents"],
                        ),
                    )
        except sqlite3.IntegrityError:
            existing = self.get_for_period(org_id, period_start, period_end)
            if existing is not None:
                return existing
            raise
        return self.get(invoice_id)

    def get(self, invoice_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one("SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,))
        if row is None:
            return None
        invoice = dict(row)
        invoice["line_items"] = [
            dict(r) for r in self.db.query(
                "SELECT * FROM invoice_line_items WHERE invoice_id = ? ORDER BY rowid",
                (invoice_id,),
            )
        ]
        return invoice

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
