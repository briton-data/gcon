from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


class TelemetryRepository:
    """
    Durable store for TelemetryEvent rows (see gcon.telemetry) --
    distinct from ClusterEventRepository (transport/connection-level
    events only) and from the in-memory notification EventBus. This
    is the fuller job-lifecycle trace: every event carries a trace_id
    minted once at submit_job() and threaded through the job's whole
    lifecycle, so `for_trace()` returns one job's complete story in
    order, not scattered log lines.
    """

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def record(
        self,
        event_id: str,
        trace_id: str,
        event_type: str,
        level: str = "INFO",
        job_id: Optional[str] = None,
        node_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        created_at: Optional[str] = None,
    ) -> None:
        from datetime import datetime, UTC

        self.db.execute(
            """
            INSERT INTO telemetry_events
                (event_id, trace_id, job_id, node_id, event_type, level, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                trace_id,
                job_id,
                node_id,
                event_type,
                level,
                json.dumps(payload) if payload is not None else None,
                created_at or datetime.now(UTC).isoformat(),
            ),
        )

    def query(
        self,
        job_id: Optional[str] = None,
        node_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        since: Optional[str] = None,
        org_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Matches the GET /telemetry/events?job_id=&node_id=&since=
        query shape directly -- every filter is optional and ANDed
        together when given. `since` is an ISO-8601 timestamp string
        (same convention as every other created_at/completed_at column
        in this DB), inclusive.

        `org_id`, when given, joins against jobs the same way
        ReceiptRepository.search_paginated's org_id filter already
        does (a telemetry_events row has no org_id column of its own,
        only jobs.org_id does) -- this is the org-scoping the
        GET /telemetry/events route applies per API key, same pattern
        as the receipts cross-tenant-leak fix earlier this project. A
        row with no job_id (not every event is about a specific job)
        is excluded when org_id is given, same as it would be from any
        job-scoped view -- there's no org to attribute it to.
        """
        clauses = []
        params: List[Any] = []
        select = "t.*" if org_id is not None else "*"
        from_clause = (
            "telemetry_events t JOIN jobs j ON j.job_id = t.job_id"
            if org_id is not None else "telemetry_events"
        )
        prefix = "t." if org_id is not None else ""
        if org_id is not None:
            clauses.append("j.org_id = ?")
            params.append(org_id)
        if job_id is not None:
            clauses.append(f"{prefix}job_id = ?")
            params.append(job_id)
        if node_id is not None:
            clauses.append(f"{prefix}node_id = ?")
            params.append(node_id)
        if trace_id is not None:
            clauses.append(f"{prefix}trace_id = ?")
            params.append(trace_id)
        if since is not None:
            clauses.append(f"{prefix}created_at >= ?")
            params.append(since)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        order_col = f"{prefix}id"
        rows = self.db.query(
            f"SELECT {select} FROM {from_clause} {where} ORDER BY {order_col} DESC LIMIT ?",
            tuple(params),
        )
        return [self._row_to_dict(r) for r in rows]

    def for_trace(self, trace_id: str, limit: int = 500) -> List[Dict[str, Any]]:
        """A job's complete lifecycle, in chronological (not reverse)
        order -- unlike query() above (reverse-chron, for "what's
        recent"), this reconstructs one trace as a story, oldest
        event first."""
        rows = self.db.query(
            "SELECT * FROM telemetry_events WHERE trace_id = ? ORDER BY id ASC LIMIT ?",
            (trace_id, limit),
        )
        return [self._row_to_dict(r) for r in rows]

    def count_by_event_type(self, since: Optional[str] = None) -> Dict[str, int]:
        """Backing query for gcon.telemetry's derived metrics
        (jobs_submitted_total, verification_pass_total, ...) --
        counts every event_type actually recorded, so a metric this
        doesn't recognize yet still shows up under its real name
        rather than being silently dropped."""
        if since is not None:
            rows = self.db.query(
                "SELECT event_type, COUNT(*) as n FROM telemetry_events WHERE created_at >= ? GROUP BY event_type",
                (since,),
            )
        else:
            rows = self.db.query(
                "SELECT event_type, COUNT(*) as n FROM telemetry_events GROUP BY event_type",
            )
        return {row["event_type"]: row["n"] for row in rows}

    @staticmethod
    def _row_to_dict(row) -> Dict[str, Any]:
        d = dict(row)
        d["payload"] = json.loads(d.pop("payload_json")) if d.get("payload_json") else None
        return d
