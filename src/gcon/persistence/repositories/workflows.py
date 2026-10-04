"""Durable storage for workflow (DAG) definitions and runtime state."""
import json
from datetime import datetime, UTC
from typing import Any, Dict, List


class WorkflowRepository:
    def __init__(self, db):
        self.db = db

    def save(self, workflow: Dict[str, Any], state: Dict[str, Any]) -> None:
        """Insert or update one workflow. `workflow` / `state` are the
        to_dict() forms of Workflow / WorkflowState."""
        now = datetime.now(UTC).isoformat()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO workflows (workflow_id, name, org_id, created_by, status,
                                       definition_json, state_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workflow_id) DO UPDATE SET
                    status = excluded.status,
                    definition_json = excluded.definition_json,
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (
                    workflow["workflow_id"], workflow.get("name"), workflow.get("org_id"),
                    workflow.get("created_by"), state["status"],
                    json.dumps(workflow), json.dumps(state),
                    workflow.get("created_at") or now, now,
                ),
            )

    def list_all(self) -> List[Dict[str, Any]]:
        rows = self.db.query("SELECT * FROM workflows ORDER BY created_at ASC")
        out = []
        for r in rows:
            d = dict(r)
            d["workflow"] = json.loads(d.pop("definition_json"))
            d["state"] = json.loads(d.pop("state_json"))
            out.append(d)
        return out
