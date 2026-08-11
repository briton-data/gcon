# GCON API Reference

Complete guide to the GCON Public API (`/api/v1`) and the `gcon_sdk` Python client.

This document was rebuilt by reading the live source — `src/gcon/api/api_v1.py`
(the FastAPI app that defines every route) and `sdk/gcon_sdk/client.py` (the
SDK that wraps it) — not from assumptions about what the API "should" have.
If this ever disagrees with what you see running, trust the code and the
interactive docs at `/api/v1/docs` over this file.

---

## Table of Contents

1. [REST API Overview](#rest-api-overview)
2. [Authentication](#authentication)
3. [Endpoints](#endpoints)
4. [Python SDK](#python-sdk)
5. [Verification](#verification)
6. [Error Handling](#error-handling)
7. [Interactive Docs](#interactive-docs)

---

## REST API Overview

**Base URL:**
```
http://localhost:8000/api/v1
```

**Protocol:** HTTP/REST, JSON request and response bodies.

**API Version:** v1 (`FastAPI` app mounted at `/api/v1` by the dashboard's
`web_server.py`, defined in `src/gcon/api/api_v1.py`).

The public API is intentionally separate from the dashboard's own
cookie-session routes (`/management/...`, `/dashboard/...`): every request
here is authenticated with a real API key, never a browser session. Both
the public API and the dashboard read from the same shared
`PresentationLayer`/`Coordinator` instances, so they always report the same
live state — there is no mock or placeholder data.

There is no `/events` or `/stream` endpoint on `/api/v1` in the current
build. Live push updates are served to the dashboard UI itself over a
WebSocket at `/ws` (cookie-authenticated), not through the public API.

---

## Authentication

Every route in `api_v1.py` is protected by an API key, checked by
`require_scope()`. Send the key as **either**:

```bash
curl -H "Authorization: Bearer gcon_YOUR_API_KEY" \
  http://localhost:8000/api/v1/cluster

# or

curl -H "X-API-Key: gcon_YOUR_API_KEY" \
  http://localhost:8000/api/v1/cluster
```

There is no unauthenticated/dev mode — a request with no key returns
`401` with `"Missing API key. Send it as 'Authorization: Bearer <key>' or
'X-API-Key: <key>'."`.

### Getting an API key

1. Log in to the dashboard.
2. Go to **Management → API Keys → Create Key**.
3. Choose scopes. Scopes are free-form strings (`APIKeyManager.create_key`
   accepts any list), but only two values are actually enforced by the
   public API today:
   - `View monitoring` — required by every `GET` endpoint below
   - `Submit workflows` — required by every job/workflow write endpoint
   - A key created without an explicit scope list defaults to both:
     `["Submit workflows", "View monitoring"]`.
4. Copy the secret — it's shown only once.

An invalid, unknown, expired, or revoked key returns `401` with a
deliberately generic `"Invalid or expired API key."` (so a failed request
can't be used to enumerate valid keys). A key missing the required scope
returns `401` with `"This API key does not have the '<scope>' scope."`.

---

## Endpoints

Every route below is defined in `src/gcon/api/api_v1.py`. "Scope" is the
value passed to `require_scope(...)` for that route.

### Cluster

| Method | Path | Scope | Summary |
|---|---|---|---|
| `GET` | `/cluster` | `View monitoring` | Current cluster state |
| `GET` | `/health` | `View monitoring` | Overall cluster health |
| `GET` | `/metrics` | `View monitoring` | Aggregate node/job metrics |

#### `GET /cluster`

```json
{
  "total_nodes": 4,
  "idle_nodes": 2,
  "registered_node_count": 4,
  "running_jobs": 1,
  "completed_jobs": 126,
  "failed_jobs": 3
}
```
The response model (`ClusterStateOut`) allows extra fields, and the
coordinator's raw payload also includes `registered_nodes` (the full
per-node list) alongside the counts above.

#### `GET /health`

```json
{
  "state": "healthy",
  "score": 98,
  "reason": "All checks passing",
  "reasons": [],
  "computed_at": "2026-08-10T09:00:00+00:00",
  "checks": { "coordinator": {"healthy": true}, "storage": {"healthy": true} },
  "services": {
    "coordinator": "online",
    "cluster": "healthy",
    "event_system": "running",
    "storage": "connected"
  },
  "last_issue": null,
  "metrics": {
    "total_nodes": 4,
    "running_jobs": 1,
    "completed_jobs": 126,
    "failed_jobs": 3
  }
}
```

#### `GET /metrics`

```json
{
  "avg_cpu": 12.4,
  "avg_memory": 34.1,
  "running_jobs": 1,
  "completed_jobs": 126,
  "failed_jobs": 3,
  "event_count": 512,
  "uptime_seconds": 7340
}
```

---

### Nodes

| Method | Path | Scope | Summary |
|---|---|---|---|
| `GET` | `/nodes` | `View monitoring` | List all registered nodes |
| `GET` | `/nodes/{node_id}` | `View monitoring` | Get one node |

```json
{
  "node_id": "worker-01",
  "status": "idle",
  "cpu": 5.2,
  "memory": 12.4,
  "running_jobs": 0,
  "last_seen": "2026-08-10T09:00:00+00:00",
  "draining": false
}
```
`cpu`/`memory` come back as the string `"N/A"` instead of a number until
the node has reported a reading. `GET /nodes/{node_id}` returns `404` if
the node isn't currently registered.

---

### Jobs

| Method | Path | Scope | Summary |
|---|---|---|---|
| `GET` | `/jobs` | `View monitoring` | List all jobs |
| `GET` | `/jobs/{job_id}` | `View monitoring` | Get one job |
| `POST` | `/jobs` | `Submit workflows` | Submit a new job |
| `POST` | `/jobs/{job_id}/cancel` | `Submit workflows` | Cancel a running job |

There is **no** `DELETE /jobs/{job_id}` — cancellation is a `POST` to a
`/cancel` sub-path, not a `DELETE` on the job resource.

#### `POST /jobs`

**Request:**
```json
{
  "job_id": "my-job-001",
  "command": "python train.py --epochs 10",
  "artifacts": ["model.pkl"]
}
```
`artifacts` is optional — a plain list of file paths to register, not a
separate upload step. There is no `timeout_seconds` or `tags` field on
this endpoint.

**Response:**
```json
{ "job_id": "my-job-001", "submitted": true }
```

**Errors:** `400` if the job can't be submitted (e.g. duplicate
`job_id` — the coordinator raises `ValueError`, which becomes `400`, not
`409`). `401` for a missing/invalid/under-scoped key.

#### `GET /jobs` / `GET /jobs/{job_id}`

```json
{
  "job_id": "my-job-001",
  "status": "completed",
  "node_id": "worker-01",
  "created_at": "2026-08-10T09:00:00+00:00",
  "completed_at": "2026-08-10T09:00:04+00:00",
  "receipt_id": "a1b2c3d4e5f6...",
  "artifacts": 1,
  "created_by": "user_abc123",
  "workflow_id": null
}
```
`GET /jobs` returns a plain JSON array (not wrapped in `{"jobs": [...],
"total": ...}`) — there's no pagination (`limit`/`offset`) or `status`
query filter on this endpoint. `artifacts` here is a **count**, not a list
of artifact objects; fetch `/artifacts` separately for file details.
`GET /jobs/{job_id}` returns `404` if the job doesn't exist.

#### `POST /jobs/{job_id}/cancel`

```json
{ "job_id": "my-job-001", "cancelled": true, "process_killed": false }
```
**Errors:** `400` if the job can't be cancelled (already finished, etc.).

---

### Workflows

| Method | Path | Scope | Summary |
|---|---|---|---|
| `POST` | `/workflows` | `Submit workflows` | Submit a workflow (DAG of jobs) |
| `GET` | `/workflows` | `View monitoring` | List all workflows |

There is no `GET /workflows/{workflow_id}` detail route on the public API
today — only the list endpoint.

#### `POST /workflows`

**Request:**
```json
{
  "workflow_id": "training-pipeline-v1",
  "name": "Training pipeline",
  "jobs": [
    { "job_id": "download", "command": "python download_data.py", "depends_on": [] },
    { "job_id": "train", "command": "python train.py", "depends_on": ["download"] }
  ]
}
```
Note the field is `jobs` (each with `job_id` / `command` / `depends_on`),
not `tasks` / `task_id`.

**Response:**
```json
{ "workflow_id": "training-pipeline-v1", "status": "pending", "submitted": true }
```
**Errors:** `400` for an invalid DAG (cycle, unknown dependency, etc.).

#### `GET /workflows`

Returns a plain array of workflow state summaries (`workflow_id`,
`status`, plus whatever else `WorkflowState.summary()` includes — the
schema allows extra fields).

---

### Receipts & Artifacts

| Method | Path | Scope | Summary |
|---|---|---|---|
| `GET` | `/receipts` | `View monitoring` | List all job receipts |
| `GET` | `/artifacts` | `View monitoring` | List all registered artifacts |

There is no `/receipts/{receipt_id}` or `/receipts/{receipt_id}/verify`
route on the public API — receipt verification is done directly against
the signed proof (see [Verification](#verification)), not as a server
round-trip.

#### `GET /receipts`

```json
[
  {
    "receipt_id": "a1b2c3d4e5f6...",
    "job_id": "my-job-001",
    "status": "completed",
    "created_at": "2026-08-10T09:00:04+00:00"
  }
]
```

#### `GET /artifacts`

```json
[
  {
    "artifact_id": "art_001",
    "filename": "model.pkl",
    "sha256": "9f86d081884c7d65...",
    "size": 524288,
    "uploaded_at": "2026-08-10T09:00:04+00:00"
  }
]
```

---

### Auth

| Method | Path | Scope | Summary |
|---|---|---|---|
| `GET` | `/whoami` | *(any valid key)* | Identify the calling API key |

```json
{
  "key_id": "key_abc123",
  "key_name": "CI pipeline",
  "scopes": ["Submit workflows", "View monitoring"],
  "owner_user_id": "user_abc123",
  "owner_name": "Jane Doe"
}
```

---

## Python SDK

### Installation

```bash
cd sdk && pip install -e .
# or just copy gcon_sdk/ into your project — its only dependency is `requests`
```

### Basic usage

```python
from gcon_sdk import GconClient

client = GconClient(api_key="gcon_...", base_url="http://localhost:8000")

# Cluster info
print(client.get_cluster())
print(client.get_health())
print(client.list_nodes())
print(client.get_node("worker-01"))

# Jobs
client.submit_job("job-001", "echo hello")
job = client.get_job("job-001")
jobs = client.list_jobs()
client.cancel_job("job-001")

# Workflows / receipts / artifacts
print(client.list_workflows())
print(client.list_receipts())
print(client.list_artifacts())

# Who am I
print(client.whoami())
```

> **Note:** `GconClient` (`sdk/gcon_sdk/client.py`) currently exposes
> `whoami`, `get_cluster`, `get_health`, `get_metrics`, `list_nodes`,
> `get_node`, `list_jobs`, `get_job`, `submit_job`, `cancel_job`,
> `list_workflows`, `list_receipts`, and `list_artifacts`. It does **not**
> have a `submit_workflow`, `get_receipt`, or `verify_receipt` wrapper
> yet — call `POST /workflows` directly (e.g. with `requests`) until the
> SDK grows a helper for it.

Use it as a context manager to close the underlying session:

```python
with GconClient(api_key="gcon_...") as client:
    print(client.list_jobs())
```

### Error handling

All non-2xx responses raise `gcon_sdk.GconAPIError`, carrying
`.status_code` and `.detail`:

```python
from gcon_sdk import GconClient, GconAPIError

client = GconClient(api_key="gcon_...")
try:
    client.submit_job("dup-id", "echo hi")
    client.submit_job("dup-id", "echo hi")  # duplicate job_id
except GconAPIError as e:
    print(e.status_code, e.detail)  # 400 "Job 'dup-id' already exists."
```

---

## Verification

There is no server-side `/receipts/{id}/verify` endpoint. Receipts are
checked against their signed proof directly. The coordinator itself
recomputes `verified` live via `ExecutionVerifier.validate_proof()` on
every call to `get_receipts()` (so the dashboard and `GET /receipts` can
never show a stale flag). Standalone, without a running coordinator:

```python
from gcon.execution.run_job import JobRunner

runner = JobRunner()
receipt = runner.get_job_receipt("a1b2c3d4e5f6...")
print(runner.print_receipt(receipt["receipt_id"], format="summary"))
```

Note there are two related but distinct verification paths in the
codebase — `gcon.execution.verifier.ExecutionVerifier` for locally-run,
single-agent job receipts (`JobRunner`), and the coordinator's own
verifier in `gcon.cluster.coordinator` for HMAC-signed, cluster-issued
receipts. Don't assume one substitutes for the other.

---

## Error Handling

### HTTP status codes actually used by `/api/v1`

| Code | Meaning | Where it comes from |
|------|----------|---|
| `200 OK` | Request succeeded | all `GET`s and successful `POST`s |
| `400 Bad Request` | Invalid input / `ValueError` from the coordinator (duplicate job/workflow id, bad DAG, job not cancellable, etc.) | `submit_job`, `cancel_job`, `submit_workflow` |
| `401 Unauthorized` | Missing, invalid, expired, or under-scoped API key | `require_scope()` |
| `404 Not Found` | Node or job id doesn't exist | `get_node`, `get_job` |
| `422 Unprocessable Entity` | Request body fails Pydantic validation | FastAPI's default, before your handler runs |

There is currently no `201 Created`, `409 Conflict`, or `503 Service
Unavailable` in this router — duplicate-id and "not ready" conditions both
surface as `400`.

### Error response format

FastAPI's default shape, produced by every `HTTPException` in this router:

```json
{ "detail": "Job 'my-job' already exists." }
```

There is no `error`, `status_code`, or `request_id` field in the body —
the status code lives only in the HTTP response line.

---

## Interactive Docs

Once the server is running:

```
http://localhost:8000/api/v1/docs          # Swagger UI
http://localhost:8000/api/v1/redoc         # ReDoc
http://localhost:8000/api/v1/openapi.json  # raw schema
```

These are generated straight from `api_v1.py`'s route/model definitions,
so they're the fastest way to confirm this document against whatever
version of GCON you're actually running.
