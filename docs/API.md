# GCON API Reference

Complete guide to the GCON REST API, Python SDK, and verification library.

---

## Table of Contents

1. [REST API Overview](#rest-api-overview)
2. [Authentication](#authentication)
3. [Endpoints](#endpoints)
4. [Python SDK](#python-sdk)
5. [Verification API](#verification-api)
6. [Error Handling](#error-handling)
7. [Rate Limiting & Quotas](#rate-limiting--quotas)

---

## REST API Overview

**Base URL:**
```
http://localhost:8000/api/v1
```

**Protocol:** HTTP/REST

**Response Format:** JSON

**API Version:** v1

---

## Authentication

### Development (Local)

```bash
# No authentication required for local development
# All API calls work without credentials
curl http://localhost:8000/api/v1/cluster
```

### Production (Future)

```bash
# Bearer token (JWT)
curl -H "Authorization: Bearer gcon_YOUR_API_KEY" \
  http://api.gcon.io/api/v1/cluster
```

**Getting an API Key:**
1. Log in to the dashboard
2. Go to Management → API Keys → Create Key
3. Choose scopes:
   - `Read` (monitoring, read-only)
   - `Submit` (submit/cancel jobs)
   - `Admin` (scale, deregister agents)
4. Copy the secret (shown only once)

---

## Endpoints

### Cluster Information

#### `GET /cluster`

Get overall cluster state.

**Response:**
```json
{
  "status": "healthy",
  "coordinator_online": true,
  "total_nodes": 4,
  "idle_nodes": 2,
  "busy_nodes": 2,
  "offline_nodes": 0,
  "total_capacity": 16,
  "available_capacity": 8,
  "queued_jobs": 0,
  "running_jobs": 4,
  "completed_jobs": 127,
  "failed_jobs": 3
}
```

---

### Jobs

#### `POST /jobs`

Submit a new job.

**Request:**
```json
{
  "job_id": "my-job-001",
  "command": "python train.py --epochs 10",
  "timeout_seconds": 300,
  "tags": ["training", "v1"]
}
```

**Response (201 Created):**
```json
{
  "job_id": "my-job-001",
  "status": "pending",
  "command": "python train.py --epochs 10",
  "created_at": "2026-07-20T13:31:23Z",
  "assigned_agent": null,
  "started_at": null,
  "completed_at": null
}
```

**Errors:**
- `400 Bad Request` — Missing required fields or invalid job_id
- `409 Conflict` — Job with this ID already exists
- `503 Service Unavailable` — Coordinator not ready

---

#### `GET /jobs`

List all jobs (optionally filtered).

**Query Parameters:**
- `status` (optional): `pending`, `running`, `completed`, `failed`
- `limit` (default: 100)
- `offset` (default: 0)

**Request:**
```bash
GET /jobs?status=running&limit=50
```

**Response:**
```json
{
  "total": 127,
  "limit": 50,
  "offset": 0,
  "jobs": [
    {
      "job_id": "my-job-001",
      "status": "completed",
      "command": "python train.py",
      "created_at": "2026-07-20T13:31:23Z",
      "started_at": "2026-07-20T13:31:25Z",
      "completed_at": "2026-07-20T13:35:50Z",
      "assigned_agent": "gpu-1",
      "exit_code": 0,
      "artifacts_count": 3
    }
  ]
}
```

---

#### `GET /jobs/{job_id}`

Get a specific job.

**Response:**
```json
{
  "job_id": "my-job-001",
  "status": "completed",
  "command": "python train.py",
  "created_at": "2026-07-20T13:31:23Z",
  "started_at": "2026-07-20T13:31:25Z",
  "completed_at": "2026-07-20T13:35:50Z",
  "assigned_agent": "gpu-1",
  "exit_code": 0,
  "artifacts": [
    { "path": "model.pkl", "size_bytes": 524288, "hash": "sha256:abc..." },
    { "path": "metrics.json", "size_bytes": 1024, "hash": "sha256:def..." }
  ]
}
```

**Errors:**
- `404 Not Found` — Job does not exist

---

#### `DELETE /jobs/{job_id}`

Cancel a running job.

**Response (200 OK):**
```json
{
  "job_id": "my-job-001",
  "status": "cancelled",
  "cancelled_at": "2026-07-20T13:40:00Z"
}
```

**Errors:**
- `404 Not Found` — Job does not exist
- `400 Bad Request` — Job is already completed/failed (can't cancel)

---

### Workflows

#### `POST /workflows`

Submit a multi-step workflow (DAG of tasks).

**Request:**
```json
{
  "workflow_id": "training-pipeline-v1",
  "tasks": [
    {
      "task_id": "download",
      "command": "python download_data.py",
      "depends_on": []
    },
    {
      "task_id": "preprocess",
      "command": "python preprocess.py",
      "depends_on": ["download"]
    },
    {
      "task_id": "train",
      "command": "python train.py",
      "depends_on": ["preprocess"]
    },
    {
      "task_id": "evaluate",
      "command": "python evaluate.py",
      "depends_on": ["train"]
    }
  ]
}
```

**Response (201 Created):**
```json
{
  "workflow_id": "training-pipeline-v1",
  "status": "running",
  "created_at": "2026-07-20T13:31:23Z",
  "tasks": [
    {
      "task_id": "download",
      "status": "running",
      "depends_on": [],
      "job_id": "training-pipeline-v1/download"
    }
  ]
}
```

**Errors:**
- `400 Bad Request` — Invalid DAG (cycle detected, orphaned task, missing dependency)
- `409 Conflict` — Workflow with this ID already exists

---

#### `GET /workflows`

List all workflows.

**Query Parameters:**
- `status` (optional): `pending`, `running`, `completed`, `failed`
- `limit` (default: 100)

**Response:**
```json
{
  "total": 23,
  "workflows": [
    {
      "workflow_id": "training-pipeline-v1",
      "status": "completed",
      "created_at": "2026-07-20T13:31:23Z",
      "completed_at": "2026-07-20T13:50:00Z",
      "task_count": 4,
      "completed_tasks": 4
    }
  ]
}
```

---

#### `GET /workflows/{workflow_id}`

Get workflow details and task status.

**Response:**
```json
{
  "workflow_id": "training-pipeline-v1",
  "status": "completed",
  "created_at": "2026-07-20T13:31:23Z",
  "completed_at": "2026-07-20T13:50:00Z",
  "tasks": [
    {
      "task_id": "download",
      "status": "completed",
      "job_id": "training-pipeline-v1/download",
      "depends_on": [],
      "started_at": "2026-07-20T13:31:25Z",
      "completed_at": "2026-07-20T13:32:50Z",
      "exit_code": 0
    }
  ]
}
```

---

### Receipts (Execution Proofs)

#### `GET /receipts`

List all receipts.

**Query Parameters:**
- `agent_id` (optional): Filter by agent
- `limit` (default: 100)

**Response:**
```json
{
  "total": 127,
  "receipts": [
    {
      "receipt_id": "receipt-001",
      "job_id": "my-job-001",
      "agent_id": "gpu-1",
      "status": "completed",
      "timestamp": "2026-07-20T13:35:50Z",
      "exit_code": 0,
      "signature_valid": true
    }
  ]
}
```

---

#### `GET /receipts/{receipt_id}`

Get a specific receipt (the signed proof).

**Response:**
```json
{
  "receipt_id": "receipt-001",
  "job_id": "my-job-001",
  "agent_id": "gpu-1",
  "command": "python train.py",
  "status": "completed",
  "exit_code": 0,
  "timestamp": "2026-07-20T13:35:50Z",
  "stdout": "Epoch 1: loss=0.523\nEpoch 2: loss=0.412\n...",
  "stderr": "",
  "artifacts": [
    {
      "path": "model.pkl",
      "size_bytes": 524288,
      "hash": "sha256:abc123..."
    }
  ],
  "signature": "-----BEGIN RSA SIGNATURE-----\nMIGEAoGBAJR8...\n-----END RSA SIGNATURE-----",
  "public_key": "-----BEGIN PUBLIC KEY-----\nMIGfMA0GCS...\n-----END PUBLIC KEY-----"
}
```

---

#### `POST /receipts/{receipt_id}/verify`

Verify a receipt offline (check signature, timestamp).

**Request:**
```json
{
  "public_key": "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----"
}
```

**Response:**
```json
{
  "receipt_id": "receipt-001",
  "valid": true,
  "signature_valid": true,
  "timestamp_valid": true,
  "checks": [
    { "check": "signature", "passed": true },
    { "check": "timestamp_recent", "passed": true },
    { "check": "job_id_binding", "passed": true },
    { "check": "agent_id_binding", "passed": true }
  ]
}
```

**Errors:**
- `400 Bad Request` — Public key format invalid
- `404 Not Found` — Receipt not found

---

### Nodes (Agents)

#### `GET /nodes`

List all agents in the cluster.

**Response:**
```json
{
  "total": 4,
  "nodes": [
    {
      "agent_id": "gpu-1",
      "hostname": "worker-1.gcon.internal",
      "status": "idle",
      "capacity": 4,
      "running_jobs": 0,
      "completed_jobs": 42,
      "failed_jobs": 1,
      "last_heartbeat": "2026-07-20T13:40:05Z",
      "cpu_percent": 5.2,
      "memory_percent": 12.4
    }
  ]
}
```

---

#### `GET /nodes/{agent_id}`

Get details for a specific agent.

**Response:**
```json
{
  "agent_id": "gpu-1",
  "hostname": "worker-1.gcon.internal",
  "status": "idle",
  "capacity": 4,
  "running_jobs": 0,
  "completed_jobs": 42,
  "failed_jobs": 1,
  "last_heartbeat": "2026-07-20T13:40:05Z",
  "public_key": "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----",
  "registered_at": "2026-07-20T10:00:00Z"
}
```

---

#### `DELETE /nodes/{agent_id}`

Deregister an agent (graceful shutdown, no new jobs assigned).

**Response (200 OK):**
```json
{
  "agent_id": "gpu-1",
  "status": "deregistering",
  "deregistered_at": "2026-07-20T13:40:10Z"
}
```

**Errors:**
- `404 Not Found` — Agent not found
- `400 Bad Request` — Agent already deregistered

---

### Events (Live Updates)

#### `GET /events`

Poll for recent events.

**Query Parameters:**
- `limit` (default: 50)
- `since` (optional): ISO 8601 timestamp

**Response:**
```json
{
  "events": [
    {
      "event_id": "evt-001",
      "type": "job.submitted",
      "timestamp": "2026-07-20T13:31:23Z",
      "job_id": "my-job-001",
      "details": {
        "command": "python train.py"
      }
    }
  ]
}
```

---

#### `GET /stream`

Server-Sent Events for real-time updates (low-latency dashboard).

**Request:**
```bash
curl -H "Accept: text/event-stream" http://localhost:8000/api/v1/stream
```

**Response Stream:**
```
data: {"type":"job.submitted","job_id":"my-job-001","timestamp":"2026-07-20T13:31:23Z"}

data: {"type":"job.assigned","job_id":"my-job-001","agent_id":"gpu-1","timestamp":"2026-07-20T13:31:25Z"}
```

---

## Python SDK

### Installation

```bash
pip install gcon-sdk
# or, from the repo:
cd sdk && pip install -e .
```

### Basic Usage

```python
from gcon_sdk import GconClient

client = GconClient(api_key="gcon_dev", base_url="http://localhost:8000")

# Cluster info
print(client.get_cluster())
print(client.list_nodes())

# Submit a job
client.submit_job("job-001", "echo hello")
job = client.get_job("job-001")

# Cancel a job
client.cancel_job("job-001")

# List jobs
jobs = client.list_jobs(status="completed", limit=50)

# Workflows
workflow = {
    "workflow_id": "pipeline-v1",
    "tasks": [
        {"task_id": "step-1", "command": "python a.py", "depends_on": []},
        {"task_id": "step-2", "command": "python b.py", "depends_on": ["step-1"]}
    ]
}
client.submit_workflow(workflow)

# Receipts
receipts = client.list_receipts()
receipt = client.get_receipt("receipt-001")

# Verify receipt
is_valid = client.verify_receipt(receipt)
print(f"Valid: {is_valid}")
```

### Error Handling

```python
from gcon_sdk import GconClient, GconAPIError

client = GconClient(api_key="gcon_dev")

try:
    client.submit_job("dup-id", "echo hi")
    client.submit_job("dup-id", "echo hi")  # Duplicate
except GconAPIError as e:
    print(f"Error {e.status_code}: {e.detail}")
```

---

## Verification API

### Standalone Verification (No Coordinator)

```python
from gcon.verification import ReceiptVerifier
import json

receipt = json.load(open("receipt.json"))
verifier = ReceiptVerifier()
is_valid = verifier.verify(receipt)

if is_valid:
    print(f"Receipt is valid")
    print(f"  Job: {receipt['job_id']}")
    print(f"  Agent: {receipt['agent_id']}")
else:
    print(f"Receipt failed verification")
```

---

## Error Handling

### HTTP Status Codes

| Code | Meaning |
|------|----------|
| `200 OK` | Request succeeded |
| `201 Created` | Resource created |
| `400 Bad Request` | Invalid input |
| `404 Not Found` | Resource not found |
| `409 Conflict` | Resource already exists |
| `500 Internal Server Error` | Server error |
| `503 Service Unavailable` | Service down |

### Error Response Format

```json
{
  "error": "ConflictError",
  "detail": "Job 'my-job' already exists.",
  "status_code": 409,
  "request_id": "req-123456"
}
```

---

## Interactive API Docs

Once the dashboard is running, access interactive Swagger UI:

```
http://localhost:8000/api/v1/docs
```