# GCON Python SDK

Official Python client for the [GCON](../README.md) Public API (`/api/v1`).

## Install

From this directory:

```bash
pip install -e .
```

Or just copy `gcon_sdk/` into your project — its only dependency is `requests`.

## Getting an API key

1. Log in to the GCON dashboard.
2. Go to **Management → API Keys → Create Key**.
3. Choose scopes (`View monitoring` for read-only, `Submit workflows` to submit/cancel jobs).
4. Copy the secret shown — it's only displayed once.

## Usage

```python
from gcon_sdk import GconClient

client = GconClient(api_key="gcon_...", base_url="http://localhost:8000")

# Read cluster state
print(client.get_cluster())
print(client.list_nodes())
print(client.get_health())

# Submit and track a job
client.submit_job("job-42", "python train.py")
print(client.get_job("job-42"))

# Cancel it
client.cancel_job("job-42")

# Workflows, receipts, artifacts
print(client.list_workflows())
print(client.list_receipts())
print(client.list_artifacts())
```

Use it as a context manager to close the underlying HTTP session automatically:

```python
with GconClient(api_key="gcon_...") as client:
    print(client.list_jobs())
```

## Error handling

All non-2xx responses raise `gcon_sdk.GconAPIError`, which carries `.status_code` and `.detail`:

```python
from gcon_sdk import GconClient, GconAPIError

client = GconClient(api_key="gcon_...")
try:
    client.submit_job("dup-id", "echo hi")
    client.submit_job("dup-id", "echo hi")  # duplicate job_id
except GconAPIError as e:
    print(e.status_code, e.detail)  # 400 "Job 'dup-id' already exists."
```

## API reference

Full interactive docs (Swagger UI) are available at `{base_url}/api/v1/docs`,
and the raw OpenAPI schema at `{base_url}/api/v1/openapi.json`.
