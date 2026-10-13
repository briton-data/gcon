<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/logo-white.svg">
    <img alt="GCON" src="assets/logo.svg" width="320">
  </picture>
</h1>

<p align="center">
GCON helps compute operators coordinate workloads, capture execution evidence, and verify what happened—so they can investigate failures, establish trust, and make operational decisions with greater confidence.
</p>

<p align="center">
  <a href="#the-problem">The problem</a> ·
  <a href="#what-gcon-does">What GCON does</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="#what-a-receipt-looks-like">Receipts</a> ·
  <a href="#status">Status</a>
</p>

---

## The problem

When a job runs on a machine you do not operate, you get an output and a log line. Nothing ties that output to the exact command that ran, the machine that ran it, or the limits it ran under. If a result is wrong, or a machine misbehaves, there is no record to check.

GCON attaches that record to the result. Every successful job comes back with a receipt: the hash of the command and of the output, the node that ran it, runtime and resource measurements, and signatures. GCON re-checks those signatures each time you read a receipt and reports a single verdict.

## What GCON does

| Capability | What it does | Availability |
|---|---|---|
| **Orchestration** | Queues jobs, picks a suitable free worker, retries when a worker drops, and chains jobs into workflows. You say what to run; GCON decides where. | Available |
| **Execution** | Runs the command on a worker node and measures runtime, CPU, memory and GPU use. A job can require a GPU, GPU memory or CPU cores. | Available |
| **Verification** | For jobs where a wrong answer is costly, runs the same job on several independent nodes and compares the results. | Optional |
| **Evidence and receipts** | Records every successful run as a stored receipt: hashes of the command and output, the node, timing and signatures. | Automatic |
| **Assurance** | Combines the signature checks, the node's own signature, policy limits and replica agreement into one verdict per receipt. | Included with each receipt |
| **Proof** | Independent proof of what happened on a job. | Not available yet |

Jobs submitted through the API are only dispatched to workers that declare sandboxed execution, and the receipt records the execution backend the node reports. Each organization sees only its own jobs and receipts. Connections between GCON components are encrypted and mutually authenticated.

## Quick start

You need an account and an API key. You can work from the website or from a terminal.

### From the website

1. Choose **Get Started** at [gcon.network](https://gcon.network) and create your account.
2. Open **Submit Job**. Enter a job name and the command to run. Optionally set minimum GPU memory, minimum CPU cores, or the number of verification workers.
3. Choose **Submit job**, then open the job to follow its status.
4. Open **Evidence** to read the receipt and the checks for each successful job.

Create an API key under **API Keys** when you want to use the API or the SDK.

### From a terminal

Set your service URL once:

```bash
export GCON_URL="<your GCON service URL>"
```

**1. Create an account and API key**

```bash
curl -X POST $GCON_URL/api/v1/auth/signup \
  -H "Content-Type: application/json" \
  -d '{"org_name":"Acme","name":"Ann","email":"ann@acme.example","password":"<a strong password>"}'
```

The response contains your API key. It is shown once, so save it.

```bash
export GCON_KEY="gcon_..."
```

**2. Submit a job**

```bash
curl -X POST $GCON_URL/api/v1/jobs \
  -H "Authorization: Bearer $GCON_KEY" \
  -H "Content-Type: application/json" \
  -d '{"client_reference":"hello-1","command":"echo hello from GCON"}'
```

The response contains the `job_id` GCON generated. Use it to follow the job:

```bash
curl $GCON_URL/api/v1/jobs/<job_id> -H "Authorization: Bearer $GCON_KEY"
```

A job goes `pending`, then `running`, then `completed` or `failed`. A successful job has a `receipt_id`.

**3. Read the receipt**

```bash
curl $GCON_URL/api/v1/receipts/<receipt_id> -H "Authorization: Bearer $GCON_KEY"
```

### With the Python SDK

```bash
pip install -e sdk/
```

```python
from gcon_sdk import GconClient

client = GconClient(api_key="gcon_...", base_url="<your GCON service URL>")

submitted = client.submit_job("hello-1", "echo hello from GCON")  # "hello-1" is your own label
job = client.get_job(submitted["job_id"])
print(job["status"], job["output"])

receipt = client.get_receipt(job["receipt_id"])
print(receipt["assurance"]["level"])    # "verified"
print(receipt["assurance"]["reasons"])  # what drove the verdict
```

## What a receipt looks like

Abridged. Values are illustrative.

```json
{
  "receipt_id": "a1b2c3d4e5f6a7b8",
  "job_id": "…",
  "agent_id": "node-id",
  "status": "completed",
  "issued_at": "2026-10-13T09:15:02+00:00",
  "input_hash": "<hash of the command>",
  "output_hash": "<hash of the output>",
  "output": "hello from GCON",
  "verified": true,
  "proof": {
    "algorithm": "HMAC-SHA256",
    "gpu": "…",
    "runtime_seconds": 0.42,
    "timestamp": "2026-10-13T09:15:02+00:00",
    "metrics": { "cpu_percent": 12.0, "memory_percent": 41.5 },
    "signature": "<signature>"
  },
  "worker_attestation": {
    "verified": true,
    "node_id": "node-id",
    "job_spec_hash": "<hash>",
    "attempt_id": "…",
    "execution_backend": "docker",
    "signature": "<node's own Ed25519 signature>",
    "public_key_pem": "…"
  },
  "execution": {
    "node_id": "node-id",
    "created_at": "…",
    "completed_at": "…",
    "status": "completed"
  },
  "assurance": {
    "level": "verified",
    "assured": true,
    "reasons": ["…"],
    "signals": {
      "crypto": { "applicable": true, "passed": true },
      "worker_attestation": { "applicable": true, "passed": true },
      "policy": { "applicable": true, "passed": true },
      "replication": { "applicable": false, "passed": null }
    }
  }
}
```

Receipts also carry policy results, replica summaries, artifacts, usage and a cost estimate where they apply.

### The assurance verdict

Each receipt gets exactly one level. Only `verified` means every applicable check passed.

| Level | Meaning |
|---|---|
| `verified` | Every applicable check passed. |
| `policy_violation` | The run exceeded the policy limits. |
| `attestation_mismatch` | The node's own signature did not check out. |
| `disputed` | Replicas of the same job gave different results. |
| `invalid` | GCON's signature on the receipt failed. |

Act on a result only when `receipt["assurance"]["level"]` is `"verified"`.

## More job types

```python
# Needs a GPU: only runs on a worker that has one
client.submit_job("train-1", "python train.py",
                  kind="resourced", requires={"gpu": True, "min_vram_gb": 12})

# Run on 2 machines and compare results
client.submit_job("calc-1", "python critical_calc.py", verify={"replicas": 2})

# Chain jobs: "train" starts only after "fetch" succeeds
client.submit_workflow("wf-1", jobs=[
    {"job_id": "fetch", "command": "python fetch.py"},
    {"job_id": "train", "command": "python train.py", "depends_on": ["fetch"]},
])

client.cancel_job(job_id)
client.retry_job(job_id)
```

For a replicated job, replicas agree only if their output is identical. A disputed job still has status `completed`, so check `job["verification"]["outcome"]` (`"agreed"`, `"disputed"` or `"unavailable"`) before using the result.

## Status

Alpha. SDK version 1.0.0. Interfaces may change.

Proof, independent proof of what happened on a job, is not available yet. Nothing on your jobs depends on it. Every successful job still has a receipt and checks. Today a receipt is signed evidence of what the node reported, and GCON re-checks it on every read.

## Documentation

[API reference](docs/API.md) · [Python SDK](sdk/README.md) · [Security](SECURITY.md)

## Security

Found a vulnerability? Follow [SECURITY.md](SECURITY.md) and do not open a public issue.

## Contact

contact@gcon.network

## License

See [LICENSE](LICENSE).
