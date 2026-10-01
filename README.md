# GCON

**GCON is a cloud-managed platform that runs your jobs on a fleet of machines and gives you signed proof of what actually ran.**

![License](https://img.shields.io/badge/License-MIT-green.svg)
![Python](https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white)

---

## What is GCON?

You give GCON a command. It picks a machine, runs it, and hands back the result **plus a receipt**: a small, tamper-evident record of what ran, where, for how long, and what came out. Anyone can check the receipt's signature later, so you don't have to just trust a log file.

One machine runs the **coordinator** (the brain). Every other machine runs a **worker** and connects to it. That's the whole shape.

---

## Core capabilities

GCON does five jobs, in order:

| | What it does | In plain words |
|---|---|---|
| **Orchestration** | Queues jobs, picks the best free worker, retries and recovers when a worker drops, chains jobs into workflows | *Who runs it, and what happens if something breaks* |
| **Execution** | Runs the command on the worker and measures runtime, CPU, memory and GPU use | *The actual work* |
| **Verification** | Checks the result. For important jobs, runs it on several machines and compares their answers | *Did we get the right answer?* |
| **Evidence & receipts** | Turns every run into a signed, stored receipt: hashes of the command and output, the machine, the timing | *The paper trail* |
| **Proof & assurance** | Checks the signatures (GCON's and the worker's own), the policy limits and the replica agreement, then gives **one clear verdict** | *Can I trust this run, yes or no?* |

The assurance verdict is one of: `verified`, `policy_violation`, `attestation_mismatch`, `disputed`, `invalid`. Only `verified` means every check passed.

**And also:** encrypted, mutually-authenticated connections (a machine can't pretend to be another), separate organizations that can't see each other's jobs, roles and an audit log, a backup coordinator that takes over if the main one dies, a live dashboard, and a REST API plus Python SDK.

---

## Security and trust

GCON is built for work where you need to *prove* what happened.

**What GCON protects**

- **Connections.** Every link between a worker and the coordinator is encrypted and mutually authenticated. A machine can't pose as another.
- **Results.** Each receipt is signed by the coordinator, and by the worker with its own key (covering the job, the command, the output and the result). Change anything afterwards and the check fails.
- **Separation.** Each organization only sees its own jobs, workers and receipts.
- **Critical answers.** Run a job on several machines and GCON compares their answers before you trust the result.

**Hardening for sensitive jobs**

1. Turn on sandboxing on each worker (needs Docker installed there): set `GCON_EXECUTION_BACKEND=docker` in the worker's environment. Job containers have **no network by default**; a job that needs the internet (e.g. downloading a dataset) needs `GCON_JOB_DOCKER_NETWORK=bridge`. If you do that on a cloud host, also block the instance-metadata endpoint (`iptables -I DOCKER-USER -d 169.254.169.254 -j DROP`), which otherwise hands the host's cloud credentials to job code; `host` networking is refused. Job containers run with no Linux capabilities, no privilege escalation and a 512-process cap (`GCON_JOB_DOCKER_PIDS_LIMIT`, `-1` for unlimited), see only their own report-file directory rather than the host's `/tmp`, and run as the image's default user; set `GCON_JOB_DOCKER_USER=1000:1000` to run as non-root if your image and job allow it. By default the coordinator only hands jobs to sandboxed workers (`GCON_SANDBOX_POLICY=required`); a worker without sandboxing gets none. If every job on your deployment is your own, set `GCON_SANDBOX_POLICY=trusted` on the coordinator to allow unsandboxed workers. For a worker that must run unsandboxed (only with `GCON_SANDBOX_POLICY=trusted`), start it as root on Linux/macOS and set `GCON_JOB_RUN_AS_USER=<an unprivileged account>`: jobs then run as that user and cannot read the worker's private keys. That protects the worker's identity only; it is not a sandbox. Set a variable with `export NAME=value` on Linux and macOS, or `$env:NAME="value"` in PowerShell.
2. Never expose the coordinator directly. Put it behind TLS (`GCON_FORCE_HTTPS=1` or a reverse proxy).
3. Encrypt the disk that holds the coordinator's `data/` folder.
4. Use `verify={"replicas": 2}` for results you can't afford to get wrong.
5. Act on a result only when `receipt["assurance"]["level"]` is `"verified"`. For a `verify` job the job itself also says so: `job["verification"]["outcome"]` is `"agreed"`, `"disputed"` or `"unavailable"`. A disputed job still has status `completed` and still carries the first replica's output, so check it before using the result.
6. Keep certificates and signing keys private (file permissions `0600`).

Full detail: [SECURITY.md](SECURITY.md).

---

## Get started

GCON's Python components support Python 3.12+ on Linux, macOS, and Windows. Production worker deployments should be validated on the target OS and execution backend.

```bash
git clone https://github.com/briton-data/GCON.git
cd GCON
pip install -r requirements.txt
pip install -e sdk/
```

### 1. Create certificates

Machines talk over encrypted, authenticated connections, so each worker needs a certificate.

```bash
python scripts/generate_dev_certs.py --cert-dir keys/grpc --node worker-01
```

### 2. Start the coordinator

```bash
python scripts/run_coordinator.py
```

Leave it running. It serves the dashboard and API at `http://127.0.0.1:8000`, and accepts workers on port `50051`.

### 3. Create your account and API key

In a second terminal:

```python
import requests

r = requests.post("http://127.0.0.1:8000/api/v1/auth/signup", json={
    "org_name": "Acme", "name": "Ann",
    "email": "ann@acme.example", "password": "correct-horse-1",
})
info = r.json()
print("org_id:", info["organization"]["org_id"])
print("api key:", info["api_key"]["secret"])   # shown once, save it
```

### 4. Start a worker

A worker only runs jobs for the organization it belongs to, so pass your `org_id`:

```bash
python scripts/run_worker.py --node-id worker-01 --coordinator localhost:50051 --cert-dir keys/grpc --org-id <your org_id>
```

Add more workers the same way (a new `--node-id` and certificate each). For a worker on another machine, copy `ca.cert.pem`, `agent-<node-id>.cert.pem` and `agent-<node-id>.key.pem` into its cert folder and point `--coordinator` at the coordinator's address.

---

## Submit a job

```python
from gcon_sdk import GconClient

client = GconClient(api_key="gcon_...", base_url="http://127.0.0.1:8000")

client.submit_job("hello-1", "echo hello from GCON")
job = client.get_job("hello-1")
print(job["status"], job["output"])        # completed  hello from GCON
```

A job goes `pending` → `running` → `completed` (or `failed`). Once finished it has a `receipt_id`.

Prefer the command line? On Linux, macOS or Git Bash:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs -H "Authorization: Bearer <your api key>" -H "Content-Type: application/json" -d '{"job_id":"hello-1","command":"echo hello from GCON"}'
curl http://127.0.0.1:8000/api/v1/jobs/hello-1 -H "Authorization: Bearer <your api key>"
```

---

## Get your proof

Every finished job has a receipt. Ask for it and read the verdict:

```python
receipt = client.get_receipt(job["receipt_id"])
print(receipt["assurance"]["level"])       # "verified"
print(receipt["assurance"]["reasons"])     # why
```

---

## More job types

```python
# Needs a GPU: only runs on a worker that has one
client.submit_job("train-1", "python train.py",
                  kind="resourced", requires={"gpu": True, "min_vram_gb": 12})

# Run on 2 machines and compare answers (for jobs where a wrong result is costly).
# The replicas agree only if their output is identical; "tolerance" just flags a
# large runtime difference between them and never decides agreement.
client.submit_job("calc-1", "python critical_calc.py",
                  verify={"replicas": 2, "tolerance": 0.02})

# Cancel or retry
client.cancel_job("train-1")
client.retry_job("calc-1")

# Chain jobs: "train" starts only after "fetch" succeeds
client.submit_workflow("wf-1", jobs=[
    {"job_id": "fetch", "command": "python fetch.py"},
    {"job_id": "train", "command": "python train.py", "depends_on": ["fetch"]},
])
```

Full SDK guide: [sdk/README.md](sdk/README.md). Interactive API docs: `http://127.0.0.1:8000/api/v1/docs`.

---

## Project layout

```text
src/gcon/     The core: coordinator, workers, scheduling, verification
sdk/          Python client (gcon_sdk)
scripts/      Start the coordinator and workers, make certificates
docs/         Architecture, API, deployment, failover
tests/        Test suites
```

## Docs

[Architecture](docs/ARCHITECTURE.md) · [API](docs/API.md) · [Deployment](docs/DEPLOYMENT.md) · [Failover](docs/FAILOVER.md) · [Quickstart](docs/QUICKSTART.md) · [SDK](sdk/README.md)

## Contributing & security

Open an issue before big changes and make sure tests pass. See [CONTRIBUTING.md](CONTRIBUTING.md). Found a vulnerability? See [SECURITY.md](SECURITY.md), and please don't open a public issue for it.

## License

MIT. See [LICENSE](LICENSE).
