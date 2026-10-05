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

**Running code safely**

- **Sandboxed execution.** Jobs submitted through the API only ever run inside isolated containers, on workers that have proven they can do that. You can't ask for, and never get, a less isolated run.
- **Critical answers.** Use `verify={"replicas": 2}` for results you can't afford to get wrong.
- **Act on verified results.** Trust a result only when `receipt["assurance"]["level"]` is `"verified"`. For a `verify` job the job itself also says so: `job["verification"]["outcome"]` is `"agreed"`, `"disputed"` or `"unavailable"`. A disputed job still has status `completed` and still carries the first replica's output, so check it before using the result.

Operating your own workers or coordinator? See [Sandboxed workers](docs/WORKER_SANDBOX.md) and [Deployment](docs/DEPLOYMENT.md).

Full detail: [SECURITY.md](SECURITY.md).

---

## Get started

<<<<<<< HEAD
GCON is cloud-managed: GCON runs the coordinator, and you can optionally connect machines of your own as workers. The steps below run the whole thing locally, which is how developers try it out.

Works the same on Linux, macOS and Windows. You need Python 3.12+ (on Linux and macOS, use `python3` wherever this page says `python`).
=======
GCON's Python components support Python 3.12+ on Linux, macOS, and Windows. Production worker deployments should be validated on the target OS and execution backend.
>>>>>>> 8f19b8c74316257a33102a6fdaead1deba6ddbcc

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

submitted = client.submit_job("hello-1", "echo hello from GCON")   # "hello-1" is just your label
job_id = submitted["job_id"]                                       # GCON generates the real id
job = client.get_job(job_id)
print(job["status"], job["output"])        # completed  hello from GCON
```

GCON generates the job's id and returns it; use that id to fetch, cancel or retry the job. The label you pass in is stored as the job's `client_reference`, so you can match jobs to your own records and search by it (`client.list_jobs(client_reference="hello-1")`).

A job goes `pending` → `running` → `completed` (or `failed`). Once finished it has a `receipt_id`.

Prefer the command line? On Linux, macOS or Git Bash:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs -H "Authorization: Bearer <your api key>" -H "Content-Type: application/json" -d '{"client_reference":"hello-1","command":"echo hello from GCON"}'
# the response contains the job_id GCON generated; use it here:
curl http://127.0.0.1:8000/api/v1/jobs/<job_id> -H "Authorization: Bearer <your api key>"
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
train = client.submit_job("train-1", "python train.py",
                          kind="resourced", requires={"gpu": True, "min_vram_gb": 12})

# Run on 2 machines and compare answers (for jobs where a wrong result is costly).
# The replicas agree only if their output is identical; "tolerance" just flags a
# large runtime difference between them and never decides agreement.
calc = client.submit_job("calc-1", "python critical_calc.py",
                         verify={"replicas": 2, "tolerance": 0.02})

# Cancel or retry
client.cancel_job(train["job_id"])
client.retry_job(calc["job_id"])

# Chain jobs: "train" starts only after "fetch" succeeds.
# Inside a workflow, job_id is just a label for depends_on; the response maps
# each label to the id GCON generated.
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
