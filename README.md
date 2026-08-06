# GCON

**A self-hosted job coordinator that gives every execution a signed, verifiable receipt.**

![License](https://img.shields.io/badge/License-MIT-green.svg)
![Python](https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white)
![Version](https://img.shields.io/badge/Version-v0.10-orange)
![Status](https://img.shields.io/badge/Status-Alpha-red)

---

## What it is

GCON runs jobs across machines you control. Agents connect to a coordinator over mTLS, pick up jobs, and run them. Every finished job gets a signed receipt — proof of what ran, where, and with what result. No trusting a log file someone could have edited.

Job coordination and verification live in one system, not bolted together.

---

## Core capabilities

- **Signed execution receipts** — every job run produces a cryptographically signed, independently verifiable receipt
- **Live cluster trust score** — computed continuously from receipt verification + node health
- **mTLS agent fleet** — authenticated, encrypted coordinator-agent transport
- **RBAC & audit log** — 5 roles, real permission checks, real audit trail
- **Job scheduling & recovery** — coordinator schedules jobs, tracks state, recovers from node failure
- **DAG job dependencies** — express multi-step jobs as a dependency graph
- **Real-time dashboard** — live cluster view over WebSocket
- **Public API + Python SDK** — `/api/v1` with API-key auth, `gcon_sdk` for programmatic use

---

## Status

This is active, early-stage development — not a finished product. Some things worth knowing before you dig in:

- Single coordinator, SQLite-backed. No HA/failover yet.
- Autoscaling manages agents within capacity you already have — it doesn't provision cloud infra.
- Workflow retries/timeouts/branching aren't built yet; DAGs work but are basic.

Building all of this out. If you hit rough edges, that's expected at this stage.

---

## Install

```bash
git clone https://github.com/briton-data/GCON.git
cd GCON
pip install -r requirements.txt
```

---

## Quickstart

**1. Generate dev certs**

```bash
python scripts/generate_dev_certs.py --node worker-01
```

**2. Start the coordinator**

```bash
python scripts/run_coordinator.py
```

**3. Start an agent**

```bash
python scripts/run_worker.py
```

**4. Or just run a job directly and see a receipt**

```python
from gcon.execution.run_job import JobRunner

runner = JobRunner(agent_id="example-agent-1")
result = runner.run_job(
    job_script="python -c \"print('hello from GCON')\"",
    job_id="example-job-001",
    timeout=10
)

print(runner.print_receipt(result["receipt"]["receipt_id"], format="summary"))
```

More examples in [`examples/`](examples/).

---

## Structure

```text
src/gcon/       Core package — coordinator, agents, scheduling, workflows, verification
sdk/            Python SDK (gcon_sdk)
scripts/        Entry points: coordinator, agent, cert setup
docs/           Architecture, API, deployment docs
tests/          Test suites
templates/      Dashboard templates
static/         Dashboard CSS/JS
```

---

## Docs

- [Architecture](docs/ARCHITECTURE.md)
- [API Reference](docs/API.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Transport & Persistence](docs/TRANSPORT_AND_PERSISTENCE.md)
- [Quickstart Guide](docs/QUICKSTART.md)

---

## Contributing

Open an issue before big changes. Make sure tests pass before a PR.

---

## License

MIT. See [LICENSE](LICENSE).
