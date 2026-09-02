# GCON

**A self-hosted job coordinator that gives every execution a signed, verifiable receipt.**

![License](https://img.shields.io/badge/License-MIT-green.svg)
![Python](https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white)
![Version](https://img.shields.io/badge/Version-v0.10-orange)
![Status](https://img.shields.io/badge/Status-Alpha-red)

---

## What it is

GCON lets you run jobs on machines you own or rent — your own GPUs, a friend's spare workstation, a fleet of rented servers — and get proof of what actually happened, not just a log file you have to take someone's word for.

Here's the shape of it: one machine runs the **coordinator**. Every other machine runs an **agent** and connects to the coordinator, ready to take work. You submit a job — a command to run — and the coordinator hands it to a free machine. When the job finishes, that machine's result gets hashed and signed, producing a **receipt**: a small, tamper-evident record of what ran, on what hardware, for how long, and what came out. Anyone holding that receipt can check the signature themselves and know it wasn't altered after the fact.

For work where a wrong answer actually matters, you can ask GCON to run the same job on several machines at once and compare their answers before trusting the result — agreement across independent machines is much harder to fake than a single machine's say-so.

Connections between machines are encrypted and mutually authenticated, so a machine can't show up pretending to be one it isn't. If the coordinator itself goes down, you can run a backup coordinator that takes over automatically. Everything — job history, receipts, which machines are online — survives a restart; nothing lives only in memory.

There's a live dashboard to watch it all happen, and an API if you'd rather script it.

It's honest about what it isn't yet, too: it doesn't prove a specific machine physically ran your job the way a notarized document would — it proves the coordinator's own signature vouches for the record, which is real protection but not the strongest guarantee possible. Where that distinction matters for what you're building, [ARCHITECTURE.md](docs/ARCHITECTURE.md) spells it out in full.

---

## Core capabilities

- **Signed receipts for every job** — proof of what ran, where, and with what result, checkable by anyone who has the receipt
- **Receipts tied to the machine that ran them** — a receipt can be bound to the specific machine's verified identity, not just its say-so
- **Run-it-twice verification** — for jobs where a wrong answer is costly, run it on multiple machines and require agreement before trusting the result
- **A live trust score** for the whole cluster, based on real verification and machine health, not a static badge
- **Encrypted, authenticated connections** between every machine and the coordinator — a machine can't impersonate another
- **A backup coordinator** can take over automatically if the main one goes down
- **Roles and an audit log** — who can do what, and a record of who did what
- **Automatic recovery** — if a machine drops out mid-job, the coordinator notices and handles it
- **Multi-step jobs** — chain jobs together with dependencies between them
- **Scale up or down automatically** within the capacity you already have
- **A live dashboard** to watch the cluster in real time
- **An API and Python client** if you'd rather script it than click through a dashboard

Technical specifics — exact fields, flags, and function names — live in [docs/](docs/), not here.

---

## Status

This is active, early-stage development — not a finished product. Some things worth knowing before you dig in:

- There's one main coordinator by default. A backup can take over if it dies, but only if both are running on the same machine — not yet across separate machines. See [FAILOVER.md](docs/FAILOVER.md).
- Auto-scaling only works in local test setups right now — it won't spin up real new machines on a real deployment yet, and says so honestly instead of pretending to.
- A receipt's signature comes from the coordinator, not independently from the machine that ran the job — real protection, but not the strongest possible guarantee. See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for the honest version.
- Multi-step jobs work, but retries, timeouts, and branching logic for them aren't built yet.
- Not everything you can do through Python is available through the web API yet — the run-it-twice verification feature, for one. See [API.md](docs/API.md).

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

**1. Generate dev certs** (`--cert-dir` is required — this issues the CA plus one cert per `--node`)

```bash
python scripts/generate_dev_certs.py --cert-dir certs --node worker-01
```

**2. Start the coordinator**

Point it at the same cert directory via `GCON_TLS_CERT_DIR` (or pass
`--db`/`--data-dir` for where the control-plane database lives — see
`scripts/run_coordinator.py --help` and its module docstring for the
full environment-variable list, including `--ha` for coordinator
failover):

```bash
GCON_TLS_CERT_DIR=certs python scripts/run_coordinator.py
```

This starts the mTLS gRPC transport (default `0.0.0.0:50051`) and the
dashboard/API (default `127.0.0.1:8000`) in the same process.

**3. Start an agent**

Every identifying value is a real required flag or env var — there's no
hardcoded node identity to edit in the script itself:

```bash
python scripts/run_worker.py \
  --node-id worker-01 \
  --coordinator localhost:50051 \
  --cert-dir certs
```

(`--node-id` and `--coordinator` are required — the script exits with a
clear error if either is missing and `GCON_NODE_ID`/
`GCON_COORDINATOR_ADDRESS` aren't set instead. `--org-id`,
`--hostname`, and repeatable `--capability KEY=VALUE` flags are optional
— see `scripts/run_worker.py --help`.)

**4. Or just run a job directly and see a receipt**

No coordinator or agent needed for this path — a single-machine,
standalone execution + verification pipeline:

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

More examples in [`examples/`](examples/). Full multi-node walkthrough:
[QUICKSTART.md](docs/QUICKSTART.md).

---

## Structure

```text
src/gcon/       Core package — coordinator, agents, scheduling, workflows, verification
sdk/            Python SDK (gcon_sdk) — see sdk/README.md
scripts/        Entry points: coordinator, agent, cert setup
docs/           Architecture, API, deployment, failover docs
tests/          Test suites
templates/      Dashboard templates
static/         Dashboard CSS/JS
```

---

## Docs

- [Architecture](docs/ARCHITECTURE.md)
- [API Reference](docs/API.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Coordinator Failover (HA)](docs/FAILOVER.md)
- [Transport & Persistence](docs/TRANSPORT_AND_PERSISTENCE.md)
- [Quickstart Guide](docs/QUICKSTART.md)
- [Python SDK](sdk/README.md)

---

## Contributing

Open an issue before big changes. Make sure tests pass before a PR. See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Security

Found a vulnerability? See [SECURITY.md](SECURITY.md) — please don't open a public issue for it.

---

## License

MIT. See [LICENSE](LICENSE).
