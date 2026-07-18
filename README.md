# GCON

**A distributed execution platform that cryptographically verifies GPU compute.**

![License](https://img.shields.io/badge/License-MIT-green.svg)
![Python](https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white)
![Version](https://img.shields.io/badge/Version-v0.10-orange)
![Execution](https://img.shields.io/badge/Execution-Verifiable-brightgreen)

---

## Overview

GPU compute marketplaces let customers rent capacity from third-party providers, but they offer no way to confirm that a workload actually ran, ran correctly, or ran on the hardware that was promised. Customers are left trusting the provider's word.

GCON is a coordination layer for distributed GPU workloads that replaces that trust with proof. Each job is scheduled onto an agent, executed, and closed out with a signed receipt that captures what ran, where, and with what result. Consumers of the network can verify these receipts independently, without re-running the job or trusting the provider's infrastructure.

The platform handles the coordination problems that come with running compute across untrusted, distributed nodes: scheduling work onto available agents, tracking job and workflow state, recovering from node failure, and scaling capacity up or down as demand changes. Verification is built into that lifecycle rather than bolted on afterward.

GCON is designed to be used as infrastructure — embedded into a larger platform, a marketplace, or an internal compute fabric — rather than as an end-user product. It is not built on blockchain consensus; verification is cryptographic and can be checked by any party with the relevant public keys.

---

## Why GCON

Distributed compute introduces a basic trust gap: the party paying for a job has no independent way to confirm it happened as claimed. This matters more as compute moves off centralized, audited infrastructure and onto distributed provider networks, spot capacity, and third-party hardware.

GCON closes that gap with verifiable execution: every job produces cryptographic evidence that can be checked after the fact, independent of the provider that ran it. Coordination and verification are treated as one system, so trust is a property of the execution itself, not a separate audit layer.

---

## Core Capabilities

| Capability | Description |
|---|---|
| **Verified execution** | Every job produces a signed receipt binding the job, its result, and the executing agent. |
| **Job coordination** | A central coordinator schedules, dispatches, and tracks jobs across the agent network. |
| **Workflow orchestration** | Multi-step jobs are expressed as DAGs with dependency-aware execution. |
| **Distributed agents** | Agents register with the network, accept work, and report execution state. |
| **Elastic scaling** | Capacity adjusts to load through autoscaling of the agent pool. |
| **Fault recovery** | Node and job failures are detected and recovered without operator intervention. |
| **Resource monitoring** | Cluster and node-level metrics are collected for observability. |
| **Artifact & receipt storage** | Execution artifacts and receipts are persisted and retrievable for later verification. |

---

## Installation

```bash
git clone https://github.com/Jug-data/GCON.git
cd GCON

pip install -r requirements.txt
```

---

## Quick Start

Run the bundled demo, which starts a coordinator, registers agents, submits jobs, and launches the web dashboard:

```bash
python -m gcon.dashboard.dashboard_server
```

For programmatic use, see [Example Usage](#example-usage) below.

---

## Example Usage

```python
from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent

coordinator = GCONCoordinator()
agent = GCONAgent("node-001")

coordinator.submit_job("job-001", "train_model")
```

Submitting a job schedules it onto an available agent and, on completion, produces a signed execution receipt. See [`docs/API.md`](docs/API.md) for the full coordinator, agent, and verification API.

Runnable examples covering multi-step workflows and framework-specific jobs are available in [`examples/`](examples/).

---

## Repository Structure

```text
src/gcon/       Core package: coordinator, agents, scheduling, workflows,
                verification, storage, and the management/dashboard API
docs/           Architecture, API reference, and setup documentation
tests/          Unit and integration test suites
scripts/        Operational and maintenance scripts
templates/      Server-rendered dashboard templates
static/         Dashboard front-end assets (CSS/JS)
```

---

## Documentation

Detailed documentation lives in [`docs/`](docs/):

- [Architecture](docs/ARCHITECTURE.md) — system design and component responsibilities
- [API Reference](docs/API.md) — coordinator, agent, and management API details
- [Quickstart Guide](docs/QUICKSTART.md) — a deeper walkthrough beyond this README

---

## Contributing

Contributions are welcome. Please open an issue to discuss significant changes before submitting a pull request, and ensure the test suite passes locally before requesting review.

---

## License

MIT License. See [LICENSE](LICENSE) for details.
