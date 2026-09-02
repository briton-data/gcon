# GCON Architecture

## System Overview

GCON (Decentralized Verified GPU Compute Network) is designed to enable verifiable execution of AI workloads on distributed GPU resources.

```
┌─────────────────────────────────────────────────────────────────┐
│                        Customer/Client                           │
│                  (Submits AI Workload)                           │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Job Queue / Scheduler                       │
│              (Matches resources to workload)                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GCON Agent (Provider)                         │
│                  (GPU Provider's Machine)                        │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 1. Workload Executor                                     │   │
│  │    - Detects GPU hardware                                │   │
│  │    - Executes job in sandbox                             │   │
│  │    - Captures stdout/stderr                              │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 2. Metrics Collector                                     │   │
│  │    - GPU utilization                                     │   │
│  │    - CPU usage                                           │   │
│  │    - Memory consumption                                  │   │
│  │    - Execution time                                      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 3. Verification Engine                                   │   │
│  │    - Hash inputs (SHA256)                                │   │
│  │    - Hash outputs (SHA256)                               │   │
│  │    - Generate HMAC signatures                            │   │
│  │    - Create execution proofs                             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 4. Receipt Generator                                     │   │
│  │    - Issue signed receipt                                │   │
│  │    - Store proof of work                                 │   │
│  │    - Return verification package                         │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Execution Receipt                             │
│                                                                   │
│  {
    "receipt_id": "abc123",
    "job_id": "job-001",
    "status": "success",
    "input_hash": "9f86d0...",
    "output_hash": "a665a4...",
    "proof": {
      "gpu": "RTX 4090",
      "runtime_seconds": 120.5,
      "signature": "9e7d3c..."
    }
  }
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Agent (`agent.py`)

**Responsibility:** Execute workloads and collect metrics

**Key Methods:**
- `detect_gpu()`: Identify GPU hardware
- `execute_job()`: Run workload and capture output
- `collect_metrics()`: Record system metrics during execution

**Data Flow:**
```
Job Script → Subprocess Execute → Metrics Collection → Results
```

### 2. Verifier (`verifier.py`)

**Responsibility:** Generate cryptographic proofs and validate receipts

**Key Methods:**
- `hash_data()`: Generate SHA256/SHA512 hash
- `hash_file()`: Hash file contents
- `sign_data()`: Create HMAC signature
- `verify_signature()`: Validate signatures
- `generate_execution_proof()`: Create signed proof
- `validate_proof()`: Verify proof authenticity

**Cryptographic Approach:**
```
Input Data
    ↓
[SHA256] → Input Hash
    ↓
[HMAC-SHA256 with secret] → Signature
    ↓
Verified Proof Package
```

### 3. Receipt Manager (`receipt.py`)

**Responsibility:** Store and manage execution receipts

**Key Methods:**
- `save_receipt()`: Persist receipt to JSON
- `load_receipt()`: Retrieve receipt from storage
- `list_receipts()`: Query receipts (with filtering)
- `delete_receipt()`: Remove receipt

**Storage Format:**
```
receipts/
├── abc123def456.json
├── xyz789uvw456.json
└── ...
```

### 4. Job Runner (`run_job.py`)

**Responsibility:** Orchestrate end-to-end job execution

**Key Methods:**
- `run_job()`: Execute job with full verification pipeline
- `get_job_receipt()`: Retrieve receipt
- `list_job_receipts()`: List all receipts
- `print_receipt()`: Format receipt for display

**Execution Pipeline:**
```
1. Calculate Input Hash
         ↓
2. Execute Job (Agent)
         ↓
3. Collect Metrics
         ↓
4. Calculate Output Hash
         ↓
5. Generate Proof (Verifier)
         ↓
6. Create Receipt
         ↓
7. Store Receipt (Manager)
         ↓
8. Return Complete Result
```

## Data Structures

### ExecutionMetrics
```python
@dataclass
class ExecutionMetrics:
    job_id: str
    gpu_name: str
    gpu_memory_total: int
    gpu_memory_used: int
    cpu_percent: float
    memory_percent: float
    runtime_seconds: float
    timestamp: str
```

### Execution Receipt
```json
{
  "receipt_id": "unique_id",
  "job_id": "job_identifier",
  "agent_id": "agent_identifier",
  "status": "success|failed|error|timeout",
  "input_hash": "sha256_hash",
  "output_hash": "sha256_hash",
  "proof": {
    "job_id": "job_identifier",
    "gpu": "GPU_name",
    "runtime_seconds": 120.5,
    "input_hash": "sha256_hash",
    "output_hash": "sha256_hash",
    "timestamp": "ISO_8601_timestamp",
    "key_id": "which HmacKeyring key signed this, if rotation is in use",
    "attested_node_id": "mTLS-authenticated node identity, only present when the coordinator has one on file for this node (see docs/TRANSPORT_AND_PERSISTENCE.md) -- omitted entirely otherwise, not null",
    "signature": "HMAC_signature"
  },
  "issued_at": "ISO_8601_timestamp"
}
```
There is no `verified` field stored anywhere in this structure — a
receipt's validity is always computed live via
`ExecutionVerifier.validate_proof(receipt["proof"])`, never read off a
stored value. A replicated job's receipt additionally carries a sibling
`execution_proof` field alongside `proof` (not inside the signed payload
— see the redundancy note above); that's specific to
`verify=`-tagged jobs and absent otherwise.

## Security Model

### Threat Model

1. **Provider Dishonesty**
   - Provider claims to have run job but didn't
   - Provider runs on cheaper hardware than advertised
   - Provider returns fraudulent results

2. **Customer Verification**
   - Proof that job ran with claimed metrics
   - Proof that hardware matched specifications
   - Proof of output integrity

### Security Mechanisms

1. **Input/Output Hashing**
   - SHA256 hashing of all inputs
   - SHA256 hashing of all outputs
   - Enables customer to verify output wasn't modified

2. **HMAC Signatures — coordinator-signed, not node-signed**
   - HMAC-SHA256 of proof data, using a coordinator-held key
     (`gcon.execution.hmac_keyring.HmacKeyring`, rotation-capable,
     persisted outside version control — see the module docstring; a
     genuinely random key is generated per deployment, never a
     hardcoded default)
   - **Important distinction, easy to get wrong reading this doc
     alone:** the signature is created by the *coordinator*
     (`GCONCoordinator.verifier`, one instance, one key), not by the
     worker node that actually ran the job. It proves "the coordinator
     recorded this," not "node X cryptographically attests to this" —
     a compromised or dishonest agent can report fabricated results and
     the coordinator will sign them, because nothing here independently
     verifies the *content* the agent reported.
   - What partially closes that gap today: **node-attested receipts**
     (`attested_node_id`, added to the signed payload when the
     coordinator has a real mTLS-authenticated identity on file for
     that node — see `docs/TRANSPORT_AND_PERSISTENCE.md`). This proves
     the result arrived over a connection the coordinator's transport
     layer cryptographically authenticated as that specific node's
     certificate, which is real evidence, but it's still the
     coordinator vouching for that fact via its own key — not the node
     independently signing with its own. True per-node non-repudiation
     (the node's own key signs its own result, checkable by a third
     party without trusting the coordinator's key custody) is not built
     — see [Future Enhancements](#future-enhancements).
   - A previous version of `generate_execution_proof()` also stored a
     static `"verified": true` field on every proof, unconditionally, at
     creation time, before any check had run. It's gone — real
     verification is always `ExecutionVerifier.validate_proof()`,
     recomputed live, never read from a stored field.
   - **Redundancy as a separate, additive check:** replicated-execution
     verification (see [Already Built](#already-built-not-future-anymore))
     is a different mechanism from either of the above — it doesn't
     strengthen any individual signature, it adds independent evidence
     by comparing N separately-dispatched nodes' results. Its own honest
     limit: agreement between colluding or commonly-compromised nodes
     would still look like agreement.

3. **Hardware Attestation (Future)**
   - GPU hardware identification
   - Hardware capability verification
   - Attestation from trusted hardware sources
   - (Node-attested receipts, above, are a real but different thing —
     transport-identity attestation, not hardware attestation)

4. **Timestamp Validation**
   - Receipts must be recent (< 24 hours) — `validate_proof()` rejects
     an otherwise-valid signature if the proof's timestamp is older
     than that
   - Prevents replay attacks
   - Enables temporal verification

## Execution Flow - Detailed

### Step 1: Job Submission

There is no `gcon submit ...` CLI. The real entry points are the
`JobRunner` CLI (single machine, no coordinator) and the cluster path
(coordinator + agents + `/api/v1`):

```bash
# Standalone (this section's pipeline)
python -m gcon.execution.run_job "python train.py" --job-id train-001

# Or, against a running coordinator (see docs/API.md)
curl -X POST -H "Authorization: Bearer $GCON_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"job_id": "train-001", "command": "python train.py"}' \
  http://localhost:8000/api/v1/jobs
```

### Step 2: Provider Agent Setup
```python
agent = GCONAgent(job_id)
gpu_info = agent.detect_gpu()
assert gpu_info['gpu_name'] == 'RTX 4090'
```

### Step 3: Job Execution with Monitoring
```python
result = agent.execute_job("python train.py")
# Agent records:
# - Execution time
# - GPU utilization
# - Memory usage
# - CPU load
# - Output hash
```

### Step 4: Proof Generation
```python
verifier = ExecutionVerifier(secret_key)
proof = verifier.generate_execution_proof(
    job_id=job_id,
    gpu_name=gpu_info['gpu_name'],
    runtime=result['runtime_seconds'],
    input_hash=input_hash,
    output_hash=output_hash,
    metrics=result['metrics']
)
# Verifier signs proof with HMAC
```

### Step 5: Receipt Issuance
```python
receipt = receipt_manager.create_receipt(
    job_id=job_id,
    execution_result=result,
    proof=proof
)
receipt_manager.save_receipt(receipt)
```

### Step 6: Receipt Return to Customer

This `verified` is the live result of calling `validate_proof()` at
response time — computed fresh, not read from anywhere inside the
receipt itself (see the correction under
[Security Mechanisms](#security-mechanisms) above):

```json
{
  "verified": true,
  "proof": {...},
  "timestamp": "2024-01-15T10:30:45Z"
}
```

## Known Limitations

### Coordinator High Availability: Real, Opt-In, With Real Caveats

**This section previously said GCON runs exactly one `GCONCoordinator`
process with no leader election, "not implemented yet." That's no
longer accurate** — `gcon.cluster.leader_election.LeaderElector` and
`scripts/run_coordinator.py --ha` are real, built, and covered in detail
in **[docs/FAILOVER.md](FAILOVER.md)**. Read that document for the full
picture (what it gives you, what it explicitly doesn't, how failover
actually plays out). Summary for this page:

- **Default (no `--ha`):** exactly one coordinator process, as originally
  described below — this default behavior is unchanged.
- **With `--ha`:** N coordinator processes share one control-plane
  database; a SQLite-backed lease (`coordinator_leases` table) determines
  which one is active. The active one runs the gRPC transport and
  dispatches jobs; standbys block (see FAILOVER.md's operational notes —
  a standby doesn't even serve read-only queries today, contrary to what
  you might assume). Failover is lease-expiry-based, not consensus-based:
  there's a real, bounded gap (up to the lease TTL, default 10s) between
  a leader dying and a standby taking over, and it's active-passive, not
  active-active.

**What the original "if the Coordinator process crashes" behavior below
still describes accurately, `--ha` or not:**
- In-flight job recovery on restart/failover is the same
  `restore_from_persistence` path either way — HA doesn't add new
  recovery logic, it just gets a new process to run that same path
  sooner (once it acquires the lease) instead of waiting for a human to
  restart the one coordinator by hand.
- No VIP/load-balancer/DNS failover is included — after a failover,
  something (you) still needs to point agents/dashboard users at the new
  leader's address. See FAILOVER.md.

**Assumptions baked into the current design that hold regardless of
`--ha`:**
- `NodeRegistry`, `Scheduler`, and the in-memory job queue all live in
  one process's memory *at a time* — with `--ha`, that's the current
  leader's memory; a promoted standby starts these fresh, not by
  inheriting the previous leader's live state (there is no live-state
  handoff, only durable-DB continuity).
- Agents dial a single, fixed coordinator gRPC address at a time. There
  is no client-side multi-address failover in `AgentDaemon` — if the
  leader changes, agents need to be pointed at the new address by
  something outside GCON (see FAILOVER.md's VIP/LB note).

**If the (leading, or only) Coordinator process crashes:**
- All *live* scheduling state is lost: `NodeRegistry`'s connected-node
  map, in-flight job assignments, and any node the scheduler considered
  "busy". Agents' gRPC streams drop.
- Jobs that were mid-execution on an agent are not automatically
  recovered or resubmitted by the crashed process (obviously), but *are*
  reconciled by whichever process (the same one restarted, or a new
  leader under `--ha`) next calls `restore_from_persistence`.
- **What continues functioning:** agents keep any job they're actively
  executing until it exits, since job execution runs on the agent, not
  the Coordinator. The control-plane database is untouched.
- **What stops functioning until a coordinator is leading again:** new
  job submission, scheduling, node registration/heartbeat processing,
  the dashboard and `/api/v1`, and any in-progress WebSocket push.
- **On the next coordinator to lead:** it reloads durable node/job/
  receipt records from the control-plane database
  (`restore_from_persistence`), but *live* connections are not restored
  automatically — agents must reconnect and re-register (`register_agent`)
  before they show up as live nodes again.

**Still genuinely not built** (see FAILOVER.md for the fully detailed
version of each):
- Consensus-based election (Raft/etcd-style) — this is lease-based.
- A distributed (non-SQLite-file) control-plane database for real
  cross-host HA — see `docs/TRANSPORT_AND_PERSISTENCE.md`'s SQLite →
  PostgreSQL migration path for what would close this.
- VIP/load-balancer/DNS integration, or client-side agent failover.
- A standby serving read-only dashboard/API traffic (the code path could
  support it; `run_coordinator.py`'s current wiring doesn't call it that
  way today).

## Scalability Considerations

### Current (MVP)
- Single agent per GPU node
- Local receipt storage (JSON)
- Direct verification

### Phase 2 (Network)
- Distributed scheduler
- Provider registry
- Network-level verification

### Phase 3 (Decentralized)
- Blockchain-based receipt anchoring
- Decentralized verification
- Smart contracts for dispute resolution

## Already Built (not "future" anymore)

The sections above describe the original single-agent verification
pipeline in isolation. Since then, a real multi-node cluster layer has
been built around it — this list used to appear under "Future
Enhancements," which was no longer accurate:

- **Coordinator + agent fleet** (`gcon.cluster.coordinator`,
  `gcon.transport`) — mTLS gRPC transport, node registry, scheduler, job
  recovery/restart from the control-plane database on coordinator
  restart.
- **Coordinator high availability** (`gcon.cluster.leader_election`,
  `scripts/run_coordinator.py --ha`) — lease-based failover across
  multiple coordinator processes sharing one control-plane database. See
  [docs/FAILOVER.md](FAILOVER.md) for what this does and does not cover;
  it replaces the single-coordinator-only assumption in
  [Known Limitations](#known-limitations) above when opted into, though
  that section's caveats (bounded failover gap, no VIP/LB, active-passive
  only) still apply.
- **Replicated-execution verification** (`gcon.execution.replication`,
  `submit_job(verify={"replicas": N, "tolerance": ...})`) — dispatches a
  job to N independently-selected nodes and compares their results for
  agreement, with each replica keeping its own independently
  HMAC-signed receipt (`self.replica_receipts`). Surfaced in the
  dashboard's Receipt Inspector as a "Replicated Execution" panel
  (witnesses, agreement, max deviation, mismatches). Not yet exposed via
  the public `/api/v1` — `POST /jobs`'s `JobSubmitRequest` model doesn't
  have a `verify` field yet; only reachable through
  `GCONCoordinator.submit_job()` directly. See
  [Security Model](#security-model) below for what this mechanism does
  and does not prove.
- **Node-attested receipts** — a receipt's signed payload can carry
  `attested_node_id`: the mTLS-authenticated identity
  (`NodeRepository.auth_fingerprint`, set only after
  `grpc_transport.py`'s `Register` handler verifies it against the
  claimed `node_id`) the coordinator has on file for that node.
  `ExecutionVerifier.create_receipt` refuses to sign — raises, doesn't
  silently proceed — if the claimed and attested identities disagree.
  This is real but partial hardware/node attestation, distinct from the
  "Hardware Attestation (Future)" item below — see
  [Security Model](#security-model).
- **Autoscaling** (`gcon.cluster.autoscaler.AutoScaler`) — opt-in
  (`GCON_AUTOSCALE_ENABLED=1`) periodic loop comparing pending jobs to
  idle nodes; only capable of creating real dispatchable nodes under
  `LocalTransport` (in-process/dev), since provisioning a real new
  worker process under a network transport is explicitly not
  implemented (`AutoScaler.scale_up` raises rather than registering a
  node that can't actually receive work). Manual scale up/down via the
  dashboard shares the same `AutoScaler` instance as the automatic loop.
- **REST API** — versioned, API-key-authenticated `/api/v1` (see
  [docs/API.md](API.md)), plus a Python SDK (`gcon_sdk`).
- **WebSocket real-time monitoring** — `/ws` on the dashboard, pushing
  live cluster events to connected browser sessions.
- **Dashboard UI** — session-authenticated web dashboard
  (`gcon.dashboard`) with RBAC (5 roles), audit log, and a Management
  panel for users, API keys, and settings.
- **DAG workflows** — multi-job dependency graphs (`gcon.workflow`),
  submittable via `POST /api/v1/workflows` or the dashboard.

## Future Enhancements

Genuinely not built yet, as of this doc:

1. **Cross-host coordinator HA** — the `--ha` lease election in
   [docs/FAILOVER.md](FAILOVER.md) is real, but it's backed by a single
   SQLite file; safe multi-process HA on one host, not yet safe
   multi-host HA (SQLite's network-filesystem locking isn't reliable
   enough to build that on top of as-is). Needs the Postgres dialect
   (see `docs/TRANSPORT_AND_PERSISTENCE.md`) wired all the way through.
   Also missing: VIP/load-balancer integration, client-side agent
   failover, and a standby actually serving read-only traffic.

2. **Container Support**
   - Docker/Singularity job execution (job commands run as subprocesses
     today, not in a container runtime)
   - Reproducible execution environments

3. **Advanced Verification**
   - Zero-knowledge proofs of correct execution (ruled out for now as
     impractical for real training workloads — proving overhead and the
     floating-point/finite-field mismatch, not just "not gotten to yet")
   - Full hardware attestation (TPM/SGX/confidential-computing GPU
     modes) — distinct from the node-attested receipts already built
     (see [Already Built](#already-built-not-future-anymore) above),
     which bind a receipt to the coordinator's own record of an mTLS
     identity, not to a hardware root of trust
   - True per-node non-repudiation: every receipt today is signed by the
     coordinator's own HMAC key, not by the executing node's own key —
     see [Security Model](#security-model)'s note on this

4. **Performance**
   - Multi-GPU support
   - Distributed training
   - Result caching

5. **Economics**
   - Token system
   - Reputation scoring
   - Dispute resolution

None of the items in this second list should be assumed to exist just
because they're described elsewhere in speculative/marketing material —
check `src/gcon/` for what's actually implemented.
