# Workers, enrollment and hosted leases

How a machine becomes a GCON worker, who is allowed to run which customer's jobs on it, and how GCON's own
("hosted") workers are lent to customers one customer at a time. Written for developers working on the coordinator,
the worker, the API or the dashboard. It describes the code as it is, including what it does not do (section 12).

Source map: `src/gcon/transport/grpc_transport.py` (Enroll, Register, Control), `src/gcon/transport/agent_daemon.py`
(the worker), `src/gcon/execution/sanitize.py` (worker clean-up), `src/gcon/cluster/registry.py` (who is
dispatchable), `src/gcon/cluster/scheduler.py` (who gets a job), `src/gcon/cluster/worker_leases.py` (leases),
`src/gcon/persistence/repositories/{enroll_tokens,fleet_nodes,worker_leases,nodes}.py`, migrations 17 and 18 in
`src/gcon/persistence/migrations/registry.py`.

---------------------------------------------------------------------------------------------------------------------

## 1. Vocabulary

| Term | Meaning |
|---|---|
| Coordinator | The GCON server. Holds the scheduler, the certificate authority (CA) and the database. |
| Worker (node) | A machine running the worker daemon (`scripts/run_worker.py`). It executes jobs. |
| Organization (org) | A customer account. Jobs, API keys and customer-owned workers belong to one. |
| Enrollment | One-time step in which a worker presents a token and receives its own certificate. |
| Registration | Each time a worker connects, it registers over mTLS and is added to the live registry. |
| Customer-hosted worker | A worker the customer runs and enrolls itself (section 3). |
| Hosted-fleet worker | A worker GCON runs. It has no organization of its own and serves a customer only while leased (sections 5 and 6). |
| Lease | A customer's exclusive hold on one hosted worker (section 6). |
| Shared pool | The older mechanism: staff-flagged organization-less workers that an organization's job could fall back to. **Has a master switch on the dashboard, OFF by default**; leases are the safer replacement (section 3.3). Hosted-fleet workers were never part of it. |

## 2. The two kinds of worker

| | Customer-hosted | Hosted fleet |
|---|---|---|
| Who runs the machine | The customer | GCON |
| Enrolled with | A per-organization token (`kind='org'`) | An operator-only fleet token (`kind='fleet'`) |
| Organization in the certificate | The customer's | None |
| Who it serves | Its organization, permanently | The organization holding a lease on it, for the lease's duration |
| How the customer gets it | Enrolls it | Requests it through the API (`POST /workers/leases`) |
| Limit that applies | `max_workers` (plan) | `max_leased_workers` (plan) |
| Cleaned between customers | Not needed (one owner) | Always, and verified |
| Sandbox | Required for customer jobs under the default policy | Always required |

Both can be used by one organization at the same time; the two plan limits are independent.

## 3. Enrollment

### 3.1 The flow

1. The worker starts with a token (`--enroll-token` / `GCON_ENROLL_TOKEN`), the coordinator's enroll address
   (`GCON_ENROLL_ADDRESS`) and the CA fingerprint or certificate (`GCON_CA_FINGERPRINT` / `GCON_CA_CERT_B64`).
2. It generates its own private key and a certificate signing request (CSR). The key never leaves the worker.
3. It calls `Enroll` on the **enroll port** (the main gRPC port + 1). This port needs no client certificate; the
   token is the only credential. It is opened only when the coordinator has `GCON_ENROLL_TOKEN` set (section 3.4).
4. The coordinator validates the token and the request (section 3.2), signs the CSR and returns the certificate and
   the CA certificate. Every attempt, accepted or refused, is written to `node_enrollment_audit` with the source
   address and the reason (`GET /nodes/{id}/enrollment-history`).
5. From then on the worker connects to the main port with mutual TLS using that certificate and calls `Register`,
   then opens its `Control` stream.

The organization is read from the **certificate** (its OU), never from anything the worker says. That is what makes
an organization an attested fact. A hosted-fleet worker's certificate has no OU.

### 3.2 What the coordinator checks, in order

1. **Token.** A known, non-revoked token is used; an expired or used-up token is refused as such and never falls
   through to the shared token. An unknown token is tried against the shared `GCON_ENROLL_TOKEN`.
2. **Node name.** `node_id` is required.
3. **Hosted-fleet names are protected.** If the name is already in the hosted fleet, only a fleet token may enroll it.
   Otherwise anyone holding an organization token or the shared token could enroll under a hosted worker's name and
   take over its identity.
4. **No switching owner by name.** A name already recorded with a different organization (or none) cannot be
   enrolled under another. So a customer's worker cannot be turned into a hosted one by name, or the reverse.
5. **Plan cap** (organization tokens only): `max_workers`. Archived workers do not count, and a worker enrolling again
   under its own name is not counted against itself. Hosted-fleet workers never count against any customer.
6. **Signing**, then the token's use is consumed atomically so the last use cannot be taken twice.

### 3.3 Token kinds

| Kind | Created by | Organization | Lifetime / uses | Resulting worker serves |
|---|---|---|---|---|
| `org` | Staff (Administration, "Worker enrollment tokens") or `scripts/create_enroll_token.py` | The one chosen | Dashboard always sets both (defaults 72 h, 5 uses; maximums 720 h, 1000). The script leaves them unlimited unless given `--expires-in-hours` / `--max-uses`. | That organization's jobs |
| `fleet` | Owner/Administrator (Administration, "GCON hosted fleet") | None (stored under the marker `(hosted-fleet)`, never a real org id) | Same figures and limits as above | Nobody until leased |
| Shared `GCON_ENROLL_TOKEN` | Environment variable on the coordinator | None | Never expires, unlimited | Only jobs that have no organization (staff/dev jobs). While the shared pool is off (the default) it cannot run a customer's job at all; with the pool on, only if staff flagged it |

Tokens are stored as hashes. The plaintext is shown once, when created. A fleet token can never be mistaken for an
organization's: `lookup_org_id` ignores `kind='fleet'`, and Enroll reads the kind explicitly so the marker can never
reach a certificate.

The **shared pool** (older mechanism) has a **master switch on the dashboard** (Administration, "Shared worker pool"),
**off by default**. It lets an organization's job fall back, per job, to an organization-less worker that staff have
flagged and that runs in a sandbox, according to the organization's pool mode (`off` / `basic` / `full`). That
duplicates leases with weaker isolation: consecutive jobs of different customers run on the same machine with nothing
wiped between them, which is why it is off.

- **The switch** is stored (`settings.shared_pool_enabled`, `on` / `off`), survives restarts, and is changed by an
  Owner or Administrator (`PUT /management/shared-pool`, audit-logged). `GCON_SHARED_POOL=on` only sets the starting
  value for a deployment that has never stored a choice. A change takes effect on the next job placement (the stored
  value is re-read at most every 2 seconds).
- **While it is off**, no job is placed on a flagged worker, whatever the flags and modes say; they are kept, so
  turning it on restores them. `GET /org/shared-pool` (customer API) reports `enabled: false` and points to leases.
- **Flags and modes** are managed in the same panel: a worker with no organization that is not hosted can be added
  or removed from the pool (`PUT /management/nodes/{id}/shared-pool`); each organization has a mode
  (`PUT /management/organizations/{id}/shared-pool`).
- **Hosted workers can never be flagged**: the route refuses them, and the registry never marks one as shared.

### 3.4 Coordinator and CA persistence

The enroll port opens only if `GCON_ENROLL_TOKEN` is set. This means per-organization tokens work only while the
shared token also exists, and the shared token is itself a valid token for an organization-less worker (section 12).

The CA, signing key and databases live under `GCON_DATA_DIR`, or the attached volume (`RAILWAY_VOLUME_MOUNT_PATH`)
when there is one, and the worker certificate directory under `GCON_TLS_CERT_DIR` or the volume's `keys/grpc`.
**Attach a volume.** Without one, a redeploy creates a new CA, every certificate becomes invalid, and every worker
must enroll again with a valid token.

## 4. Registration, connection and "dispatchable"

A worker is **registered** when its `Register` call succeeds; it is **connected** when its `Control` stream is up.
These are not the same moment: the stream comes up just after registration, and can drop before the heartbeat
timeout notices. The dashboard shows "not connected" in between.

`registry.dispatchable(info)` is the single definition of "idle": a job can really be sent to this worker now. It is
used by the scheduler, `get_idle_nodes` and the idle count. A worker is dispatchable only if all of:

- status is `idle`
- not draining, not quarantined
- not **outdated** (its reported version is below `GCON_MIN_WORKER_VERSION`; unset means no check; a worker reporting
  no readable version counts as below any minimum)
- its control stream is connected (`RemoteNodeProxy.is_connected`)
- **if hosted-fleet:** it has an **active** lease, it runs in a sandbox, and its version is at least
  `FLEET_MIN_WORKER_VERSION` (the first version able to clean itself on request)

The **staff dashboard's** `/nodes` states, per worker, `connected`, `agent_version`, `outdated`, `sandboxed`,
`serves`, `blockers` (why it cannot take a customer's job right now) and `can_run_customer_jobs`. The **customer
API's** `GET /nodes` returns a smaller model (`NodeOut`) that does not include those fields, so today a customer
cannot see why one of its own workers is not taking jobs (listed in `CUSTOMER_SIDE_CARRYOVER.md`). Neither shows a
customer a hosted worker's network address: the customer API's `address` is null for hosted workers.

## 5. The hosted fleet

A worker enrolled with a fleet token is recorded in `fleet_nodes` **by the coordinator at enrollment**. The worker
never declares it, and having no organization does not imply it (the shared token also yields organization-less
workers). Registry rules:

- Only a worker with no organization of its own can be hosted; a customer's own worker is never leasable, whatever
  the table says.
- While leased, the worker's effective organization is the lease holder's. Nothing about its certificate changes; the
  worker never learns who holds it. The coordinator does.
- A hosted worker is never part of the shared pool.
- A fleet worker must report version 1.1.0 or newer (`AGENT_VERSION` in `agent_daemon.py` is 1.1.0; a test keeps the
  two in agreement) and must run jobs in containers (`GCON_EXECUTION_BACKEND=docker`) so it counts as sandboxed.

A fleet worker is **available** when it is connected, idle, sandboxed, new enough, not draining/quarantined, not
held, not being cleaned and has no open lease.

## 6. Leases

### 6.1 Data model (migration 18)

- `fleet_nodes(node_id, enrolled_at, enroll_token_id, held_reason, held_at, sanitizing_since, sanitize_ref)`
- `worker_leases(lease_id, node_id, org_id, state, requested_by, started_at, last_activity_at, ending_at, ended_at,
  end_reason, ended_by, sanitize_result)`; rows are never deleted. They are the record of which customer held which
  worker and when.
- `enroll_tokens.kind` (`org` | `fleet`).
- A partial unique index `uq_worker_leases_node_open ON worker_leases(node_id) WHERE state != 'ended'`. **A worker can
  have at most one open lease, enforced by the database.** Two requests racing for one worker (even from two
  coordinator processes) cannot both win; the loser gets `not_enough_workers`.
- Migration 17 adds `nodes.archived_at` (workers staff have archived; kept for history, not counted against caps).

### 6.2 States

```
 active --release / idle / staff--> ending --no job running--> sanitizing --worker says clean--> ended
    \                                  \                          \
     \__ worker lost too long __________\__________________________\__ clean-up failed / silent / lost --> ended + worker HELD
```

| State | Meaning |
|---|---|
| active | The customer's jobs run on the worker. |
| ending | No new jobs; a job already running is left to finish. |
| sanitizing | The coordinator asked the worker to clean itself and awaits its answer. |
| ended | Over. The worker is back in the pool only if it answered clean; otherwise it is **held**. |

**Held** (`fleet_nodes.held_reason`): a worker lost while leased, or whose clean-up failed, timed out or could not be
requested. It cannot be leased until staff have it cleaned (dashboard "Clean", `POST /management/fleet/{id}/clean`),
and it returns to the pool only when it then answers clean. A hold survives restarts and cannot be cleared by
enrolling again. **A worker is never returned to the pool on the strength of a timeout.**

### 6.3 Operations

- **Request** (`WorkerLeaseManager.request`): all or none, in this order: count 1 to 50 (`bad_request`); the plan
  allows hosted workers at all, i.e. `max_leased_workers > 0`, and `held + count <= cap` where `held` counts every
  lease not yet ended, including ones ending or being cleaned (`plan_limit`); enough workers are available
  (`not_enough_workers`, with the number available). Leases are created in one transaction.
- **Release**: customer, staff, idle timer. Releasing a lease already ending or ended changes nothing. A customer can
  only release its own; any other lease id answers "not found", the same as an id that does not exist.
- **tick()** runs from the coordinator's health loop (about every 3 s) and: moves `ending` to `sanitizing` once the
  worker has no running job; releases leases idle too long (a lease with a running job is never idle); gives up on a
  leased worker lost too long; and holds a worker that has not answered a clean-up in time. Every state change is
  written to the database first, then to the live registry.
- **Restart recovery**: a reconnecting worker is given its lease back at registration (`registry.lease_lookup`).
  A worker that disconnects in the middle of a clean-up never answers that request (it was sent on the old stream);
  the lease then ends with the worker held, when the clean-up timeout or the lost-worker grace passes, whichever
  comes first. A reconnecting worker is not trusted to be clean.

| Setting | Env var | Default |
|---|---|---|
| Idle lease released after | `GCON_LEASE_IDLE_MINUTES` | 60 minutes |
| Wait for a clean-up answer | `GCON_LEASE_SANITIZE_TIMEOUT_SECONDS` | 300 s |
| Leased worker unreachable before the lease is given up | `GCON_LEASE_WORKER_LOST_SECONDS` | 600 s |

Events: `WORKER_LEASE_STARTED`, `WORKER_LEASE_ENDING`, `WORKER_LEASE_ENDED`, `WORKER_HELD`,
`WORKER_RETURNED_TO_POOL`.

### 6.4 The clean-up (`gcon.execution.sanitize.sanitize_host`)

The coordinator sends `CoordinatorEnvelope.sanitize{ref}` down the worker's stream; the worker answers
`AgentEnvelope.sanitize_result{ref, ok, detail}`. The coordinator takes the worker's identity from the stream's
session, never from the message, and ignores an answer whose `ref` it did not ask for.

The worker, only when no job is running (otherwise it refuses and reports not ok):

1. Force-removes every container named `gcon-job-*` (running or stopped). It touches no other container.
2. Empties the job IO directory (`GCON_JOB_IO_ROOT` or the temp dir; run directories and usage/stage reports).
   Files a container running as root left behind, which the worker's own user cannot delete, are removed by a
   short-lived helper container that mounts only that directory and has no network.
3. **Verifies**: no job container remains and the directory is empty. If either is untrue, or Docker cannot be
   asked, it answers not ok with the reason. A worker that cannot verify itself cannot report itself clean.

It does not touch images (the image is the operator's: `GCON_JOB_DOCKER_IMAGE`, default `python:3.12-slim`, never a
customer's) or anything outside the job IO directory.

## 7. Isolation: what is guaranteed and by what

| Guarantee | Enforced by |
|---|---|
| A leased worker runs only its lease holder's jobs | `scheduler.select_node` org filter; the lease sets the worker's effective org; `dispatchable` requires an `active` lease. Both dispatch sites in the coordinator go through `select_node`. |
| An unleased hosted worker runs nothing, not even organization-less jobs | `dispatchable` (no active lease means not dispatchable) |
| An ending or cleaning worker takes no new job | `dispatchable` (state must be `active`) |
| Hosted workers never fall back to per-job sharing | `shared_pool` is never set for a fleet worker; the shared-pool tier of `select_node` needs it |
| One owner at a time | Partial unique index on open leases |
| Customer A cannot release, list or discover customer B's lease | Org-scoped API; identical 404 for "not yours" and "does not exist" |
| A hosted worker's identity cannot be taken over by name | Enroll step 3 |
| A worker is not reused until verified clean | `held` + the verification in 6.4; no timeout path returns a worker to the pool |
| Customers do not learn where a hosted worker lives | Address omitted from an organization's node views |

Tracing a worker suspected of compromise: `GET /management/fleet/{node_id}/leases` lists every organization that has
ever held it, with times and the clean-up result. Receipts and job attempts are recorded against the node id, so
the jobs it ran in each window can be listed from them.

## 8. Customer API (organization API key)

| Call | Scope | Result |
|---|---|---|
| `POST /workers/leases` `{ "count": 1..50 }` | Submit workflows | `200` list of leases; `400 bad_request`; `403 plan_limit`; `409 not_enough_workers` (with `available`) |
| `GET /workers/leases` | View monitoring | This organization's open leases and recently ended ones |
| `DELETE /workers/leases/{lease_id}` | Submit workflows | The lease, now ending/sanitizing; `404 not_found` (also for another organization's id) |

A lease: `lease_id, node_id, state, started_at, ending_at, ended_at, end_reason` (`customer_release`, `idle`,
`worker_lost`, `staff_release`). Error bodies carry a stable `code` next to `detail`. A leased worker also appears in
the customer's `GET /nodes`, without its address. Jobs are submitted exactly as before; they simply find the
customer's leased workers.

### 8.1 The submission gate: no workers, no job

Hosted workers are claimed, never assigned automatically. So a job (`POST /jobs`) or workflow (`POST /workflows`) from
an organization that has nothing that could run it is **refused at submission with `409`**, before anything is
created, with the next step in the body:

```json
{ "detail": "This job cannot run: you have no workers. Your plan lets you hold up to 1 GCON-hosted worker(s) and you hold 0. Request 1 with POST /workers/leases, or enroll your own.",
  "code": "no_workers", "workers_have": 0, "workers_needed": 1,
  "max_leased_workers": 1, "leased_workers_held": 0, "request_workers": "POST /workers/leases" }
```

- **What counts as a worker:** the organization's own workers that are registered and not archived (online or not: a
  briefly offline worker makes the job wait, not fail), plus hosted workers under an **active** lease. A lease that is
  ending or being cleaned runs nothing, so it does not count (but it still counts toward the plan's limit). Busy
  workers count: busy means wait.
- **Codes:** `no_workers`, and `not_enough_workers_to_verify` for a job with `verify` when the organization has fewer
  workers than `verify.replicas` (default 2): "Verification needs 2 workers and you have 1." Verification therefore
  needs at least two workers; on a plan that includes one hosted worker, that means a larger plan or the
  organization's own workers.
- **What the body says to do:** `request_workers` is `POST /workers/leases` when the plan has room (the detail says
  how many to request); it is `null` when the plan is at its limit (the detail says to release one, enroll your own, or
  move to a larger plan) or includes no hosted workers (enroll your own, or move to a larger plan).
- **Order of checks:** request validation first (an invalid command or `verify` spec is a `400`, never a `409`), then
  this gate, then the concurrent-job cap (`429`). A job the workflow engine submits for an already-admitted workflow is
  not gated again. A key with no organization (system key) is never gated.
- **Implementation:** `ManagementLayer.worker_gate` is installed as `coordinator.org_worker_gate_resolver`;
  `GCONCoordinator.submit_job` raises `WorkersRequiredError`; the API maps it to `409`. A coordinator without a
  `ManagementLayer` (internal use, most unit tests) has no gate.

## 9. Staff (dashboard and `/management/...`, permission "Manage cluster" unless noted)

| Where / call | What |
|---|---|
| Administration, Node Management | Per worker: status, "not connected", version, who it serves, why it cannot take customer jobs; **Archive** (offline workers) and **Archive all offline** (`POST /management/nodes/{id}/archive`, `/archive-offline`). An online worker, or one with an open lease, cannot be archived. |
| Administration, Worker enrollment tokens | Mint, list, revoke organization tokens (`/management/enroll-tokens`; Owner or Administrator). |
| Administration, GCON hosted fleet | Fleet counts; each hosted worker's state (available, leased, ending, cleaning, held, unavailable) and why; **Release** a lease (`POST /management/leases/{id}/release`); **Clean** a held worker; recent leases; mint a fleet token (`POST /management/fleet/tokens`; Owner or Administrator). `GET /management/fleet`. |
| Administration, Shared worker pool | The master switch (Owner or Administrator), the workers that could be in the pool with Add / Remove, and each organization's mode. |
| Administration, GCON hosted fleet, per worker | **History**: every customer that has held that worker (`GET /management/fleet/{id}/leases`). |
| Plans table | `max_workers` and `max_leased_workers` per plan (`Max workers`, `Max hosted workers`). |
| Events, Notifications, Incidents, Audit Logs | Clear / dismiss controls (below). |

Clearing: notifications can be deleted (one, or all that are read); resolved incidents deleted (one or all; open
incidents never); the event feed hides what was shown (events are kept, because other code reads them); old audit
entries can be deleted by an Owner only ("older than N days"), and that action is itself recorded.

Plan starting values (placeholders for staff to set): `max_workers` Standard 5, Pro 25, Enterprise 100;
`max_leased_workers` Standard 1, Pro 4, Enterprise 20. Verification needs two workers, so on Standard (one hosted
worker) it needs the organization's own workers or a larger plan.

## 10. Configuration reference

| Variable | Where | Effect |
|---|---|---|
| `GCON_ENROLL_TOKEN` | Coordinator | Opens the enroll port; also the shared organization-less token (legacy). Worker side: the token to present. |
| `GCON_ENROLL_ADDRESS`, `GCON_CA_FINGERPRINT`, `GCON_CA_CERT_B64`, `GCON_ENROLL_INSECURE` | Worker | Where and how to enroll and verify the coordinator. |
| `GCON_MIN_WORKER_VERSION` | Coordinator | Workers below it stay visible but get no jobs. Unset = off. |
| `GCON_SHARED_POOL` | Coordinator | Starting value of the shared-pool switch for a deployment that has never stored a choice. After that, the dashboard switch decides (section 3.3). Unset or `off`: off. |
| `GCON_SANDBOX_POLICY` | Coordinator | `required` (default): customer jobs only on sandboxed workers. `trusted`: operator accepts unsandboxed workers. Hosted workers always need a sandbox. |
| `GCON_EXECUTION_BACKEND=docker` | Worker | Makes the worker sandboxed. |
| `GCON_JOB_IO_ROOT`, `GCON_JOB_DOCKER_IMAGE` | Worker | Job IO directory; the container image (operator's). |
| `GCON_LEASE_IDLE_MINUTES`, `GCON_LEASE_SANITIZE_TIMEOUT_SECONDS`, `GCON_LEASE_WORKER_LOST_SECONDS` | Coordinator | Section 6.3. |
| `GCON_DATA_DIR`, `GCON_TLS_CERT_DIR`, `RAILWAY_VOLUME_MOUNT_PATH` | Coordinator | Where the CA and databases live (section 3.4). |

## 11. Runbooks

**Add a hosted worker.** Dashboard, Administration, GCON hosted fleet: create a fleet token (keep lifetime short, uses
equal to the machines you are adding). On the machine: Docker installed, `GCON_EXECUTION_BACKEND=docker`, the token,
enroll address and CA details, then start `scripts/run_worker.py`. It appears as "available" once connected. If it is
not, the row says why.

**Add a customer's own worker.** Administration, Worker enrollment tokens: pick the organization, accept or change
the figures, create the token and give it to the customer once.

**A held worker.** Open it in the fleet table. If it is connected, press Clean; it returns to the pool only if it
answers clean. If cleaning fails again, read the reason, fix the machine (often files or containers it cannot
remove), and retry, or rebuild the machine.

**A suspected compromised hosted worker.** Release its lease if one is open; do not return it to the pool. Pull its
history (`/management/fleet/{id}/leases`) to know which customers to tell and for which windows. Rebuild the machine
and enroll it again.

**Retire a worker.** Make sure it has no open lease, stop it, then Archive it once it shows offline.

**After a redeploy that lost the CA.** Every worker must enroll again. Prevent this by attaching the volume.

## 12. Known limits and open decisions

- **Shared pool.** Kept, with a dashboard switch that is off by default (section 3.3). Whenever it is turned on, the
  weaker isolation above applies to the workers flagged in it. Nothing forces a customer who is on a leased plan away
  from it while it is on; leaving it off is what keeps the per-job sharing out.
- **Self-hosting is still allowed** and the Standard plan's `max_workers` starting value (5) invites it. Set it to 0
  to make self-hosting a paid-plan feature; that is a plan number, not code.
- **A customer who claims a worker and goes idle for the idle time (60 minutes by default) loses it**, and their next
  job gets the `409` above. That is intended; the message must say so clearly. A longer idle time for small plans is a
  setting (`GCON_LEASE_IDLE_MINUTES`), not per plan today.
- **The shared `GCON_ENROLL_TOKEN`** never expires, has no use limit and yields organization-less workers, and the
  enroll port needs it set. Decision pending on making it opt-in/development-only.
- **A worker compromised during a lease exposes that customer's data from that lease.** Leases limit exposure to one
  customer at a time and make it traceable; they do not make a compromised machine safe.
- **GPU memory** is not separately scrubbed or verified by the clean-up; it relies on the container exiting.
- **Windows** hosted workers are untested: the clean-up uses the Docker CLI.
- **Version string.** Workers report a constant (`AGENT_VERSION`). Bump it when worker behavior that GCON depends on
  changes, and raise `GCON_MIN_WORKER_VERSION` or `FLEET_MIN_WORKER_VERSION` accordingly.
- **Placeholders** for staff to confirm: plan limits (section 9) and token defaults (72 h, 5 uses).
- **Multiple coordinators.** The lease unique index is shared and protects against two coordinators leasing the same
  worker; the per-process lock only reduces contention. The health-loop timers (`tick`) run on each coordinator.

## 13. Tests

`tests/cluster/test_worker_leases.py` (availability, request limits, isolation, lifecycle, held workers, restarts),
`tests/execution/test_sanitize_host.py` (clean-up and its failure modes),
`tests/transport/test_fleet_enroll_and_sanitize.py` (fleet enrollment, name protection, clean-up round trip over real
gRPC), `tests/transport/test_enroll_worker_cap.py` (plan cap), `tests/api/test_worker_lease_routes.py` (customer API),
`tests/management/test_fleet_admin.py` and `test_worker_enrollment_controls.py` (staff controls, tokens, archive),
`tests/management/test_clear_dashboard_noise.py` (clear/dismiss), `tests/api/test_worker_gate.py` (the submission gate:
refusal bodies, what counts as a worker, verification, validation order), `tests/cluster/test_shared_pool.py`,
`tests/api/test_shared_pool_route.py` and `tests/management/test_shared_pool_switch.py` (the pool, its dashboard
switch, and that it is off by default), `tests/test_codemap_current.py`
(`docs/CODEMAP.md` is current).

Test files that are about something other than worker availability set `coordinator.org_worker_gate_resolver = None`
in their setup, so the gate does not apply to them; the gate's own tests do not.
