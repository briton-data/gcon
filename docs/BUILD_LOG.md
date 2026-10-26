# GCON build log

What was built, what was deliberately left out, and why. Newest decisions last in each section.
Statements here come from the decisions recorded in chat; checks marked **(verified)** were run against the code on the date given.

## Built

- **Customer API (`/api/v1`)**: signup/login with a session key, API keys, jobs (submit/cancel/retry/clear), receipts, workflows, nodes, cluster, health, telemetry, account plan, billing reads, change password, delete account. 36 routes.
- **Plans** (Standard / Pro / Enterprise), set from the staff dashboard: label, API request rate, concurrent-job cap, allowed programs, runtime limit. Starting numbers are placeholders to confirm.
- **Job command filters** (2026-10-12): cheap check at the door, worker fails fast if the program is missing, allowed programs per plan, runtime limit per plan. Rule: an invalid job is rejected and recorded nowhere.
- **Distributed correctness**: durable attempts, retry cap, restart recovery, stale-attempt fencing, idempotency key on `POST /jobs`, per-node Ed25519 worker attestation, assurance synthesis, Postgres backend for the control plane.
- **Shared worker pool and self-enrollment** (2026-10-10): verified-TLS enrollment, pool flag per node, per-org pool mode changed only by a logged-in Owner/Administrator.
- **Customer site (`gcon-web`)**: landing, auth, jobs, submit, job detail, API keys, settings (profile, plan, password, delete account), workers, usage & billing.
- **Billing (2026-10-15)**: receipts billed only when their signature verifies, periods must have ended and cannot overlap, own-worker GPU rate, total rounded once with an explicit adjustment line, automatic monthly **draft** invoices, staff dashboard controls.
- **Load tool**: `benchmarks/benchmark.py`.

## Deliberately deferred

| Item | Why |
|---|---|
| VIP / load balancer in front of coordinators | deployment work, outside the code; see "Running two coordinators" |
| Password-reset email | no SMTP provider yet (`GCON_SMTP_*` unset), so reset links are not delivered |
| Persisting the coordinator CA | needs a volume; workers re-enroll after every redeploy |
| Real payment provider | Stripe is a stub; the mock provider marks invoices paid without moving money |
| Customer-created worker enroll tokens | tokens are issued by GCON staff; the Workers page says so |
| Customer roles | customer accounts have no roles, so customer self-serve pool-mode changes stay off |
| Assurance counters across restarts | the Assure counters reset when the coordinator restarts |
| Artifact persistence | the artifact registry is in memory |
| Cryptographic proof (ZK/TEE) | not implemented; receipts are HMAC + per-node Ed25519 signatures |

## v1 contract freeze: checklist (2026-10-16)

Freeze means: v1 only gets additive changes; anything breaking goes to `/api/v2`.

**Fix before freezing**
- [ ] 16 routes declare no response schema and the spec declares no bearer security scheme **(verified)**
- [ ] `command` is described as "Shell command" but the filter allows one allowed program plus arguments, and `python <anything>` passes **(verified)**; decide and document the meaning
- [ ] `docs/API.md` omits 11 existing routes (`/auth/*`, `/jobs/clear`, attempts, retry, enrollment-history, telemetry) **(verified)**
- [x] `docs/DEPLOYMENT.md` said Postgres is not wired; corrected (2026-10-16)
- [x] Postgres billing: `GET /billing/invoices*` failed (`ORDER BY rowid`) and fractional `unit_price_cents` was stored as 0. Fixed by migration 16 (exact `unit_price`, `position`); tested on PostgreSQL 16 (2026-10-16)
- [x] Production persistence: databases, certificate authority and signing key now follow an attached Railway volume automatically; startup warning and a dashboard banner when they don't (2026-10-16). **Still to do by you: attach the volume**

**Decide now (cheap while additive, costly later)**
- [ ] List routes return bare arrays with no paging beyond `limit`
- [ ] Errors: `detail` is a string, except validation errors (an array); no machine-readable `code`
- [ ] Rate-limit headers exist only on 429 (`Retry-After`)
- [ ] Mark `/artifacts`, `/workflows` and proof fields as experimental in the spec
- [ ] Whether customers get `POST` to mint worker enroll tokens in v1 or v2

**Then freeze**
- [ ] Commit `openapi-v1.json` and a contract test that fails on removed fields, renamed fields or changed types

**Also open from earlier rounds (found by re-reading this log against the code, 2026-10-16)**
- [ ] Plan numbers (request rate, concurrent jobs, runtime, allowed programs) are placeholders; `/account/plan` exposes them, so confirm them before they become promises
- [ ] Job spec is not durable: the `jobs` table has no columns for `kind`, `verify`, `requires`, `stages` or `dataset_artifacts` **(verified)**, so replicated jobs fail on restart and those fields can be absent after one
- [ ] `recover_jobs` only watches a replicated job's first node **(verified)**: a non-first replica dying triggers no recovery
- [x] A runner thread that died on an unexpected exception left its job "running" forever while the worker showed idle **(reproduced, then fixed 2026-10-16)**: both runner threads are now wrapped; the job ends as `failed` with the reason (and a job already in a final state is never overwritten)
- [x] **(fixed 2026-10-23)** "Idle" is defined three ways: `get_idle_nodes()` counts draining and quarantined workers; `get_idle_node_count()` does not; neither considers the sandbox policy, so a worker that cannot take customer jobs (no sandbox) shows idle while jobs sit pending. The reason is in the Scheduler tab's last failure message. Now one definition (`registry.dispatchable`: idle, connected, not draining, not quarantined, not outdated) is used by the scheduler, `get_idle_nodes` and the idle count, and each worker states why it cannot take customer jobs (`blockers`, `serves`) on the Administration tab
- [ ] A job completes with NO receipt when receipt creation throws (best-effort by design); the Evidence coverage number is the only signal
- [ ] Receipts signed before a lost signing key can never be checked again; they now report "signed with a key this coordinator no longer has" instead of "Invalid signature". Back up the signing key file (`GCON_HMAC_KEY_PATH`)
- [x] **(fixed 2026-10-23: a worker shows "not connected" and gets no job until its stream is up)** Register can finish before the worker's Control stream is up ("registered" is not "connected"); flagged earlier, never fixed
- [ ] `Idempotency-Key`: a replay with a different request body silently returns the original job, and keys never expire (deliberately not built; decide before freezing)
- [ ] A job queued with no worker online has no queue timeout and no customer-facing reason; only a "waiting for a worker" label exists
- [ ] Leftovers from 2026-10-10: per-org pool metering, docker image addresses, a `gcon-worker` / terminal CLI. **Done 2026-10-23:** enroll-token expiry and use limit (staff mint tokens in Administration; starting figures 72 h and 5 uses, placeholders to confirm), tier cap on enrolled workers (`max_workers` per plan, placeholders 5 / 25 / 100, enforced at Enroll), worker version check (`GCON_MIN_WORKER_VERSION`; workers currently report a hardcoded 1.0.0). Also: archive offline workers (migration 17), and clear/dismiss for notifications, resolved incidents, the event feed and old audit entries. **Still open:** the legacy shared `GCON_ENROLL_TOKEN` enrolls workers with no organization and never expires
- [ ] No unified assurance badge in the staff dashboard (the `assurance` field exists on receipts)

## Running two coordinators (high availability), as of 2026-10-16

Design: active-passive. N coordinators share one control-plane database; one holds a lease and dispatches, the rest wait. A standby serves nothing (not even the dashboard) until it becomes leader.

What works: two processes on **one host** with `--ha`, sharing one data dir (SQLite). That gives restart-without-outage, nothing more.

What does **not** work yet across hosts (each **verified**):
- **The lease was not safe on Postgres: FIXED (2026-10-16).** Two standbys racing for an expired lease both believed they were leader in about 90% of rounds (132-141 of 150, PostgreSQL 16). The update is now a compare-and-swap (and the first acquisition `ON CONFLICT DO NOTHING`); 3 x 150 races afterwards gave exactly one leader every time, and the tests fail on the old code.
- **The identity database is SQLite only** (`GCON_DB_PATH`, default `data/gcon.db`): organizations, users, API keys, sessions, audit. A second coordinator on another host has none of them. `GCON_CONTROL_PLANE_DATABASE_URL` covers only the control plane.
- **Signing and certificate material is generated locally**: the receipt HMAC key (`GCON_HMAC_KEY_PATH`, default `./keys/hmac_secret.key`) and the worker CA (`keys/grpc`). A second coordinator makes its own, so receipts signed by one fail verification on the other and workers' certificates stop validating.
- **No standby serving, no leader flag in `/health`, no VIP or load balancer, and workers dial a single address.**
- Railway attaches a volume to one service instance, so on Railway today there is effectively one coordinator.

To get real cross-host HA: guard the lease update (compare-and-swap on holder and term, handle the insert race); move the identity database to Postgres; load the HMAC key, CA key and enroll token from shared secrets; expose leadership in `/health` and route through a load balancer; give workers a failover address list; make the job spec durable.

## v2 agenda (not v1 blockers)

- **Worker allocation (leases): BUILT (2026-10-23)**, described in full in `docs/WORKERS_AND_ENROLLMENT.md`. GCON hosts a fleet (workers enrolled with an operator-only fleet token, no organization); a customer on a plan with `max_leased_workers` > 0 requests N workers (`POST /workers/leases`, all or none), keeps those exact workers until it releases them, goes idle (60 min) or loses them, and they run that customer's jobs and nobody else's. A partial unique index allows one open lease per worker. At the end of a lease the worker is told to clean itself (`Sanitize`), verifies it, and returns to the pool only if it answers clean; a failed, silent or lost worker is HELD until staff have it cleaned. Customer-hosted workers (enroll with an organization token) are unchanged. Staff: fleet panel in Administration, per-worker lease history for tracing. **Done 2026-10-24:** the old shared pool is kept but has a master switch on the staff dashboard, off by default and stored (`GCON_SHARED_POOL` is only the starting value); its worker flags and organization modes are managed in the same panel, and a hosted worker can never be flagged; Standard now includes 1 hosted worker; a job (or workflow) with no worker to run it is refused at submission with a `409` that says what to do; verification needs 2 workers (`not_enough_workers_to_verify`); `GET /account/plan` shows the worker limits. **Still open / decisions:** (1) none left for the pool: it stays available behind the switch; (2) the shared `GCON_ENROLL_TOKEN` (never expires, organization-less) and the enroll port needing it; (3) there is deliberately no "pool capacity" endpoint (it would tell customers the fleet size; a refused request reports how many are free); (4) GPU memory is not scrubbed or verified; (5) Windows hosted workers untested; (6) plan numbers and token defaults are placeholders.
- **What "verified" means**: today the Verify pillar and `receipt.verified` mean only "the receipt signature is intact", which the coordinator checks against its own key, so every job that finishes is "verified". The intended meaning is independent replicated execution (`assurance.level`: `verified` / `disputed`), which exists but is only for jobs submitted with `verify`. A disputed job still reports status `completed` and `verified: true` **(verified)**. Needs: separate integrity, attestation and verification everywhere they are shown; count assurance outcomes in the Verify pillar; say why a verify job is waiting (it needs N free workers at once; with fewer it waits silently) **(verified)**; replicas on independent owners, not only the customer's own workers; a way to compare non-deterministic output. **Done 2026-10-22 (staff dashboard Verify pillar only)**: Verify now counts replicated-execution outcomes (verified = replicas agreed, disputed, awaiting replicas, no verdict, completed with verify not requested); the receipt-signature count moved to the Evidence pillar as "signature not confirmed". Everything else in this list is still open, including the other places that show `receipt.verified` as "verified".
- Where the work comes from: a git repository/ref instead of a bare command string; other languages and runtimes; environment and secrets.
- Customer roles and scoped keys for account-level actions.
- Envelope responses with paging; one error shape with codes.
- Real payment provider and invoice delivery.
- Performance: the single coordinator process tops out around 120 jobs/s on the benchmark, and SQLite lock contention is the first limit.

- **Codebase inventory (2026-10-24):** `docs/CODEMAP.md` is generated from the code (`python scripts/gen_codemap.py`) and `tests/test_codemap_current.py` fails when it is stale. After changing a route, table, migration, environment variable, event, plan limit, permission, dashboard tab, test file or doc, regenerate it. `docs/CUSTOMER_SIDE_CARRYOVER.md` is the hand-written guide for wiring the customer site to the backend and must be updated whenever a customer-facing route, field or error changes.
- **Trust Center redesign (2026-10-26):** the page now explains the score (50% receipt integrity, 30% workers online, 20% platform health, the weights read from one place, `HealthService.TRUST_WEIGHTS`, so the page and the formula cannot drift), shows "--" and "No receipts yet" instead of a 100% that means nothing, separates **receipt signatures** ("Signatures valid") from **replica verification** (verified / disputed / awaiting, from the same numbers as the Overview), replaces the bar strip with a line chart (axis, hover value, dots where the score fell) and adds "What is affecting trust" listing the failing health checks and open problems with a link to where to fix each. This also settles the naming collision on this page. The history is in memory only (about 25 minutes, 500 samples, empty after a restart), and the page says so; persisting it needs a schema change and was not attempted. The `/trust-center` response gained `components`, `replica_verification`, `affecting` and `history_window`; its existing keys are unchanged.