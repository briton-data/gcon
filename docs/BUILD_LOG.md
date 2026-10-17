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
- [x] 16 routes declare no response schema and the spec declares no bearer security scheme **(verified)**. Done 2026-10-17: all 36 routes document a 200 schema (via `responses=`, so runtime output is unchanged), `BearerAuth` and `ApiKeyAuth` are declared, the 5 public routes are `security: []`. Pinned by `tests/api/test_openapi_contract.py`.
- [x] `command` is described as "Shell command" but the filter allows one allowed program plus arguments, and `python <anything>` passes **(verified)**; decide and document the meaning. Done 2026-10-17: meaning kept as it behaves today (one allowed program plus unchecked arguments; a plan with no program list accepts any command line; the sandbox, not the filter, is the boundary) and the spec now says so. No behavior change. Restricting arguments would be a breaking change, so it belongs in `/api/v2` if wanted. Pinned by `tests/api/test_command_meaning.py`.
- [ ] `docs/API.md` omits 11 existing routes (`/auth/*`, `/jobs/clear`, attempts, retry, enrollment-history, telemetry) **(verified)**
- [ ] `docs/DEPLOYMENT.md` says Postgres is not wired; it is (`GCON_CONTROL_PLANE_DATABASE_URL`)
- [ ] Postgres: `GET /billing/invoices*` fails (`ORDER BY rowid`, SQLite-only) and fractional `unit_price_cents` is stored as 0 because the column is INTEGER **(verified on PostgreSQL 16)**; SQLite is unaffected
- [ ] Production persistence: identity DB (orgs, users, API keys) is always SQLite under `GCON_DATA_DIR`; on an ephemeral container disk it is lost on every redeploy. Confirm a volume is mounted. 2026-10-17: the coordinator now logs where each of the five state locations lives and warns `DATA WILL NOT SURVIVE A REDEPLOY` for any not on a mounted volume (`src/gcon/persistence/durability.py`). The signing key, webhook key and CA default to `./keys`, not `GCON_DATA_DIR`, so they need a volume too. **Still open: mounting the volume on the host.**

**Decide now (cheap while additive, costly later)**
- [ ] List routes return bare arrays with no paging beyond `limit`
- [ ] Errors: `detail` is a string, except validation errors (an array); no machine-readable `code`
- [ ] Rate-limit headers exist only on 429 (`Retry-After`)
- [ ] Mark `/artifacts`, `/workflows` and proof fields as experimental in the spec
- [ ] Whether customers get `POST` to mint worker enroll tokens in v1 or v2

**Then freeze**
- [ ] Commit `openapi-v1.json` and a contract test that fails on removed fields, renamed fields or changed types

## v2 agenda (not v1 blockers)

- Where the work comes from: a git repository/ref instead of a bare command string; other languages and runtimes; environment and secrets.
- Customer roles and scoped keys for account-level actions.
- Envelope responses with paging; one error shape with codes.
- Real payment provider and invoice delivery.
- Performance: the single coordinator process tops out around 120 jobs/s on the benchmark, and SQLite lock contention is the first limit.
