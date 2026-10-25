# Customer-side carryover map

This records what was **removed from the internal staff dashboard UI**
when the control plane became aggregate-only, and **which backend route
each removed control called**.

Nothing in this document was deleted from the backend. Every route
listed below is still live, still registered, and still tested. The
removals were frontend-only, on the basis that this functionality
belongs to the customer-facing product rather than to internal staff
operations.

When the customer dashboard is built, this is the wiring list — the
work is building a UI against routes that already exist, not
re-implementing the routes.

---

## 1. Control Operations panel (removed from Overview)

The whole `#operations-panel` block, plus `setupOperationsPanel()` and
`populateOperationsSelectors()` in `dashboard.js`.

| Removed control | Backend route | Method |
|---|---|---|
| Pause Scheduler | `/cluster/scheduler/pause` | POST |
| Resume Scheduler | `/cluster/scheduler/resume` | POST |
| Refresh | `/cluster` | GET |
| Emergency Stop | `/cluster/emergency-stop` | POST |
| Drain Node | `/cluster/nodes/{node_id}/drain` | POST |
| Restart Worker | `/cluster/nodes/{node_id}/restart` | POST |
| Stop Worker | `/cluster/nodes/{node_id}/stop` | POST |
| Cancel Running Job | `/jobs/{job_id}/cancel` | POST |
| Clear Queue | `/cluster/queue/clear` | POST |
| Retry Failed Jobs | `/jobs/retry-failed` | POST |
| Clear Failed Jobs | `/jobs/clear-failed` | POST |
| Verify Receipts | `/receipts/verify-all` | POST |
| Export Logs | `/logs/export` | GET |
| Export Metrics | `/metrics/export` | GET |
| Snapshot Cluster | `/cluster/snapshot` | GET |
| Scale Up / Scale Down (`cc-scale-*`) | `/admin/scale-{up,down}` | POST |

Note: the Scale Up/Down controls still exist in the **Administration**
tab (`admin-scale-*`), which was left untouched. Only the duplicate
copies on the Overview were removed.

## 2. Executions tab (removed entirely)

Removed: `loadExecutionsTab`, `setupExecutionsTab`,
`renderExecutionsTable`, `renderExecutionSummaryTiles`,
`openExecutionDetail`, `buildLifecycleStepper`, and the
`#tab-executions` markup.

| Capability | Backend route |
|---|---|
| Paginated / filtered job list | `/jobs` (server-side pagination via `get_jobs_page`) |
| Per-job detail drawer | `/jobs/{job_id}` (`get_execution_detail`) |
| Per-client job list | `/management/organizations/{org_id}/jobs` |

`Coordinator.get_jobs_page()` already supports `status`, `search`,
`org_id`, `limit`, `offset` and queries the durable control-plane DB,
so a customer-facing job list does not need new backend work.

## 3. Receipts tab (removed entirely)

Removed: `loadReceiptsTab`, `setupReceiptsTab`, `renderReceiptCards`,
`renderReceiptSummaryTiles`, `openReceiptDetail` (191 lines), and the
`#tab-receipts` markup.

| Capability | Backend route |
|---|---|
| Paginated receipt list | `/receipts` |
| Receipt detail | `/receipts/{receipt_id}` (`get_receipt_detail`) |
| Bulk re-verification | `/receipts/verify-all` |

`get_receipt_detail()` still returns the full payload the removed
drawer rendered, including `stages`, `execution_proof`, `replicas`,
`policy` and `output`. This is the richest single endpoint for the
customer evidence view — it is the product surface.

## 4. Topology Map tab (removed entirely)

Removed: `loadTopology`, `buildTopologySvg` callers,
`buildTopologyLegend`, `openNodeTopologyDetail`,
`openCoordinatorTopologyDetail`, `attachTopologyHandlers`.

| Capability | Backend route |
|---|---|
| Topology graph data | `/topology` |

`buildTopologySvg()` itself was **kept** — the small Live Topology
preview on the Infra tab still uses it.

## 5. Analytics & History tab (removed entirely)

Removed: `loadAnalytics`, `renderBarChart`, `#tab-analytics` markup.
This was job history, which is customer-side.

| Capability | Backend route |
|---|---|
| Historical job/analytics data | `/analytics` |

## 6. Live Metrics tab (merged, not removed)

`loadMonitoring` and `#tab-monitoring` were removed, but the panels
that were not duplicated elsewhere (Avg CPU, Avg Memory, Node Health,
Storage Capacity) were folded into the merged **Infra** tab. The
Coordinator/Scheduler status panel was dropped as a genuine duplicate
— it is already shown in the header strip and on Overview.

## 7. Trust Center (trimmed to aggregate)

Kept as a tab, but these per-item sections were removed:

- Verification Failures list (per-receipt rows)
- Node Trust Status table (per-node rows)
- Verification Timeline (per-event rows)

Retained: the four aggregate metric cards, Trust Score History, and
the Signature Validation summary — all already aggregate.

The data behind the removed sections still arrives in the same
`/trust-center` payload (`verification_failures`, `node_trust`,
`verification_timeline`); only the rendering was removed.

---

## What changed in the backend (and why)

Two additive changes were made, both required for the pillars to show
real numbers rather than invented ones:

1. `PresentationLayer.get_dashboard_metrics()` now also returns
   `pending_jobs` and `cancelled_jobs`, counted in the same single
   loop that already produced running/completed/failed. Without this
   the Orchestrate pillar's "queued" figure would have to be derived
   by subtraction, which would silently fold cancelled jobs into
   "queued".

2. `Coordinator.get_policy_evaluation_counts()` (plus the
   `_record_policy_result()` helper and two running counters) provides
   the Assure pillar's compliant/exception totals. These are counted
   incrementally, matching the existing `_verified_receipt_count`
   pattern, because this payload is rebuilt on every websocket tick
   and scanning every receipt's `policy_report` per tick would be
   O(total receipts).

   **Known limitation, stated honestly:** `policy_report` is attached
   to an in-memory receipt and has no persisted column, so these
   counts are per-process and reset when the coordinator restarts.
   The method returns `evaluated` alongside the counts so the UI can
   tell "nothing evaluated yet" apart from "everything passed" —
   `renderPillars()` shows `--` rather than `0` in that case, so an
   empty counter is never displayed as a clean bill of health.
   Making these survive a restart requires a schema migration to
   persist policy results, which was **not** attempted here.

No route was deleted, renamed, or had its behaviour changed.

---

## Added 2026-10-24: workers, hosted fleet and leases

The map above is about what left the staff dashboard. This part is what the **customer** side now needs for
workers. Full behaviour is in `docs/WORKERS_AND_ENROLLMENT.md`. Everything marked "exists" is live and tested.

### 8. Hosted workers (leases): the customer wiring list

| Capability | Route (customer API key) | Status |
|---|---|---|
| See what my plan allows and what I hold | `GET /account/plan` -> `max_workers` (own workers; null = no limit), `max_leased_workers` (**0 = none, not unlimited**), `leased_workers_held` (includes leases still ending or being cleaned) | exists |
| Request N hosted workers (all or none) | `POST /workers/leases` `{count}`; errors `400 bad_request`, `403 plan_limit`, `409 not_enough_workers` (with `available`) | exists |
| List my leases | `GET /workers/leases` (open and recently ended) | exists |
| Release one | `DELETE /workers/leases/{lease_id}` (finishes its running job, is cleaned, returns to the pool) | exists |
| See my workers, including leased ones | `GET /nodes`, `GET /nodes/{node_id}` (a hosted worker's `address` is null) | exists, see gaps |
| Enrollment attempts for one of my workers | `GET /nodes/{node_id}/enrollment-history` | exists |
| Submit a job or workflow | `POST /jobs`, `POST /workflows`: **new `409`** when I have no worker to run it (below) | exists |
| Is GCON's old shared pool on? | `GET /org/shared-pool` -> `enabled` (false unless staff turned the pool on in the dashboard; hide any pool UI while false) | exists |

Lease states for the UI: `active`, `ending` (finishing a running job), `sanitizing` (being cleaned), `ended` with
`end_reason` `customer_release` / `idle` / `staff_release` / `worker_lost`.

### 8.1 How the site should handle the submission `409` (no workers)

`POST /jobs` and `POST /workflows` return `409` when the organization has nothing that could run the job. Body:
`detail` (a sentence to show as is), `code` (`no_workers` or `not_enough_workers_to_verify`), `workers_have`,
`workers_needed`, `max_leased_workers`, `leased_workers_held`, `request_workers`.

| What the site does | When |
|---|---|
| Show `detail` and a **Request worker** button that calls `POST /workers/leases` `{count: workers_needed - workers_have}` and then re-submits | `request_workers` is not null (the plan has room) |
| Show `detail` with **Release a worker you do not need**, **Add your own worker**, **Upgrade plan** | `request_workers` is null and `max_leased_workers` > 0 (at the limit) |
| Show `detail` with **Add your own worker**, **Upgrade plan** | `request_workers` is null and `max_leased_workers` = 0 |
| Do not offer verification when the customer has fewer than 2 workers (use `workers_have`, i.e. count of own workers from `/nodes` plus active leases from `/workers/leases`); explain that it needs 2 | before submitting a job with `verify` |

Other submission errors keep their meaning: `400` an invalid request (shown before this check), `429` the concurrent-job
limit, `409` from `/workers/leases` is a different body (`not_enough_workers`, with `available`). Distinguish by `code`.
A lease is released automatically after it sits idle (60 minutes by default), so a customer returning after a day will
get this `409`; the site should say their worker was released for being idle, not that something broke.

### 9. Gaps: needed on the customer side, not available to customers today

| Gap | Today |
|---|---|
| Why my own worker is not taking jobs | The reasons (`connected`, `serves`, `blockers`, `can_run_customer_jobs`, `agent_version`, `outdated`, `sandboxed`) exist on the **staff** `/nodes` only. The customer `NodeOut` model omits them. |
| Create my own worker enrollment token | Staff only (`/management/enroll-tokens`). A customer who self-hosts must ask staff for a token. |
| Archive my own offline workers | Staff only (`/management/nodes/{id}/archive`). Offline workers keep counting against `max_workers` until staff archive them. |
| Notifications, incidents, event feed, audit log | The clear/dismiss controls added to the staff dashboard are staff-only. There is no customer notification feed. |
| Show the customer, before they submit, that they have no worker | The data exists (`/account/plan` and `GET /workers/leases` and `/nodes`); the site must use it, see 8.1 |

### 10. Backend changes since the map above (all additive unless stated)

- `PresentationLayer.get_dashboard_metrics()` returns a `verification` block (verified / disputed / unavailable /
  awaiting / not_requested). **"Verified" now means only that replicas of a job agreed.** A receipt's signature being
  intact is not verification; the customer UI must not label it so. Per job, the outcome is the `verification` field
  of `GET /jobs/{job_id}` (`agreed` / `disputed` / `unavailable`).
- Behaviour change (not a route): "idle" now means dispatchable (connected, not draining, not quarantined, not
  outdated; for hosted workers, under an active lease). `get_idle_nodes`, the idle count and the scheduler agree.
- Staff `GET /nodes` gained `connected`, `agent_version`, `outdated`, `sandboxed`, `shared_pool`, `serves`,
  `blockers`, `can_run_customer_jobs`, and the node summary gained `customer_ready`.
- `GET /account/plan` gained `max_workers`, `max_leased_workers`, `leased_workers_held`.
- New tables: `fleet_nodes`, `worker_leases` (migration 18); `nodes.archived_at` (migration 17); `enroll_tokens.kind`.

### 11. Decisions made 2026-10-24 and their state

| Decision | State |
|---|---|
| Hosted workers are claimed by the customer (`POST /workers/leases`); no automatic assignment | built |
| The old shared pool is kept but off (it duplicates leases) | **built**: master switch on the staff dashboard (Administration, "Shared worker pool"), off by default, stored; flags and modes managed in the same panel |
| The Standard plan includes 1 hosted worker; Pro 4; Enterprise 20 (placeholders) | **built** (`max_leased_workers`) |
| A job with no worker to run it is refused with an error that says what to do | **built**: the `409` above |
| Verification needs 2 workers, so on Standard it needs the customer's own workers or a larger plan | **built**: `not_enough_workers_to_verify` |
| Standard `max_workers` (own workers) is 5 | placeholder: set to 0 if self-hosting should be a paid feature |
| The shared `GCON_ENROLL_TOKEN` (never expires, organization-less) | undecided; see `docs/WORKERS_AND_ENROLLMENT.md` section 12 |

### 12. Staff-dashboard controls that exist in the backend but have no button (checked 2026-10-25)

Method: every staff route listed in `docs/CODEMAP.md` was searched for in `static/js/dashboard.js` and the templates.
145 routes; the ones below have no caller. Most are **deliberate**: Round 7 removed per-node and per-job controls from
the staff console on purpose (incident response is not done from that UI; the routes were kept for the customer side).

| Route(s) | What it does | Why there is no button |
|---|---|---|
| `POST /cluster/nodes/{id}/drain`, `/stop`, `/restart`, `/clear-quarantine` | Operate one worker | Removed in Round 7 on purpose. The dashboard still **shows** draining and quarantined state. |
| `POST /cluster/emergency-stop`, `POST /cluster/queue/clear` | Cluster-wide stop; empty the queue | Removed in Round 7 on purpose |
| `POST /jobs/{id}/cancel`, `/jobs/{id}/retry`, `GET /jobs/{id}` | One job | Removed in Round 7 on purpose (per-job drill-in belongs to the customer side). The customer API already has cancel/retry/detail for a customer's own jobs. |
| `POST /jobs/retry-failed`, `/jobs/clear-failed`, `GET /jobs/status-counts` | Bulk failed-job actions | Removed in Round 7 with the job list |
| `POST /receipts/verify-all`, `GET /receipts-summary` | Re-check all receipts | Removed in Round 7 with the receipts tab |
| `GET /analytics`, `/artifacts`, `/cluster/snapshot`, `/hero-status`, `/system-metrics`, `/trust-history`, `/logs/export`, `/metrics/export`, `/management/search`, `/management/dashboard-cards` | Read-only data and exports | Data endpoints, not controls; the dashboard gets its numbers through the live feed instead |
| `POST /admin/scale-up`, `/admin/scale-down` | Scale workers | The buttons exist; they only work with a real cloud provisioner (explicitly left as is) |

Built since: shared pool switch, flags and modes (panel in Administration); lease history per hosted worker.

**One decision this raises:** hosted-fleet workers have Release, Clean and History on the dashboard but no Stop or
Drain. For a suspected compromised hosted worker the runbook is "release its lease, do not return it to the pool,
stop it", and stopping it is done outside the dashboard today. Adding Stop and Drain for hosted workers only would
reverse Round 7 for that one place; it needs an explicit yes.

