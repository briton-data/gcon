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
