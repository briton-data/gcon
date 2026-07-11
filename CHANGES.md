# Changes made in this pass

## Users Management (UI-first, sample data)

Added a new "Management" section to the dashboard (Users, Organizations,
Teams, API Keys, Permissions, Audit Logs, Notifications), matching the
requested nav structure. Only **Users Management** is fully built this
round; the rest are polished "Coming soon" placeholders so the nav doesn't
dead-end.

This is explicitly **UI-first, not backed by a real user system** — GCON
has no concept of a user, login, or auth today, so building this "for
real" (persistent accounts, actual RBAC enforcement) is a separate,
much bigger effort. What's here:

- Dashboard cards: Total / Active / Pending / Suspended+Disabled users
- Search (name/email/role) + status filter + role filter
- User table: avatar, name/email, role badge, status badge, last active,
  job count, edit/delete actions
- Add/Edit User modal (name, email, role, status)
- Delete with confirmation
- User Profile page (opens on row click) with 7 tabs: Overview, Jobs,
  Workflows, Activity, API Keys, Permissions, Settings — populated with
  per-user sample stats (jobs submitted/running/failed/completed,
  workflows created, CPU/storage usage, API requests, login count,
  an activity timeline, sample API keys, and role-derived permissions)

All of it runs on an in-memory sample dataset (`SAMPLE_USERS` in
`dashboard.js`, 10 seeded users spanning every role/status) — Add/Edit/
Delete mutate that array directly. **Nothing persists across a page
reload**, by design, since there's no backend user store to write to yet.
If/when this becomes a real feature, the natural next step is a `users`
table + `presentation.get_users()`/`create_user()`/etc. backed by actual
storage, same pattern as everything else in this app.

Cross-referenced to `GCON-bug-report.md`. Every item below was verified with
the test suite (`tests/test_gcon.py` + new `tests/test_coordinator.py`, 47
tests, all passing) and/or by rendering the templates directly with jinja2.

## Dashboard 2.0 update merge (`GCON-dashboard-update.zip`)

The uploaded bundle was a significantly more feature-complete dashboard
(tabbed Control Center / Topology / Explorer / Monitoring / Analytics /
Admin views) that had independently picked up some of the earlier fixes
(consistent `node_id` naming, job timestamps, working `deregister_agent`)
but also reintroduced a couple of bugs and dropped others. Merged it in and:

- **Reintroduced duplicate-registry bug fixed again**: `coordinator.py` was
  back to `from Noderegistry import NodeRegistry`. Repointed at the
  consolidated `node_registry.py`.
- **`assign_job()` idempotency guard was gone** — the double-dispatch bug
  (same job sent to two nodes) was back. Re-added the status check.
- **Duplicate `get_artifacts()` definition** — `coordinator.py` defined it
  twice; the second (reading from an always-empty `self.artifacts` dict)
  silently shadowed the first (the real one, backed by
  `artifact_registry`), so the Explorer's Artifacts tab would always have
  shown nothing despite real artifacts existing. Removed the dead
  duplicate and the always-empty `self.artifacts` dict along with it.
- **`metrics.py` still read the old `job["agent"]` key** — this dashboard
  version stores jobs under `"node_id"`. Fixed, and added a regression test
  (`test_dashboard_v2.py`) since this file wasn't part of the uploaded
  bundle and could silently drift again.
- **Workflow engine wiring was dropped** — restored (`coordinator.py` owns
  a `WorkflowEngine` again; `get_workflows()` is real, not a hardcoded `[]`).
- **Node/job "offline" mislabeled as "Idle" again** in `dashboard.js`'s
  `statusBadge()` — same bug as before, reintroduced in the new JS. Fixed:
  offline is now red/"Offline", distinct from idle.
- Naive `datetime.now()` calls (non-UTC) cleaned up for consistency, same
  as the earlier pass.
- Added a `/workflows` route (present in the earlier version, missing here).

## Dashboard 2.0 features implemented

- **Live updates via Server-Sent Events**: new `GET /stream` route in
  `web_server.py`. It's a real push channel, not disguised polling — it
  subscribes to the coordinator's event bus and forwards each event to
  connected clients immediately (bridged from the coordinator's background
  threads into the asyncio loop via `call_soon_threadsafe`), plus a 2-second
  heartbeat snapshot so the numbers never go stale during quiet periods.
  `dashboard.js`'s `connectStream()` consumes it and drives the Control
  Center's headline metrics and health badges in near real time. Table
  polling (nodes/jobs/events lists, which `/stream` doesn't carry in full)
  stays in place underneath as before — SSE covers the glanceable numbers,
  polling keeps the detail tables fresh.
- **Animated counters**: every numeric metric card now eases from its old
  value to its new one (~500ms, ease-out) instead of jumping instantly —
  `setCounter()` in `dashboard.js`, applied across Control Center,
  Monitoring, Analytics, and Admin tabs.
- **Real health indicators**: the three dynamic badges in the Cluster
  Health panel (Coordinator Online / Cluster Healthy / Event System
  Running) are now derived from actual cluster state (failed job count,
  node count) instead of being permanently hardcoded green. ("Storage
  Connected" stays static — there's genuinely no failure mode modeled for
  local-disk storage in this architecture, so making it "dynamic" would
  mean fabricating a signal that doesn't exist.)
- **Auto-updating event feed**: already working from the uploaded bundle
  (`loadEvents()` on the poll interval); now also gets an immediate nudge
  when `/stream` reports a new event, rather than waiting for the next
  5-second tick.

## Not yet started: Dashboard 3.0 / 4.0

The uploaded bundle already covers a good chunk of 3.0/4.0's *data* surface
(topology SVG view, an Explorer tab across jobs/nodes/receipts/artifacts
with search, an Analytics tab with a job-outcome bar chart, an Admin tab
with scale up/down and node deregistration). Genuinely not built yet:

- **3.0**: real CPU/Memory time-series charts (currently single
  point-in-time averages, not graphs); network throughput (no backend data
  source for this exists anywhere in the codebase — nodes don't currently
  track bytes sent/received, so this needs a design decision, not just UI);
  queue depth as a visual/chart (the number exists via `/admin/config`, not
  graphed); job progress bars (jobs are binary pending/running/completed/
  failed with no percentage-complete concept — an honest implementation
  would be an indeterminate "in progress" bar for running jobs, not a
  fabricated percentage).
- **4.0**: click-to-inspect on topology nodes; a node detail drawer; a job
  inspector (detail view for a single job/receipt); a proper alert center
  (derivable from real data — failed jobs, offline nodes — but needs a
  dedicated panel); a log viewer (would need a log-capturing handler added
  to the coordinator, since nothing currently retains log lines beyond
  stdout).

## Backend (original prototype audit)

- **Consolidated the duplicate `NodeRegistry`.** Deleted `registry.py`
  (broken, missing `datetime` import) and `Noderegistry.py` (the one
  actually in use), replaced both with a single `node_registry.py`. Added a
  `.get()` alias for `.get_node()` so any caller expecting dict-like access
  works.
- **`coordinator.deregister_agent()`**: fixed the call to a nonexistent
  `registry.get()`, and — more importantly — it now actually calls
  `registry.remove()`. Previously it printed "deregistered successfully"
  without removing anything.
- **`coordinator.dashboard()`**: fixed the `Dashboard()` constructor call
  (was passing 2 args to a 1-arg constructor) and the no-op `dashboard.refresh`
  reference (was missing `()`, so refresh silently did nothing).
- **`coordinator.assign_job()` is now idempotent.** Found this while
  smoke-testing the live pipeline: `submit_job()` queues jobs for an
  always-running background scheduler thread, but nothing stopped a job from
  also being assigned a second time (e.g. an explicit `assign_job()` call
  racing the background thread) — which dispatched the *same job* to *two
  nodes* simultaneously. It now checks the job's status before dispatching.
- **`coordinator.get_jobs()` field mismatch fixed.** Jobs are stored
  internally under the `"agent"` key, but `get_jobs()` was reading
  `job.get("node_id")` — always `None`. Every job in the dashboard showed no
  assigned node until this was fixed.
- **`created_at`/`completed_at` are now actually set** on jobs (submit and
  on completion/failure) — previously read by `get_jobs()` but never written
  anywhere, so those columns were always blank.
- **`event_bus.unsubscribe()`**: fixed a typo (`self._subscriber` →
  `self._subscribers`) that made it raise `AttributeError` whenever called.
- **`event.py`**: removed a dangling module-level type annotation and
  duplicate import left over from a bad paste. Added `Event.to_dict()` and
  `Event.to_message()` helpers (used by the live events feed, see below).
- **Workflow engine wired into the coordinator.** `WorkflowEngine` existed
  as a fully-implemented, standalone class that nothing ever instantiated.
  `GCONCoordinator` now owns one (`self.workflow_engine`), and
  `get_workflows()`/`submit_workflow()` are real instead of a hardcoded `[]`.
- **`presentation.py`**: `get_events()` now delegates to the real event bus
  instead of returning three hardcoded fake events; `get_workflows()`
  delegates to the coordinator; `register_node()`/`deregister_node()` now
  call the coordinator's actual method names (`register_agent`/
  `deregister_agent` — they were calling nonexistent `register_node`/
  `deregister_node` on the coordinator); removed unreachable debug `print()`s
  after a `return` and a leftover debug print in `get_cluster_state()`.
- **Cleanup**: removed three stray `.bak` files at the repo root; fixed three
  inconsistent naive `datetime.now()` calls in `coordinator.py` to use
  `datetime.now(UTC)` like the rest of the codebase; fixed deprecated
  `datetime.utcnow()` in `tests/test_gcon.py`; removed an unused
  `from network import GCONNetwork` / `from node import GCONNode` import in
  `stage10_test.py`.
- **Marked genuinely dead code.** `node.py` (`GCONNode`), `network.py`
  (`GCONNetwork`), and `dispatcher.py` (`JobDispatcher`) are an unused
  legacy path — nothing in the live coordinator calls into them (job
  assignment goes through `Scheduler`/`NodeRegistry` directly, and
  `GCONNode` doesn't even implement `heartbeat()`, which `assign_job()`
  requires). Left in place but each file now has a note explaining this, so
  nobody spends time debugging a path that was never live.
- **`stage5_test.py`**: was building nodes through that dead
  `GCONNode`/`GCONNetwork`/`JobDispatcher` chain (which the coordinator never
  reads from) and separately through a plain `registry.py` instance the
  coordinator also never saw — so `assign_job()` always failed with "no
  available nodes". Rewritten to register `GCONAgent` instances directly
  with the coordinator, matching how the rest of the live system
  (`dashboard_server.py`, `stage10_test.py`, etc.) actually does it.

## Frontend

- **The Jobs panel is now actually wired up.** `loadJobs()` existed but was
  never called from the refresh loop; and if it *had* been called, it would
  have recursed into itself infinitely (its own body ended by calling
  itself). Both fixed.
- **XSS gap closed**: `loadJobs()` interpolated job fields (including the
  shell command a job runs) straight into `innerHTML` with no escaping. Now
  uses the same `escapeHtml()` every other panel uses.
- **Field names now match the real API.** The JS was written for an older
  job shape (`agent`, `command`, `artifacts` as an array) that no longer
  matches what `/jobs` actually returns (`node_id`, no `command` field,
  `artifacts` as a count). Fixed to match, and reordered the built table rows
  to match the `<thead>` column order.
- **Node table column order fixed** — the JS was building `cpu, memory,
  running_jobs` but the table header is `running_jobs, cpu, memory`, so
  every live refresh put numbers under the wrong column headers.
- **Node status badges fixed.** The template only special-cased `"healthy"`
  and `"offline"` — but nodes are never actually in a `"healthy"` state
  (real values are `idle`/`busy`/`offline`), and `"offline"` nodes were
  labeled **"Idle"** in the UI, which is actively misleading. Now correctly
  shows Idle (green) / Busy (blue) / Offline (red).
- **Root cause of "dashboard isn't live" found and fixed**: `dashboard.html`
  (the template actually served) is a newer Bootstrap redesign that never
  had the `id` attributes `dashboard.js` needs to find and update elements —
  those ids only existed in `templates/index.html`, an old single-page
  template that no route ever serves and which references a `style.css`
  file that doesn't exist. Added the missing ids across `metric_card.html`,
  `nodes.html`, `jobs.html`, `activity.html`, `cluster_overview.html`, and
  the navbar's status badge, so the real, served template is the one that's
  actually live. Removed `templates/index.html` since it was dead and
  actively misleading.
- **Cluster Health panel now actually appears.** The second column in the
  dashboard's top panel row was just an empty `<div>` — `cluster_health.html`
  existed but was never `{% include %}`'d anywhere. Also: because that
  column was empty, `dashboard.js` was calling `.textContent = ...` on
  `null` for several elements every refresh, throwing inside `loadCluster()`
  and causing the whole cluster-status badge to incorrectly flip to
  "Unavailable" on every single refresh, even though the actual `/cluster`
  data had loaded fine. Fixed both: the panel is now included, and the JS
  helper (`setText()`) is null-safe regardless.
- **Live activity feed added.** Added a `GET /events` route and a
  `loadEvents()` poller so the "Cluster Activity" panel now shows real
  events instead of never updating.
- **Fixed malformed HTML in `base.html`**: a stray `>` after the CSS
  `<link>` tag, and a stray `<`/`>` wrapping the dashboard.js `<script>` tag.
- **`requirements.txt`**: moved `GPUtil` to a commented-out optional line
  with an explanation — it's not needed (the code already handles its
  absence), and requiring it needlessly blocks installs on machines without
  the NVIDIA-specific package available.

## Tests

- Added `tests/test_coordinator.py` — 29 new tests covering
  `NodeRegistry`, `EventBus`, `Event`, `GCONCoordinator` (agent lifecycle,
  job assignment, idempotency, cluster state), and `PresentationLayer`.
  Several are explicit regression tests for the bugs above so they can't
  silently come back.
- Fixed the 3 pre-existing test failures (`stage5_test.py`, `stage10_test.py`,
  `stage16_test.py`) — see the backend fixes above; these were failing
  because of the underlying bugs, not because the tests were wrong.
- Fixed deprecated `datetime.utcnow()` usage in `tests/test_gcon.py`.
- Full suite: **47/47 passing**, plus all `stageN_test.py` / `Stage16.py` /
  `stage17.py` integration scripts run clean end-to-end.
