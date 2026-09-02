# GCON Coordinator Failover (High Availability)

This document covers `gcon.cluster.leader_election.LeaderElector` and the
`--ha` flag on `scripts/run_coordinator.py` — real, built, and verified
against the actual source. Earlier versions of `docs/ARCHITECTURE.md` and
`README.md` described HA/leader election as "not implemented yet." That
was true when written; it no longer is. If anything in this file ever
disagrees with `src/gcon/cluster/leader_election.py`, trust the source.

---

## Table of Contents

1. [What this gives you](#what-this-gives-you)
2. [What this does NOT give you](#what-this-does-not-give-you)
3. [How it works](#how-it-works)
4. [Running an HA cluster](#running-an-ha-cluster)
5. [What happens during a failover](#what-happens-during-a-failover)
6. [Configuration](#configuration)
7. [Operational notes](#operational-notes)
8. [Related: receipt/node identity, not coordinator identity](#related-receiptnode-identity-not-coordinator-identity)

---

## What this gives you

Run N `GCONCoordinator` processes pointed at the **same control-plane
database**. Exactly one holds a lease and is "active" — it runs the mTLS
gRPC transport, accepts worker connections, and dispatches jobs. The rest
are "standby" — they serve read-only dashboard/API queries against the
same shared DB, but never dispatch. If the active process dies, its lease
expires and a standby acquires it and takes over, typically within one
lease TTL (default 10s).

This is real, tested infrastructure (`gcon.cluster.leader_election`,
backed by `gcon.persistence.repositories.leases.LeaseRepository`), not a
roadmap item.

## What this does NOT give you

Straight from `leader_election.py`'s own module docstring — stated here
exactly as honestly as it's stated in the source:

- **Zero-downtime failover.** There's a real gap — up to `ttl_seconds`
  worst case — between the leader dying and a standby noticing. That's
  the honest cost of lease-based (vs. a real consensus protocol like
  Raft) election; getting it reliably below a few seconds needs a
  dedicated coordination service (etcd/ZooKeeper/Consul) this deployment
  doesn't have.
- **In-flight job continuity across a failover.** A job the since-dead
  leader had dispatched to a node is handled by the *existing*
  reconciliation path (`recover_jobs` / `restore_from_persistence`'s
  in-flight-job handling), not anything new here — the new leader picks
  it up the same way any coordinator restart already would.
- **Active-active.** This is active-passive. Agents still connect to
  *one* coordinator's gRPC endpoint at a time. Making that endpoint
  itself durable across a failover (a VIP, a load balancer
  health-checking `/api/v1/health` and routing only to the leader, or
  client-side multi-address failover in the agent) is deployment/infra
  work outside GCON's own code — deliberately not invented here. Until
  you build that, a failover means agents need to be pointed (manually,
  or via your own external mechanism) at whichever process just became
  leader.
- **A distributed control-plane database.** The lease itself lives in
  the same SQLite `coordinator_leases` table every other control-plane
  data lives in. See [Operational notes](#operational-notes) for what
  that means for where your coordinator processes can actually run.

## How it works

`LeaderElector` (`src/gcon/cluster/leader_election.py`) wraps
`LeaseRepository.try_acquire_or_renew` (`src/gcon/persistence/repositories/leases.py`)
— a single-row-per-lease-name compare-and-swap store:

```
coordinator_leases
  lease_name   (default: "coordinator-leader")
  holder_id    (e.g. "coord-host-a:41213:9f2a1b3c")
  term         (increments every time the holder actually changes)
  acquired_at
  expires_at
  updated_at
```

Every acquire-or-renew attempt runs inside one
`ControlPlaneDatabase.transaction()` — which holds both the process-level
`threading.RLock` and SQLite's own write lock for the duration — so two
coordinator processes racing to acquire the same lease can never both
succeed, even though they're different OS processes sharing only the
database file. The read-modify-write (check current holder + expiry,
then write) happens inside that one serialized transaction.

The decision logic, per attempt:

- No row yet → whoever asks first creates it (term 1) and is leader.
- Row exists, `holder_id` matches the caller → renewed in place, same
  term, `expires_at` pushed forward. (This is how the active leader keeps
  its lease alive.)
- Row exists, a *different* `holder_id`, and it's expired
  (`expires_at <= now`) → the caller takes over. `term` increments.
- Row exists, a different `holder_id`, and it's **not** expired →
  `try_acquire_or_renew` returns `None`. The caller stays (or remains)
  standby.

`LeaderElector` wraps this in a small state machine:

- `run_until_leader()` — blocks, retrying at `renew_interval_seconds`
  (a third of the TTL by default), until this process acquires the
  lease. Called once at startup, *before* the gRPC transport or
  dashboard starts serving — a coordinator should never begin
  dispatching before it knows it's actually safe to.
- `start()` — starts a background thread that keeps calling
  `try_acquire_or_renew` every `renew_interval_seconds`, both to renew
  while leading and to keep trying to acquire while standby.
- `is_leader` — a thread-safe property. `scheduler_loop` checks this on
  every tick; `submit_job()` checks it on every call (see
  [How the coordinator uses this](#how-the-coordinator-uses-this)).
- `on_become_leader` / `on_lose_leadership` — callbacks fired exactly on
  the True→False / False→True transition, not on every tick.

Renewal happens well inside the TTL, not at its edge — the default
`renew_interval_seconds` is `ttl_seconds / 3`, so one missed tick (a GC
pause, a slow disk write) costs nothing; it takes roughly 3 consecutive
missed renewals before a healthy leader would actually lose its lease.

### How the coordinator uses this

Two real gating points in `src/gcon/cluster/coordinator.py`, both keyed
off the same `self.leader_elector` (`None` by default — a coordinator
with no `--ha` is always "leader", unaffected by any of this):

- **`scheduler_loop`** — a standby (`leader_elector is not None and not
  leader_elector.is_leader`) never drains the job queue. It just waits
  and re-checks; it does not dispatch.
- **`submit_job()`** — a standby raises `NotLeaderError` immediately
  rather than silently accepting the job. Accepting it would durably
  persist the job (the DB write itself is harmless — any coordinator can
  write) but leave it pending forever, since a standby's scheduler loop
  never drains the queue. Rejecting loudly lets the caller retry against
  the actual leader instead of a job silently going nowhere.

```python
class NotLeaderError(RuntimeError):
    ...
```

## Running an HA cluster

Two (or more) coordinator processes, same `--db` (or same `GCON_DATA_DIR`):

```bash
# Process A
python scripts/run_coordinator.py --ha --coordinator-id coord-a --db /shared/gcon_control_plane.db

# Process B (different host or same host, different process)
python scripts/run_coordinator.py --ha --coordinator-id coord-b --db /shared/gcon_control_plane.db
```

- `--ha` is required to opt in at all — without it, `leader_elector`
  stays `None` and none of this runs; a plain `run_coordinator.py` is
  unaffected.
- `--coordinator-id` is optional. It sets a stable `holder_id` for this
  process. Without it, `default_holder_id()` generates
  `hostname:pid:random-hex` fresh on every start — fine for a one-off
  test, but pass an explicit id if you want a restarted process
  recognizable as "the same" coordinator in logs/lease history.
- Whichever process wins the initial race becomes leader and starts
  serving (gRPC transport + dashboard/API). The other blocks in
  `run_until_leader()`, quietly retrying, serving nothing — **note**:
  today, a standby doesn't serve the dashboard/API either, since
  `run_coordinator.py` only calls `web_server.start()` after
  `leader_elector.run_until_leader()` returns. A standby is fully
  passive until it becomes leader, not "read-only" in the sense of
  actively answering requests. (This is a real, current behavior worth
  knowing, not a documented design goal — see
  [Operational notes](#operational-notes).)

## What happens during a failover

1. Leader process A dies (crash, `kill -9`, host failure — anything that
   stops it from renewing).
2. A's lease keeps counting down. Standby B is polling
   `try_acquire_or_renew` every `renew_interval_seconds` the whole time,
   getting `None` back (A's lease is still live) — nothing visible
   happens yet.
3. Once `expires_at` passes (worst case: just under one `ttl_seconds`
   after A's last successful renewal), B's next attempt sees an expired
   lease under a different holder_id and takes it — `term` increments,
   B is now leader.
4. B's `on_become_leader` fires; `run_coordinator.py`'s wiring means B
   was already blocked in `run_until_leader()` at startup, so this is
   the moment B actually starts its gRPC transport and dashboard/API.
5. Jobs B has durable records of but that were mid-flight when A died are
   handled by the same `restore_from_persistence` / in-flight-job
   recovery path any single-coordinator restart already goes through —
   not special HA logic.
6. If A comes back later (process restarted by a supervisor), it starts
   in `run_until_leader()` again, finds B's lease live, and sits as
   standby — it does not fight B for leadership or cause a split-brain
   moment. (`term` having incremented is exactly the record of this
   handover, if you're inspecting `coordinator_leases` directly.)
7. **If A is still alive but loses its lease** (e.g. a long GC pause or
   disk stall made it miss enough renewals), `on_lose_leadership` fires
   the callback `run_coordinator.py` wires: it logs a `critical` line and
   calls `os._exit(1)` — hard process exit, not a graceful shutdown. See
   the comment on that callback in `run_coordinator.py` for why: a
   demoted process that kept running would still have its gRPC
   transport and web server up, so a worker or dashboard request could
   still land on it even though `scheduler_loop`/`submit_job` now
   correctly refuse to act — confusing `NotLeaderError`s/503s forever
   instead of a clean restart into standby. A process supervisor
   (systemd `Restart=on-failure`, a container orchestrator) is expected
   to bring it back up, at which point it re-enters `run_until_leader()`
   as a standby.

## Configuration

| Setting | Default | Notes |
|---|---|---|
| `--ha` | off | Required to enable any of this |
| `--coordinator-id` | `hostname:pid:random` | Stable identity across restarts, for logs/lease history |
| `GCON_HA_LEASE_TTL_SECONDS` | `10` | Also settable via `LeaderElector(ttl_seconds=...)` directly if constructing one outside `run_coordinator.py` |
| renew interval | `ttl_seconds / 3` | Not independently configurable via env var today — derived from the TTL |
| `--db` / `GCON_DATA_DIR` | `data/gcon_control_plane.db` | **Must** point every participating coordinator process at a DB file they can all actually reach — see below |

## Operational notes

- **Where can coordinator processes actually run?** The lease (and
  everything else in the control plane) lives in one SQLite file.
  Multiple processes on **one host** sharing that file is safe — SQLite
  serializes writers at the file level, and `try_acquire_or_renew`'s
  whole read-decide-write happens in one transaction. Multiple processes
  across **different hosts** sharing that file over a network filesystem
  (NFS, SMB, etc.) is a different story: SQLite's file-locking guarantees
  are well known to be unreliable over most network filesystems. This
  document does not claim cross-host HA is safe with the current SQLite
  backend — treat same-host, multi-process HA (e.g. a supervisor running
  two `run_coordinator.py --ha` processes so one can restart without a
  full outage) as the well-supported case today. See
  `docs/TRANSPORT_AND_PERSISTENCE.md`'s SQLite → PostgreSQL migration
  path for what closes this gap for real cross-host HA.
- **A standby serves nothing today.** As noted above, `run_coordinator.py`
  doesn't start the dashboard/API until `run_until_leader()` returns —
  there is currently no "standby answers read-only queries" behavior
  actually wired up in the entry point, even though `LeaderElector`
  itself would support it (nothing stops you from calling
  `leader_elector.start()` instead of blocking on
  `run_until_leader()` and serving anyway, gating only writes — that's
  just not what the shipped script does). If you want that behavior,
  it's a `run_coordinator.py` change, not a `LeaderElector` limitation.
- **No VIP/load-balancer wiring is included.** After a failover, agents
  and dashboard users need to reach the *new* leader's address. GCON
  does not provide a floating IP, DNS failover, or load-balancer
  health-check integration — bring your own (an LB health-checking
  `GET /api/v1/health` on each coordinator and routing only to whichever
  one reports `"healthy"` and is actually leading is the common pattern,
  but note `/health` today doesn't itself expose "am I the leader" — see
  next point).
- **Checking which process is leader.** There is no dedicated
  `/api/v1` field exposing leadership state as of this writing. The
  most direct way to check is reading the `coordinator_leases` table
  directly against the shared DB:
  ```bash
  sqlite3 /shared/gcon_control_plane.db \
    "SELECT holder_id, term, expires_at FROM coordinator_leases WHERE lease_name = 'coordinator-leader';"
  ```
  If you need this exposed over HTTP for a load balancer or monitoring
  system, that's a real, small, currently-unbuilt addition — not
  something to assume exists.

## Related: receipt/node identity, not coordinator identity

This document is about *coordinator* failover — which coordinator
process is authoritative. It's unrelated to (and shouldn't be confused
with) node-level identity in receipts: since a recent fix, an execution
receipt can carry an `attested_node_id` binding it to the mTLS-verified
identity of the *worker* node that ran the job (see
`docs/TRANSPORT_AND_PERSISTENCE.md`'s mutual-authentication section and
`ExecutionVerifier.create_receipt`). That mechanism doesn't change or
depend on which coordinator process happens to be leading at the time —
any coordinator, active or (if it were extended to write while standby,
which it isn't today) standby, would sign a receipt with the same
verification logic.
