import time
import threading
import socket
import uuid
import itertools
import os
from collections import deque
from queue import Queue
from datetime import datetime, UTC

from .registry import NodeRegistry
from .scheduler import Scheduler
from .communication import CommunicationManager
from gcon.cluster.autoscaler import AutoScaler
from gcon.transport.local_transport import LocalTransport

from gcon.execution.verifier import ExecutionVerifier
from gcon.execution.artifact_registry import ArtifactRegistry
from gcon.execution.policy_engine import PolicyEngine
from gcon.execution.staking import StakeLedger
from gcon.storage.storage_manager import StorageManager
from gcon.events.event import Event
from gcon.events.event_types import EventType
from gcon.events.event_bus import EventBus
from gcon.workflow.workflow_engine import WorkflowEngine
from gcon.monitoring.health_service import HealthService
from gcon.monitoring.metrics import MetricsCollector
from gcon.dashboard.dashboard import Dashboard
from gcon.transport.config import TransportConfig

class NotLeaderError(RuntimeError):
    """Raised by a standby coordinator (see gcon.cluster.leader_election)
    when asked to do something only the active leader may do."""
    pass


class GCONCoordinator:
    """
    Coordinates GCON agents, job execution, and receipt management.
    """
    def __init__(self, transport=None, control_plane=None):
        # A real, stable identity for this coordinator process — not a
        # hardcoded display string. Hostname makes it recognizable in a
        # multi-host deployment; the short uuid disambiguates restarts.
        self.coordinator_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self.started_at = datetime.now(UTC)

        # Node offline-detection window is derived from the same
        # heartbeat_interval_seconds / heartbeat_miss_threshold config
        # the transport layer uses (env -> control-plane DB ->
        # default), not a value hardcoded independently in
        # NodeRegistry. Previously NodeRegistry always used a fixed
        # 10s timeout regardless of this config, so operators raising
        # the heartbeat interval (e.g. to reduce load when scaling to
        # hundreds of nodes) got mass false "offline" flips as soon as
        # any heartbeat took longer than 10s to arrive.
        transport_config = TransportConfig.load(control_plane)
        node_timeout_seconds = (
            transport_config.heartbeat_interval_seconds
            * transport_config.heartbeat_miss_threshold
        )
        self.registry = NodeRegistry(timeout_seconds=node_timeout_seconds)
        # Historical/durable node records restored from the control
        # plane (see restore_from_persistence). This is deliberately
        # separate from self.registry, which holds *live* scheduling
        # state (transport channel, heartbeat timer) for nodes that
        # are actually connected right now -- a coordinator restart
        # can restore what nodes existed, but not fabricate a live
        # connection to them; agents repopulate self.registry
        # themselves by reconnecting and calling register_agent().
        self.nodes = {}

        self.scheduler = Scheduler(self.registry, control_plane)
        self.communication = CommunicationManager(transport=transport)
        self.agents = {}
        self.event_bus = EventBus()
        
        self.jobs = {}
        self.jobs_lock = threading.RLock()
        self.job_queue = Queue()
        
        self.receipts = {}
        # Guards self.receipts. receive_receipt() (called from every
        # per-job _run_job worker thread as soon as a job completes)
        # inserts into this dict concurrently with reads from
        # get_receipts()/verify_all_receipts()/etc. -- notably from
        # health_check_loop's periodic compute_trust() call. Iterating
        # self.receipts.items()/.values() without holding this lock
        # (or a snapshot taken under it) races with those inserts and
        # raises "dictionary changed size during iteration".
        self.receipts_lock = threading.RLock()
        # Cache of job_id -> already-computed validate_proof() result.
        # A receipt's proof is immutable once stored (receive_receipt
        # only ever stores a brand-new receipt for a job_id, never
        # mutates one in place), so re-running the real HMAC check on
        # the same unchanged receipt every single time get_receipts()
        # is called -- previously every 3s, forever, via
        # health_check_loop -- was pure repeated work that only ever
        # grew with cumulative history, not with load. Guarded by
        # receipts_lock alongside self.receipts; invalidated in
        # receive_receipt() so a genuinely new/overwritten receipt for
        # a job_id is still verified fresh, exactly as before.
        self._receipt_verified_cache = {}
        # FIFO of job_ids whose receipt has arrived (or been
        # overwritten) but not yet been through validate_proof(). Lets
        # health_check_loop's periodic tick verify only what's new
        # since the last tick (see _drain_pending_receipt_verifications)
        # instead of re-scanning all of self.receipts every 3s to find
        # out what isn't cached yet -- that scan was itself O(total
        # receipts) even after the cache eliminated the crypto cost.
        self._pending_receipt_ids = deque()
        # Running totals mirroring _receipt_verified_cache's contents,
        # kept in lockstep by every code path that writes to that
        # cache (get_receipts() and _drain_pending_receipt_verifications).
        # Lets compute_trust() read a verification rate in O(1) on the
        # health tick instead of rebuilding/summing the full receipt
        # list every call.
        self._verified_receipt_count = 0
        self._unverified_receipt_count = 0
        # Per-replica receipt trail for verify-tagged jobs (see
        # gcon.execution.replication and _run_replicated_job). Every
        # replica keeps its own independently-signed receipt here,
        # keyed by job_id -> list of receipts (one per node that
        # actually completed), so which specific node produced which
        # specific signed result is never collapsed into a single
        # record. self.receipts (job_id -> one receipt) still also
        # gets one canonical entry per verify-tagged job -- the first
        # replica to complete -- purely so every existing consumer of
        # self.receipts (get_receipts/get_receipt_detail/health-check
        # verification cache/dashboard) keeps working unchanged for
        # verify-tagged jobs too; those consumers surfacing the full
        # replica list is follow-up work, not done here.
        self.replica_receipts = {}
        self.replica_receipts_lock = threading.RLock()
        self.artifact_registry = ArtifactRegistry() 
        self.storage_manager = StorageManager()
        self.workflow_engine = WorkflowEngine(self)
        self.health_service = HealthService(self)
        self.verifier = ExecutionVerifier()
        # policy.json (or GCON_POLICY_FILE) at the repo root -- this
        # file already existed and looked authoritative, but nothing
        # ever loaded it at runtime until now (see PolicyEngine's
        # docstring for the full history). Missing/malformed file
        # falls back to PolicyEngine's built-in defaults, same as
        # before.
        self.policy_engine = PolicyEngine(
            policy_file=os.environ.get("GCON_POLICY_FILE", "policy.json")
        )
        # self.jobs/self.receipts are unbounded in-memory dicts -- every
        # job ever submitted stayed in memory for the coordinator's
        # entire lifetime, which is a real problem at the job volumes
        # this project is aiming for (10k-15M+ jobs). See
        # _evict_completed_if_over_capacity for what gets evicted, when,
        # and the honest fidelity limits of falling back to the DB for
        # an evicted job (jobs are only partially durable today -- see
        # that method's docstring). Defaults chosen to keep a generous
        # recent-history window for the dashboard while still bounding
        # memory for a long-running coordinator; override via env for a
        # specific deployment's real memory budget.
        self._max_jobs_in_memory = int(os.environ.get("GCON_MAX_JOBS_IN_MEMORY", "5000"))
        self._max_receipts_in_memory = int(os.environ.get("GCON_MAX_RECEIPTS_IN_MEMORY", "5000"))
        self.scheduler_paused = False

        # Bounded, in-memory trust-score time series, sampled every
        # health_check_loop tick (see check_cluster_health). Never
        # pre-seeded — it only ever contains real, live-computed
        # samples taken while this coordinator has been running.
        self._trust_history = deque(maxlen=500)
        # Last observed overall health state / set of unverified
        # receipt ids, used only to detect *transitions* so we emit
        # one HEALTH_DEGRADED/RECOVERED or RECEIPT_VERIFICATION_FAILED
        # event when something actually changes, instead of publishing
        # the same event on every health-check tick.
        self._last_health_state = None
        self._known_unverified_receipt_ids = set()

        # Signals scheduler_loop/health_check_loop to exit; set by
        # shutdown(). Without this there is no way to stop these
        # daemon threads short of process exit, so every coordinator
        # ever constructed (e.g. one per test) keeps running forever.
        self._shutdown_event = threading.Event()

        # Durable control-plane handle (jobs/nodes/receipts survive a
        # restart in its DB, see gcon.persistence). Optional: local-only
        # coordinators (most existing tests, tests/stages/*, the default
        # LocalTransport path) pass none and simply run with no
        # persisted history, exactly as before.
        self.control_plane = control_plane
        # Bonded-stake ledger (see gcon.execution.staking). Requires a
        # real control-plane DB to persist balances against, same
        # restriction as the receipt/job durability above -- a
        # local-only coordinator with no control_plane simply has no
        # staking (gating is off by default anyway; see StakeLedger).
        self.stake_ledger = StakeLedger(control_plane) if control_plane is not None else None
        # Outbound webhook delivery (see gcon.transport.webhooks).
        # Same control_plane-required guard as everything else durable
        # here -- _dispatch_webhook() already no-ops without one, this
        # just means there's also no background sender thread to run.
        self.webhook_dispatcher = None
        if control_plane is not None:
            from gcon.transport.webhooks import WebhookDispatcher
            self.webhook_dispatcher = WebhookDispatcher(control_plane, event_bus=self.event_bus)
            self.webhook_dispatcher.start()
        # HA leader election (see gcon.cluster.leader_election) is set
        # up by run_coordinator.py, not here -- unlike webhooks/staking
        # it needs an operator-chosen coordinator identity + whether
        # this process should even participate, which __init__ doesn't
        # take as arguments. None here just means "not running as part
        # of an HA cluster" (the default/back-compat case).
        self.leader_elector = None
        # DB-level retention sweep state (see _maybe_sweep_retention).
        # Lazily constructs RetentionPolicy on first tick rather than
        # here so env vars set after coordinator construction (common
        # in tests) are still picked up.
        self._retention_policy = None
        self._last_retention_sweep = 0.0
        self.restore_from_persistence()

        self.scheduler_thread = threading.Thread(
                target=self.scheduler_loop,                       
                daemon=True
        )       
        self.scheduler_thread.start()

        self.health_check_thread = threading.Thread(
                target=self.health_check_loop,
                daemon=True
        )
        self.health_check_thread.start()

        # AutoScaler is owned here, not by PresentationLayer, so the
        # dashboard's manual scale_up/scale_down buttons and this
        # loop's automatic decisions share one instance -- and
        # therefore one `scaled_nodes` bookkeeping list. Two separate
        # AutoScaler(coordinator) instances (previously: one built
        # fresh inside PresentationLayer.__init__, none anywhere else)
        # would each track their own idea of "which nodes did I
        # create", so a manual scale-down could never remove a node
        # the automatic loop had created, and vice versa -- see
        # gcon.cluster.autoscaler's AutoScaler for the bookkeeping
        # this shares.
        self.autoscaler = AutoScaler(self)

        # Opt-in: GCON_AUTOSCALE_ENABLED=1 (default off). Automatically
        # creating/destroying cluster nodes is a real behavior change
        # that every existing caller of GCONCoordinator() -- most
        # tests included -- should not suddenly get for free just by
        # constructing a coordinator. GCON_AUTOSCALE_INTERVAL_SECONDS
        # (default 5.0) controls how often check_scale() runs; slower
        # than health_check_loop's 3s on purpose, since scaling
        # decisions should react to a sustained gap between pending
        # jobs and idle nodes, not every single momentary blip.
        self.autoscale_enabled = os.environ.get("GCON_AUTOSCALE_ENABLED", "0") in ("1", "true", "True")
        self.autoscale_thread = None
        if self.autoscale_enabled:
            self.autoscale_thread = threading.Thread(
                target=self.autoscale_loop,
                daemon=True,
            )
            self.autoscale_thread.start()

        print("GCON Coordinator initialized.")

    def restore_from_persistence(self):
        """
        Load jobs, receipts, and node records already durably written
        to the control-plane DB (by a previous run of this coordinator
        / its GrpcTransport, via JobRepository / ReceiptRepository /
        NodeRepository) back into the in-memory views the dashboard
        and API read from, so a coordinator restart doesn't wipe the
        dashboard's history even though the underlying rows survived.

        No-op if this coordinator wasn't given a control_plane (e.g.
        LocalTransport-based tests), and cleanly handles a fresh/empty
        database (list_all() -> [] on first boot, nothing to restore).
        Never raises -- a corrupt or unreachable DB should degrade to
        "empty history", not prevent the coordinator from starting.
        """
        if self.control_plane is None:
            return

        try:
            # Bounded, not list_all(): a coordinator that's been
            # running for months could otherwise have to load every
            # job it's ever seen just to restart. Non-terminal jobs
            # (pending/running) are loaded in full regardless -- they
            # need reconciliation below no matter how many there are
            # -- then topped up with the most recent terminal jobs, up
            # to the same cap in-memory eviction already uses
            # (_max_jobs_in_memory), so restore never hands the
            # in-memory store more than it would keep after its first
            # eviction pass anyway. See gcon.persistence.retention for
            # the complementary DB-side purge that keeps the
            # underlying table itself from growing unbounded.
            non_terminal = self.control_plane.jobs.list_non_terminal()
            recent_terminal = self.control_plane.jobs.list_recent_terminal(
                self._max_jobs_in_memory
            )
            # self.jobs is insertion-ordered oldest-first (get_jobs's
            # fast path relies on that to reverse it into newest-first
            # cheaply) -- recent_terminal comes back newest-first from
            # the DB, so re-sort the combined set back to ascending
            # submitted_at before inserting, rather than inserting in
            # DESC order and silently breaking that invariant.
            combined = sorted(non_terminal + recent_terminal, key=lambda j: j["submitted_at"])
            for job in combined:
                self.jobs[job["job_id"]] = {
                    "command": job["command"],
                    "node_id": None,
                    "status": job["status"],
                    "artifacts": [],
                    "created_at": job["submitted_at"],
                    "completed_at": job.get("completed_at"),
                    "result": job.get("result"),
                    "created_by": job.get("created_by"),
                    "workflow_id": job.get("workflow_id"),
                    "org_id": job.get("org_id"),
                }
        except Exception as e:
            print(f"[RESTORE] Failed to restore jobs from control plane: {e!r}")

        # Reconcile jobs that were still in flight (dispatched but not
        # yet marked completed/failed) when this coordinator last
        # stopped, whether cleanly or via a crash. Nothing else will
        # ever revisit them: dispatch only happens synchronously inside
        # submit_job(), there is no background loop that re-scans
        # "pending" jobs, and recover_jobs() -- the only code path that
        # un-sticks a "running" job -- is only ever triggered by a live
        # node-disconnect event, which these jobs can never generate
        # since their node_id was intentionally nulled out just above
        # (we have no way to know which node, if any, was still working
        # on them). Left alone they would sit in self.jobs forever
        # showing a stale "pending"/"running" status that no longer
        # reflects reality, invisible to any recovery path. Mark them
        # failed instead -- both in memory and back in the control
        # plane -- so they show up as needing resubmission rather than
        # silently never finishing.
        interrupted_statuses = {"pending", "running"}
        reconciled = 0
        for job_id, job in self.jobs.items():
            if job["status"] not in interrupted_statuses:
                continue
            job["status"] = "failed"
            job["completed_at"] = datetime.now(UTC).isoformat()
            job["result"] = {
                "error": (
                    "Job was still in flight when the coordinator was "
                    "last restarted and was not resumed; it will need "
                    "to be resubmitted."
                )
            }
            reconciled += 1
            if self.control_plane is not None:
                try:
                    self.control_plane.jobs.set_status(
                        job_id, "failed", result=job["result"], completed=True,
                    )
                except Exception as e:
                    print(
                        f"[RESTORE] Failed to persist reconciled status "
                        f"for job {job_id}: {e!r}"
                    )
        if reconciled:
            print(
                f"[RESTORE] Marked {reconciled} in-flight job(s) as "
                "failed after coordinator restart (interrupted, not resumed)."
            )

        try:
            # Bounded, same reasoning as jobs above -- see
            # ReceiptRepository.list_recent's docstring for why
            # receipts (unlike jobs) have no non-terminal subset that
            # must be loaded in full.
            for receipt in self.control_plane.receipts.list_recent(
                self._max_receipts_in_memory
            ):
                # `payload` is exactly the receipt dict the issuing side
                # (ExecutionVerifier.create_receipt locally, or
                # ReceiptGenerator.generate on a remote agent) built --
                # same shape self.receipts already stores at runtime
                # (receive_receipt), so no translation is needed here.
                payload = receipt.get("payload") or {}
                job_id = receipt.get("job_id") or payload.get("job_id")
                if job_id is None:
                    continue
                self.receipts[job_id] = payload
                # Queue it so its verification result (and contribution
                # to the running verified/unverified totals used by
                # compute_trust) gets computed the first time anything
                # asks -- the health tick's next drain, or a
                # get_receipts() call, whichever comes first -- instead
                # of silently sitting uncounted until some caller
                # happens to touch it. Safe without receipts_lock here:
                # this runs before scheduler_thread/health_check_thread
                # are started, so nothing else is touching self.receipts
                # yet.
                self._pending_receipt_ids.append(job_id)
        except Exception as e:
            print(f"[RESTORE] Failed to restore receipts from control plane: {e!r}")

        try:
            for node in self.control_plane.nodes.list_all():
                self.nodes[node["node_id"]] = node
        except Exception as e:
            print(f"[RESTORE] Failed to restore nodes from control plane: {e!r}")

        if self.jobs or self.receipts or self.nodes:
            print(
                f"[RESTORE] Restored {len(self.jobs)} job(s), "
                f"{len(self.receipts)} receipt(s), {len(self.nodes)} node "
                "record(s) from the control plane."
            )

    def get_persisted_nodes(self):
        """
        Historical node records restored from the control plane on
        boot (see restore_from_persistence), keyed by node_id. Unlike
        self.registry, these are not live/connected nodes -- an agent
        that hasn't reconnected since the restart will still appear
        here (with whatever status it last reported) but will not be
        schedulable until it registers again.
        """
        return dict(self.nodes)

    def register_agent(self, node):
        """
        Register a GCON agent with the coordinator.
        """
        self.registry.register(node)
        self.communication.register_node(node)

        # For gRPC-connected agents, grpc_transport.py's Register() RPC
        # handler already writes this node's row to control_plane.nodes
        # (with the real transport endpoint, TLS auth fingerprint, etc.)
        # BEFORE this method ever runs. But this method is also called
        # directly for LocalTransport/manually-constructed nodes (see
        # GCONNode, and this coordinator's own tests), which have no
        # equivalent RPC handler to do that write -- so with a real
        # control_plane attached, jobs run on such a node could never
        # actually persist a receipt at all: receipts.job_id/node_id
        # are FK columns, and the INSERT would fail every single time
        # with no row for this node_id to reference (this exact gap is
        # what surfaced the receipts.upload() IntegrityError-swallowing
        # bug fixed alongside this one -- that bug is what let this one
        # stay invisible). upsert() is idempotent, so this is a safe
        # no-op for a node already registered via the real gRPC path.
        if self.control_plane is not None:
            try:
                self.control_plane.nodes.upsert(
                    node_id=node.node_id,
                    hostname=node.node_id,
                    status=node.status,
                    org_id=getattr(node, "org_id", None),
                )
            except Exception as e:
                print(f"[WARN] Failed to persist node registration for '{node.node_id}': {e!r}")

        print(f"Node '{node.node_id}' registered successfully.")
        self.event_bus.publish(
            Event(
                timestamp=datetime.now(UTC),
                event_type=EventType.NODE_REGISTERED,
                source="Coordinator",
                payload={
                    "node_id": node.node_id,
                    "status": node.status
        },
    )
)
    
    def submit_job(
        self, job_id, command, artifacts=None, created_by=None, workflow_id=None,
        org_id=None, kind="command", requires=None, stages=None, dataset_artifacts=None,
        callback_url=None, verify=None,
    ):
        """
        Submit a new job to the coordinator.

        `created_by` is the user_id of the authenticated principal that
        submitted this job (from an API key owner or, in the future, a
        dashboard session) -- it is real ownership metadata, never a
        placeholder, and is left as None when the submission path has
        no authenticated identity attached (e.g. internal/system jobs).
        `workflow_id` links a job back to the workflow that generated
        it, when applicable. `org_id` is the company this job should
        be attributed to for the dashboard's Companies panel and
        org-scoped API access -- normally derived from `created_by`'s
        organization (see api_v1.py's submit_job route), left None for
        jobs with no company association.

        `kind` is one of "command" (default -- today's plain,
        unstructured behavior), "resourced" (adds `requires`, matched
        against node capabilities at scheduling time so the job never
        lands on a node that can't run it), or "staged" (adds
        `stages`, a per-checkpoint progress-reporting contract -- see
        GCONAgent.execute_job's stage_report_path). GCON still never
        interprets what a job's command actually does; these only
        change scheduling (`requires`) and what proof looks like
        (`stages`), never how the command itself runs.

        `dataset_artifacts` are IDs of artifacts already registered
        with this coordinator (not filepaths, unlike `artifacts`) that
        this job declares as its input data. Their content hashes get
        folded into the job's receipt input_hash at completion (see
        _run_job), so which exact data version fed a run becomes part
        of the tamper-evident record.

        `callback_url`, if given, is POSTed a signed JSON payload when
        this job reaches a terminal state (completed/failed/
        cancelled) -- see gcon.transport.webhooks -- so the submitter
        doesn't have to poll. Independent of, and in addition to, any
        standing org-level webhook subscription for the same org_id.

        `verify`, if given, e.g. {"replicas": 2, "tolerance": 0.02},
        dispatches this job to N independently-selected idle nodes
        instead of one, and compares their results for agreement --
        see gcon.execution.replication and _run_replicated_job. This
        is orthogonal to `kind`/`requires`/`stages`: a "resourced" or
        "staged" job can also ask for replication. None (default)
        means today's single-node dispatch, completely unchanged.
        Independent of HMAC signing -- each replica still gets its
        own normal signed receipt; `verify` only adds a derived
        agreement annotation on top, it never replaces or weakens the
        per-node signature.

        Raises NotLeaderError if this coordinator is running as part
        of an HA cluster (leader_elector set -- see
        gcon.cluster.leader_election) and is currently a standby, not
        the leader. A standby's job_queue is never drained (see
        scheduler_loop's HA gating), so silently accepting a
        submission here would durably persist the job but leave it
        pending forever -- rejecting it immediately, loudly, lets the
        caller retry against the actual leader instead.
        """
        if self.leader_elector is not None and not self.leader_elector.is_leader:
            raise NotLeaderError(
                "This coordinator is a standby, not the current leader; "
                "resubmit to the active coordinator."
            )
        if artifacts is None:
             artifacts = []

        if kind not in ("command", "resourced", "staged"):
            raise ValueError(
                f"Unknown job kind '{kind}' -- must be one of "
                "'command', 'resourced', 'staged'."
            )

        if verify is not None:
            replicas = verify.get("replicas", 2)
            if not isinstance(replicas, int) or replicas < 2:
                raise ValueError(
                    f"verify.replicas must be an int >= 2 (got {replicas!r}) "
                    "-- replication needs at least 2 independent nodes to "
                    "compare against each other."
                )

        dataset_artifacts = dataset_artifacts or []
        for artifact_id in dataset_artifacts:
            if self.artifact_registry.get_artifact(artifact_id) is None:
                raise ValueError(
                    f"dataset_artifacts references unknown artifact "
                    f"'{artifact_id}' -- register it first."
                )

        with self.jobs_lock:
            if job_id in self.jobs:
                raise ValueError(f"Job '{job_id}' already exists.")
        artifact_ids = []

        for filepath in artifacts:
            artifact_id = self.artifact_registry.register_artifact(filepath)
            artifact_ids.append(artifact_id)
        
        
        with self.jobs_lock:    
            self.jobs[job_id] = {
                "command": command,
                "node_id": None,
                "status": "pending",
                "artifacts": artifact_ids,
                "created_at": datetime.now(UTC).isoformat(),
                "completed_at": None,
                "created_by": created_by,
                "workflow_id": workflow_id,
                "org_id": org_id,
                "kind": kind,
                "requires": requires,
                "stages": stages,
                "dataset_artifacts": dataset_artifacts,
                "callback_url": callback_url,
                "verify": verify,
    }
        self.queue_job(job_id)

        if self.control_plane is not None:
            # Persist immediately, not just lazily at dispatch time
            # (grpc_transport.py's send_job() also calls ensure_exists,
            # but only once a node picks the job up -- without this, a
            # job that's still pending when the coordinator restarts,
            # or a LocalTransport-based coordinator that never dispatches
            # over gRPC at all, would never get a durable row, and
            # org_id specifically would never make it into the DB for
            # jobs dispatched through send_job's no-op ensure_exists,
            # since that call site doesn't have org_id in scope).
            try:
                self.control_plane.jobs.ensure_exists(
                    job_id, command, workflow_id=workflow_id,
                    created_by=created_by, org_id=org_id,
                )
                if callback_url:
                    self.control_plane.db.execute(
                        "UPDATE jobs SET callback_url = ? WHERE job_id = ?",
                        (callback_url, job_id),
                    )
            except Exception as e:
                print(f"[PERSIST] Failed to persist job '{job_id}': {e!r}")

        self.event_bus.publish(
            Event(
                timestamp=datetime.now(UTC),
                event_type="JOB_SUBMITTED",
                source="Coordinator",
                payload={
                    "job_id": job_id,
                    "command": command,
                    "artifacts": artifact_ids,
                    "created_by": created_by,
                    "workflow_id": workflow_id,
                    "org_id": org_id,
        },
    )
)
        print(f"[QUEUE] Job {job_id} queued")
        print(f"[QUEUE] Pending jobs: {self.job_queue.qsize()}")
    
    def assign_job(self, job_id):
        """
        Assign a job to an available node and execute it.
        """

        if job_id not in self.jobs:
            raise ValueError(f"Job '{job_id}' does not exist.")

        job = self.jobs[job_id]
        
        if job["status"] != "pending":
    # Already assigned/running/completed -- most likely a race
    # with the background scheduler_loop thread, which also
    # consumes the job queue. Assigning twice would run the
    # same job on two nodes.
            print(
                f"[QUEUE] Job {job_id} is already '{job['status']}', "
                "skipping re-assignment."
    )
            return

        verify_cfg = job.get("verify")
        if verify_cfg:
            self._assign_replicated_job(job_id, job, verify_cfg)
            return

        node = self.scheduler.select_node(requires=job.get("requires"))

        if node is None:
            if job.get("requires"):
                raise RuntimeError(
                    f"No available node satisfies job '{job_id}'s requirements "
                    f"({job['requires']}) -- either none are idle right now, or "
                    "none have matching reported capabilities."
                )
            raise RuntimeError("No available nodes to execute the job.")

    # Mark node and job as busy/running. select_node() worked from a
    # snapshot, so the node can have been deregistered (e.g. a
    # concurrent "Scale Down") in the window between that snapshot
    # and this heartbeat call. That race is the same recoverable
    # "nothing to assign to right now" condition as node is None
    # above, not a bug -- re-raise as RuntimeError so scheduler_loop
    # requeues the job and retries, instead of the underlying
    # ValueError killing the scheduler thread with nothing to
    # restart it.
        node.status = "busy"

        try:
            self.registry.heartbeat(
                node.node_id,
                "busy",
                node.heartbeat()["timestamp"]
            )
        except ValueError:
            raise RuntimeError(
                f"Node '{node.node_id}' was deregistered before it could be assigned."
            )

        job["status"] = "running"
        job["node_id"] = node.node_id
        
        thread = threading.Thread(
        target=self._run_job,
        args=(node, job_id),
        daemon=True
    )

      
        self.event_bus.publish(
            Event(
                timestamp=datetime.now(UTC),
                event_type="JOB_STARTED",
                source="Scheduler",
                payload={
                    "job_id": job_id,
                    "node_id":node.node_id,
        },
    )
)
        thread.start()
        return 

    def _assign_replicated_job(self, job_id, job, verify_cfg):
        """
        Claim N independent idle nodes for a verify-tagged job and
        dispatch _run_replicated_job on them, instead of the single
        node + _run_job path assign_job normally takes.

        Each scheduler.select_node() call is independently atomic
        (see NodeRegistry.claim_best_idle_node) -- calling it N times
        in a row can never claim the same node twice, since a node
        this call just claimed is marked busy before the lock is
        released and so is no longer idle for the next call. If fewer
        than N nodes are available, every node already claimed in
        this attempt is released back to idle before raising, so a
        failed replicated dispatch never leaves nodes stranded busy
        with nothing actually running on them.
        """
        replicas = verify_cfg.get("replicas", 2)
        nodes = []

        for _ in range(replicas):
            node = self.scheduler.select_node(requires=job.get("requires"))
            if node is None:
                for claimed in nodes:
                    claimed.status = "idle"
                    try:
                        self.registry.heartbeat(
                            claimed.node_id, "idle", claimed.heartbeat()["timestamp"]
                        )
                    except ValueError:
                        pass
                raise RuntimeError(
                    f"Only {len(nodes)}/{replicas} nodes available to satisfy "
                    f"job '{job_id}'s verification replica count "
                    f"(requires={job.get('requires')})."
                )
            nodes.append(node)

        for node in nodes:
            node.status = "busy"
            try:
                self.registry.heartbeat(
                    node.node_id, "busy", node.heartbeat()["timestamp"]
                )
            except ValueError:
                # This node was deregistered in the window between being
                # claimed and this heartbeat call -- release every other
                # claimed node the same way a failed initial claim does
                # above, then let assign_job's caller retry, same
                # recoverable-condition contract as the single-node path.
                for other in nodes:
                    if other is node:
                        continue
                    other.status = "idle"
                    try:
                        self.registry.heartbeat(
                            other.node_id, "idle", other.heartbeat()["timestamp"]
                        )
                    except ValueError:
                        pass
                raise RuntimeError(
                    f"Node '{node.node_id}' was deregistered before it could "
                    f"be assigned as a replica for job '{job_id}'."
                )

        job["status"] = "running"
        # node_id holds the first replica for backward compatibility with
        # every existing job-detail consumer that expects a single node_id;
        # replica_node_ids is the real, complete list.
        job["node_id"] = nodes[0].node_id
        job["replica_node_ids"] = [n.node_id for n in nodes]

        thread = threading.Thread(
            target=self._run_replicated_job,
            args=(nodes, job_id),
            daemon=True,
        )

        self.event_bus.publish(
            Event(
                timestamp=datetime.now(UTC),
                event_type="JOB_STARTED",
                source="Scheduler",
                payload={
                    "job_id": job_id,
                    "node_id": nodes[0].node_id,
                    "replica_node_ids": job["replica_node_ids"],
                },
            )
        )
        thread.start()

    def _attested_node_id(self, node_id):
        """
        The mTLS-authenticated identity the control plane has on file
        for `node_id`, if any -- see grpc_transport.py's Register
        handler (only ever sets this after verifying the connecting
        certificate's Common Name matches the claimed node_id) and
        NodeRepository.upsert's auth_fingerprint COALESCE handling
        (preserves it across later registration calls that don't pass
        one, e.g. register_agent's own post-Register upsert).

        Returns None -- not an error -- when there's no control_plane
        at all, when this node_id has no row yet, or when it connected
        over a transport with no cryptographic identity (e.g.
        LocalTransport, used for in-process/dev/test nodes). Passing
        None through to create_receipt()/generate_execution_proof()
        means "nothing to attest," not "attested and empty" -- see
        their docstrings.
        """
        if self.control_plane is None:
            return None
        try:
            info = self.control_plane.nodes.get(node_id)
        except Exception as e:
            print(f"[WARN] Could not look up auth_fingerprint for '{node_id}': {e!r}")
            return None
        if info is None:
            return None
        return info.get("auth_fingerprint")

    def receive_receipt(self, job_id, receipt):
        """
        Store a receipt for a completed job.
        """

        if job_id not in self.jobs:
            raise ValueError(f"Job '{job_id}' does not exist.")

        with self.receipts_lock:
            self.receipts[job_id] = receipt
            # A fresh/overwritten receipt invalidates any cached
            # verification result from a previous receipt that used
            # to live at this job_id -- see _receipt_verified_cache --
            # and its contribution to the running totals, since those
            # totals must stay in lockstep with the cache's contents.
            had_cached = self._receipt_verified_cache.pop(job_id, None)
            if had_cached is True:
                self._verified_receipt_count -= 1
            elif had_cached is False:
                self._unverified_receipt_count -= 1
            # Queue it for verification -- picked up either by the next
            # health-tick drain or by the next get_receipts() call,
            # whichever comes first.
            self._pending_receipt_ids.append(job_id)

        print(f"Receipt received for job '{job_id}'.")

    def receive_replica_receipt(self, job_id, receipt, is_primary):
        """
        Store one replica's independently-signed receipt in the
        per-replica trail (self.replica_receipts), for a verify-tagged
        job. Called once per node that completed, from
        _run_replicated_job -- unlike receive_receipt (which always
        overwrites the single job_id -> receipt slot), this appends,
        so every replica's own signed record survives even though
        they all share the same job_id.

        `is_primary` additionally routes this same receipt through
        receive_receipt() -- storing it in self.receipts too -- so
        every existing consumer of self.receipts (get_receipts,
        get_receipt_detail, the health-check verification cache, the
        dashboard) keeps working for verify-tagged jobs without
        modification, seeing the first replica's receipt as if it
        were an ordinary single-node job. The complete replica list
        lives only in self.replica_receipts for now; surfacing all N
        receipts in the dashboard/API is follow-up work, not done
        here.
        """
        with self.replica_receipts_lock:
            self.replica_receipts.setdefault(job_id, []).append(receipt)

        if is_primary:
            self.receive_receipt(job_id, receipt)

    def get_job_status(self, job_id):
        """
        Get the current status of a job.
        """

        if job_id not in self.jobs:
            raise ValueError(f"Job '{job_id}' does not exist.")

        return self.jobs[job_id]
    
    def check_cluster_health(self):
        """
        Check node health and recover jobs from failed nodes, then
        sample live health/trust state and publish events for any
        real change (never a hardcoded/synthetic tick) so the
        notification system and Trust Center's history chart stay
        current without polling from the request path.
        """
        offline_nodes = self.registry.check_node_health()

        for node_id in offline_nodes:
            print(f"Node '{node_id}' marked OFFLINE")
            self.event_bus.publish(Event(
                timestamp=datetime.now(UTC),
                event_type="NODE_OFFLINE",
                source="Coordinator",
                payload={"node_id": node_id},
            ))
            self.recover_jobs(node_id)

        self._sample_health_and_trust()
        self._maybe_sweep_retention()

    def _maybe_sweep_retention(self):
        """
        Runs the DB-level retention purge (see gcon.persistence
        .retention) at most once every GCON_RETENTION_SWEEP_INTERVAL_
        SECONDS (default 300 = 5 min) -- check_cluster_health ticks
        every 3s, and there is no reason to re-run a DELETE-heavy
        sweep that often. A no-op with no control_plane, or when
        no retention policy is actually configured (RetentionPolicy
        .enabled is False by default -- see its docstring for why
        this stays opt-in).
        """
        if self.control_plane is None:
            return
        interval = float(os.environ.get("GCON_RETENTION_SWEEP_INTERVAL_SECONDS", "300"))
        now = time.monotonic()
        if now - self._last_retention_sweep < interval:
            return
        self._last_retention_sweep = now

        if self._retention_policy is None:
            from gcon.persistence.retention import RetentionPolicy
            self._retention_policy = RetentionPolicy()
        if not self._retention_policy.enabled:
            return
        try:
            removed = self._retention_policy.sweep(self.control_plane)
            if any(removed.values()):
                print(f"[RETENTION] Purged: {removed}")
        except Exception as e:
            print(f"[RETENTION] Sweep failed: {e!r}")

    def _sample_health_and_trust(self):
        """
        Compute live health + trust exactly once per tick, record the
        trust sample, and publish an event only when the observed
        state actually changes (state transition or a receipt that
        newly failed/regained verification).
        """
        health = self.health_service.compute()
        # Verify only what's arrived since the last tick instead of
        # rebuilding the full receipts list every 3s -- see
        # _drain_pending_receipt_verifications. get_receipts() (full
        # list, still O(total receipts)) stays reserved for the
        # dashboard/API callers that actually need every receipt
        # rendered, which run at human/request cadence, not a fixed
        # timer that only gets tighter as history grows.
        newly_verified = self._drain_pending_receipt_verifications()
        with self.receipts_lock:
            verified_count = self._verified_receipt_count
            total_count = verified_count + self._unverified_receipt_count
        trust = self.health_service.compute_trust(
            verified_count=verified_count, total_count=total_count
        )

        self._trust_history.append({
            "timestamp": trust["computed_at"],
            "score": trust["trust_score"],
        })

        state = health["state"]
        if state != self._last_health_state:
            if self._last_health_state is not None:
                if state == "healthy":
                    self.event_bus.publish(Event(
                        event_type=EventType.HEALTH_RECOVERED,
                        source="HealthService",
                        payload={"state": state, "reason": health["reason"]},
                    ))
                else:
                    event_type = (
                        EventType.HEALTH_CRITICAL if state == "critical"
                        else EventType.HEALTH_DEGRADED
                    )
                    self.event_bus.publish(Event(
                        event_type=event_type,
                        source="HealthService",
                        payload={"state": state, "reason": health["reason"]},
                    ))
            self._last_health_state = state

        # Each receipt is verified exactly once, ever (see
        # _receipt_verified_cache), so a receipt_id can only ever
        # appear here on the single tick it was first verified --
        # equivalent to the old "re-check every receipt every tick"
        # loop in outcome (the FAILED/RECOVERED events still only fire
        # once per transition), just without redundantly re-examining
        # every already-known receipt on every subsequent tick.
        for job_id, receipt, is_valid in newly_verified:
            receipt_id = receipt.get("receipt_id", job_id)
            if not is_valid:
                if receipt_id not in self._known_unverified_receipt_ids:
                    self._known_unverified_receipt_ids.add(receipt_id)
                    self.event_bus.publish(Event(
                        event_type=EventType.RECEIPT_VERIFICATION_FAILED,
                        source="Verifier",
                        payload={"receipt_id": receipt_id, "job_id": job_id},
                    ))
                    # Slash the node that produced this receipt -- see
                    # gcon.execution.staking module docstring for why
                    # this specific trigger (a bad/missing HMAC
                    # signature) and not ordinary job failure. A no-op
                    # if this coordinator has no control_plane, or the
                    # node has nothing bonded.
                    if self.stake_ledger is not None:
                        node_id = receipt.get("agent_id")
                        if node_id:
                            slashed = self.stake_ledger.slash_for_failed_verification(
                                node_id, job_id=job_id, receipt_id=receipt_id,
                            )
                            if slashed is not None:
                                self.event_bus.publish(Event(
                                    event_type=EventType.NODE_STAKE_SLASHED,
                                    source="StakeLedger",
                                    payload={
                                        "node_id": node_id,
                                        "amount": slashed["slashed_total"],
                                        "reason": "failed_receipt_verification",
                                        "job_id": job_id,
                                        "receipt_id": receipt_id,
                                    },
                                ))
            elif receipt_id in self._known_unverified_receipt_ids:
                self._known_unverified_receipt_ids.discard(receipt_id)
                self.event_bus.publish(Event(
                    event_type=EventType.RECEIPT_VERIFICATION_RECOVERED,
                    source="Verifier",
                    payload={"receipt_id": receipt_id, "job_id": receipt.get("job_id", job_id)},
                ))

    def get_trust_score(self):
        """
        Return the current live trust score. Also used as the read
        path for /trust-center and the dashboard hero, so the number
        shown to a user is always freshly computed, not the
        background sample.
        """
        return self.health_service.compute_trust()

    def get_trust_history(self, limit=100):
        """
        Return the recorded trust-score time series, newest last,
        for the Trust Center's history chart. Entirely built from
        real samples taken by check_cluster_health(); empty until
        the coordinator has been running long enough to take one.
        """
        return list(self._trust_history)[-limit:]

    
    def on_node_disconnected(self, node_id):
        """
        Handle an immediate, known disconnect (e.g. the gRPC stream
        for this node closed) rather than waiting for the heartbeat
        timeout to notice.

        Mirrors what check_cluster_health()/registry.check_node_health()
        do for a *silently* lost node -- mark it offline in the
        registry (which also removes it from
        registry.available_nodes(), so the scheduler's pool can never
        dispatch new work to it) and recover any job it was running --
        but does so immediately instead of waiting out the full
        heartbeat window, during which a node we already know is gone
        could otherwise still be selected by the scheduler.
        """
        changed = self.registry.mark_offline(node_id)
        if not changed:
            # Either never registered with this coordinator's scheduler
            # (e.g. only ever known at the transport layer) or already
            # offline -- nothing further to do.
            return

        print(f"Node '{node_id}' marked OFFLINE (disconnected)")
        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC),
            event_type="NODE_OFFLINE",
            source="Coordinator",
            payload={"node_id": node_id, "reason": "disconnected"},
        ))
        self.recover_jobs(node_id)

    def recover_jobs(self, node_id):
        """
        Recover unfinished jobs assigned to a failed node.
        """

        print(f"Recovering jobs from '{node_id}'...")
        with self.jobs_lock:

            for job_id, job in list(self.jobs.items()):

                if job["node_id"] == node_id and job["status"] == "running":

                    print(f"Recovering job '{job_id}'")

                    # Reset the job
                    job["status"] = "pending"
                    job["node_id"] = None

                    # Reassign the job -- only for the job(s) we just
                    # reset above, not every job in self.jobs. This
                    # used to run unconditionally for the whole
                    # collection (a stray indent had it outside the
                    # `if`), which meant every recovery pass silently
                    # tried to re-assign every already-pending/running/
                    # completed job in the cluster too.
                    try:
                        self.assign_job(job_id)
                        print(f"Job '{job_id}' reassigned successfully.")
                    except RuntimeError as e:
                        print(f"Recovery failed for '{job_id}': {e}")
    
    
    def receive_heartbeat(self, heartbeat):
        """
        Process a heartbeat received from a node.
        """
        node_id = heartbeat["node_id"]
        status = heartbeat["status"]

        self.registry.heartbeat(
            heartbeat["node_id"],
            heartbeat["status"],
            heartbeat["timestamp"]
        )

        print(f"Heartbeat received from {node_id} ({status})")
    
    def receive_resource_report(self, resources):
        """
        Process a resource report received from a node.
        """

        node_id = resources["node_id"]

        self.registry.update_node_resources(node_id, resources)

        print(
            f"Resources updated for {node_id} "
            f"(CPU: {resources['cpu']}%, "
            f"Memory: {resources['memory']}%, "
            f"Jobs: {resources['running_jobs']})"
    )
        
    def dashboard(self):
        dashboard = Dashboard(self)

        self.event_bus.subscribe(dashboard.handle_event)

        dashboard.refresh()
        dashboard.display()

        return dashboard
    
    def _advance_workflow(self, job_id, job, success):
        """
        If `job` belongs to a workflow, tell the workflow engine it
        finished so the DAG actually advances -- dispatching newly-
        unblocked dependents on success, or blocking them on failure.

        Without this call, workflow_engine.process_completed_job()/
        process_failed_job() are never invoked by anything: a
        workflow's jobs would each run exactly once (its initial root
        jobs) and nothing downstream in the DAG would ever be
        submitted, silently stalling every workflow after its first
        layer. Best-effort: workflow bookkeeping must never prevent
        the job itself from being recorded as completed/failed above,
        so any failure here is caught and logged rather than raised.
        """
        workflow_id = job.get("workflow_id")
        if not workflow_id:
            return

        try:
            engine = self.workflow_engine
            workflow = engine.workflows.get(workflow_id)
            dag = engine.dags.get(workflow_id)
            state = engine.states.get(workflow_id)
            if workflow is None or dag is None or state is None:
                return

            if success:
                engine.process_completed_job(workflow, dag, state, job_id)
            else:
                engine.process_failed_job(dag, state, job_id)
        except Exception as e:
            print(f"[WARN] workflow advancement failed for job "
                  f"'{job_id}' (workflow '{workflow_id}'): {e}")

    def _run_job(self, node, job_id):
        """
        Execute a job in a background thread.

        Every mutation of the shared `job` dict below holds
        self.jobs_lock. Without it, this method races with
        recover_jobs() -- a concurrent heartbeat-timeout/disconnect
        for `node` (a real, observed scenario over a flaky remote
        connection, not just a theoretical one) can see this job
        still "running" at the same moment this method is mid-flight
        waiting on a slow remote execution, reset its status back to
        "pending" and node_id to None, and reassign it elsewhere --
        while this method, unaware that happened, goes on to write
        the *real* completion result a moment later. The net effect
        used to be a job correctly marked "completed" with a real
        result, but node_id stuck at whatever the race left it at
        (typically None) -- right, but with the audit trail silently
        wrong about which node actually did the work.
        """

        job = self.jobs[job_id]

        # Only "staged" jobs need anything communicated to the agent
        # itself -- "resourced"'s `requires` was already consumed by
        # the scheduler back in assign_job(), before this node was
        # even chosen, so it has no reason to travel any further.
        dispatch_metadata = None
        if job.get("kind") == "staged":
            dispatch_metadata = {"kind": "staged", "stages": job.get("stages")}

        try:
            response = self.communication.send_job(
                node.node_id,
                job_id,
                job["command"],
                metadata=dispatch_metadata,
            )

            result = response["result"]

        except Exception as e:
            # Anything going wrong here (network error, agent crash,
            # bad response shape, etc.) must NOT leave the job
            # "running" and the node "busy" forever.
            print(f"[ERROR] _run_job failed for '{job_id}' on "
                  f"'{node.node_id}': {e}")

            # Best-effort: tell the node to stop the job before freeing
            # it back to idle. Without this, a dispatch that failed
            # from the *coordinator's* side (timeout, bad response,
            # etc.) while the agent is genuinely still alive and
            # working leaves that subprocess running orphaned on the
            # node -- which the scheduler can now immediately hand a
            # second job to, since nothing here ever told it to stop.
            # At scale, dispatch timeouts are a certainty, not an edge
            # case, so this isn't a rare double-booking: it's a
            # standing resource-contention risk that gets worse the
            # longer a busy cluster runs. cancel_job() itself never
            # blocks long (it either fails fast on an unreachable node
            # or fires a one-way message on the stream -- see
            # grpc_transport.py), and any failure here must not stop
            # this method from still freeing the node/failing the job
            # below, since a node the agent genuinely can't reach isn't
            # made worse by a cancel it'll never receive.
            try:
                self.communication.cancel_job(node.node_id, job_id)
            except Exception as cancel_error:
                print(f"[WARN] best-effort cancel_job failed for "
                      f"'{job_id}' on '{node.node_id}' (proceeding "
                      f"anyway): {cancel_error}")

            self._advance_workflow(job_id, job, success=False)

            with self.jobs_lock:
                cancelled = job.get("cancel_requested", False)
                job["status"] = "cancelled" if cancelled else "failed"
                job["completed_at"] = datetime.now(UTC).isoformat()
                job["result"] = {"status": "error", "message": str(e)}
            self._persist_job_status(job_id, job)
            self._dispatch_webhook(job_id, job, "JOB_FAILED" if job["status"] == "failed" else "JOB_CANCELLED")

            node.status = "idle"
            self.registry.heartbeat(
                node.node_id,
                "idle",
                node.heartbeat()["timestamp"]
            )

            self.event_bus.publish(
                Event(
                    timestamp=datetime.now(UTC),
                    event_type="JOB_CANCELLED" if cancelled else "JOB_FAILED",
                    source="Coordinator",
                    payload={
                        "job_id": job_id,
                        "node_id": node.node_id,
                        "error": str(e),
                    },
                )
            )
            self._evict_completed_if_over_capacity()
            return

        node.status = "idle"

        self.registry.heartbeat(
            node.node_id,
            "idle",
            node.heartbeat()["timestamp"]
        )

        heartbeat = node.heartbeat()
        self.receive_heartbeat(heartbeat)

        resources = node.report_resources()
        self.receive_resource_report(resources)

        with self.jobs_lock:
            # Re-affirm node_id here too (not just at original
            # dispatch) -- this is the actual node that produced
            # `result`, and this write happens under the same lock
            # recover_jobs() uses, so the two can no longer interleave.
            job["node_id"] = node.node_id

            if result["status"] == "success":
                job["status"] = "completed"
                job["completed_at"] = datetime.now(UTC).isoformat()
            else:
                cancelled = job.get("cancel_requested", False)
                job["status"] = "cancelled" if cancelled else "failed"
                job["completed_at"] = datetime.now(UTC).isoformat()

            job["result"] = result

        self._persist_job_status(job_id, job)
        self._dispatch_webhook(job_id, job, "JOB_COMPLETED" if job["status"] == "completed"
                                else ("JOB_CANCELLED" if job["status"] == "cancelled" else "JOB_FAILED"))

        if result["status"] == "success":
            # Generate a real, cryptographically signed receipt for
            # this execution using the coordinator's shared verifier
            # instance (so later verification uses the same key).
            try:
                # dataset_artifacts (declared at submission -- IDs of
                # artifacts already registered with this coordinator,
                # see submit_job) get folded into input_hash alongside
                # the command itself, so which exact data version fed
                # this run is part of the tamper-evident record, not
                # just the command string. Falls back to hashing the
                # command alone (today's behavior, byte-identical)
                # when a job declares none.
                dataset_artifacts = job.get("dataset_artifacts") or []
                if dataset_artifacts:
                    dataset_hashes = []
                    for artifact_id in dataset_artifacts:
                        artifact = self.artifact_registry.get_artifact(artifact_id)
                        if artifact:
                            dataset_hashes.append(artifact.sha256)
                    input_hash = self.verifier.hash_data({
                        "command": job["command"],
                        "dataset_artifacts": dataset_hashes,
                    })
                else:
                    input_hash = self.verifier.hash_data(job["command"])
                output_hash = self.verifier.hash_data(result.get("stdout", ""))
                receipt = self.verifier.create_receipt(
                    job_id, node.node_id, result, input_hash, output_hash,
                    attested_node_id=self._attested_node_id(node.node_id),
                )
                self.receive_receipt(job_id, receipt)

                # Persist this coordinator-signed receipt so it survives
                # a restart. Before this fix, nothing here ever reached
                # the control-plane DB -- the only receipt that did was
                # the separate, incompatible one agent_daemon.py used to
                # upload independently (Ed25519, signature nested one
                # level differently than this HMAC scheme expects), so
                # restore_from_persistence() had only the unverifiable
                # copy to reload after every restart. That agent-side
                # upload has been removed (see agent_daemon.py) now that
                # the coordinator's own signed receipt -- the one
                # verify_all_receipts()/validate_proof() actually
                # check -- is the one being saved here.
                if self.control_plane is not None:
                    try:
                        self.control_plane.receipts.upload(
                            job_id=job_id,
                            payload=receipt,
                            receipt_hash=self.verifier.hash_data(receipt),
                            node_id=node.node_id,
                            signature=receipt["proof"]["signature"],
                        )
                    except Exception as persist_error:
                        print(
                            f"[WARN] Failed to persist receipt for "
                            f"'{job_id}': {persist_error!r}"
                        )

                self.event_bus.publish(Event(
                    timestamp=datetime.now(UTC),
                    event_type="RECEIPT_GENERATED",
                    source="Coordinator",
                    payload={"job_id": job_id, "node_id": node.node_id},
                ))

                # Evaluate the now-signed receipt against policy.json
                # (max runtime/CPU/memory, GPU requirement). This is
                # necessarily after-the-fact -- runtime_seconds etc.
                # only exist once the job has already finished -- so a
                # violation can't reject or stop the job (it's already
                # over); it's a trust signal attached to the receipt,
                # surfaced in the dashboard (get_receipt_detail) and,
                # on failure, published as a real notification the
                # same way offline nodes and failed jobs already are.
                policy_report = self.policy_engine.evaluate(receipt)
                job["policy_report"] = policy_report
                if not policy_report["trusted"]:
                    failed_checks = [
                        c["message"] for c in policy_report["checks"] if not c["passed"]
                    ]
                    self.event_bus.publish(Event(
                        timestamp=datetime.now(UTC),
                        event_type="POLICY_VIOLATION",
                        source="Coordinator",
                        payload={
                            "job_id": job_id,
                            "node_id": node.node_id,
                            "failed_checks": failed_checks,
                        },
                    ))
            except Exception as e:
                print(f"[WARN] Receipt generation failed for '{job_id}': {e}")

            self.event_bus.publish(
                Event(
                    timestamp=datetime.now(UTC),
                    event_type="JOB_COMPLETED",
                    source="Coordinator",
                    payload={
                        "job_id": job_id,
                        "node_id": node.node_id,
                    },
                )
            )
            self._advance_workflow(job_id, job, success=True)
        else:
            cancelled = job.get("cancel_requested", False)
            self.event_bus.publish(
                Event(
                    timestamp=datetime.now(UTC),
                    event_type="JOB_CANCELLED" if cancelled else "JOB_FAILED",
                    source="Coordinator",
                    payload={
                        "job_id": job_id,
                        "node_id": node.node_id,
                    },
                )
            )
            self._advance_workflow(job_id, job, success=False)

        self._evict_completed_if_over_capacity()

    def _run_replicated_job(self, nodes, job_id):
        """
        Execute a verify-tagged job on all `nodes` in parallel and
        compare their results for agreement (see
        gcon.execution.replication). Each node's own dispatch/failure
        handling mirrors _run_job's single-node path exactly (same
        best-effort cancel_job on error, same node-freed-to-idle
        guarantee) -- the only difference is that failure of one
        replica does not by itself fail the job; the job only fails
        if too few replicas succeed to produce a usable result.

        Every successful replica gets its own independently
        HMAC-signed receipt via self.verifier (untouched, unmodified
        -- see replication.py's module docstring for why). The
        agreement result across replicas is attached as a receipt-level
        "execution_proof" field, outside the signed "proof" dict.
        """
        job = self.jobs[job_id]

        dispatch_metadata = None
        if job.get("kind") == "staged":
            dispatch_metadata = {"kind": "staged", "stages": job.get("stages")}

        results_lock = threading.Lock()
        successes = []  # list of (node, result) for nodes that completed
        failures = []   # list of (node, exception) for nodes that errored

        def run_one(node):
            try:
                response = self.communication.send_job(
                    node.node_id,
                    job_id,
                    job["command"],
                    metadata=dispatch_metadata,
                )
                result = response["result"]
            except Exception as e:
                print(f"[ERROR] _run_replicated_job failed for '{job_id}' on "
                      f"'{node.node_id}': {e}")
                try:
                    self.communication.cancel_job(node.node_id, job_id)
                except Exception as cancel_error:
                    print(f"[WARN] best-effort cancel_job failed for "
                          f"'{job_id}' on '{node.node_id}' (proceeding "
                          f"anyway): {cancel_error}")

                node.status = "idle"
                try:
                    self.registry.heartbeat(
                        node.node_id, "idle", node.heartbeat()["timestamp"]
                    )
                except ValueError:
                    pass

                with results_lock:
                    failures.append((node, e))
                return

            node.status = "idle"
            self.registry.heartbeat(
                node.node_id, "idle", node.heartbeat()["timestamp"]
            )
            heartbeat = node.heartbeat()
            self.receive_heartbeat(heartbeat)
            resources = node.report_resources()
            self.receive_resource_report(resources)

            with results_lock:
                successes.append((node, result))

        threads = [threading.Thread(target=run_one, args=(node,), daemon=True) for node in nodes]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with self.jobs_lock:
            job["replica_failures"] = [
                {"node_id": n.node_id, "error": str(e)} for n, e in failures
            ]

        # No replica produced a result at all -- fail the job exactly the
        # way the single-node path fails on a dispatch error. Every node
        # has already been freed to idle in run_one above.
        successful_results = [r for _, r in successes if r.get("status") == "success"]
        if not successful_results:
            self._advance_workflow(job_id, job, success=False)
            with self.jobs_lock:
                cancelled = job.get("cancel_requested", False)
                job["status"] = "cancelled" if cancelled else "failed"
                job["completed_at"] = datetime.now(UTC).isoformat()
                job["result"] = {
                    "status": "error",
                    "message": f"All {len(nodes)} replicas failed or errored.",
                }
            self._persist_job_status(job_id, job)
            self._dispatch_webhook(job_id, job, "JOB_FAILED" if job["status"] == "failed" else "JOB_CANCELLED")
            self.event_bus.publish(Event(
                timestamp=datetime.now(UTC),
                event_type="JOB_CANCELLED" if cancelled else "JOB_FAILED",
                source="Coordinator",
                payload={"job_id": job_id, "replica_node_ids": [n.node_id for n in nodes]},
            ))
            self._evict_completed_if_over_capacity()
            return

        # Primary result: the first replica to complete successfully.
        # Kept as job["node_id"]/job["result"] for every existing
        # consumer that expects a single node/result per job.
        primary_node, primary_result = successes[0]
        for n, r in successes:
            if r.get("status") == "success":
                primary_node, primary_result = n, r
                break

        with self.jobs_lock:
            job["node_id"] = primary_node.node_id
            job["status"] = "completed"
            job["completed_at"] = datetime.now(UTC).isoformat()
            job["result"] = primary_result

        self._persist_job_status(job_id, job)
        self._dispatch_webhook(job_id, job, "JOB_COMPLETED")

        # Build one input_hash for the whole group -- every replica ran
        # the same command against the same declared inputs, so this is
        # computed once, exactly like the single-node path in _run_job.
        try:
            dataset_artifacts = job.get("dataset_artifacts") or []
            if dataset_artifacts:
                dataset_hashes = []
                for artifact_id in dataset_artifacts:
                    artifact = self.artifact_registry.get_artifact(artifact_id)
                    if artifact:
                        dataset_hashes.append(artifact.sha256)
                input_hash = self.verifier.hash_data({
                    "command": job["command"],
                    "dataset_artifacts": dataset_hashes,
                })
            else:
                input_hash = self.verifier.hash_data(job["command"])

            # Per-successful-node output_hash + comparison inputs.
            comparable = []
            per_node_output_hash = {}
            for node, result in successes:
                if result.get("status") != "success":
                    continue
                output_hash = self.verifier.hash_data(result.get("stdout", ""))
                per_node_output_hash[node.node_id] = output_hash
                comparable.append({
                    "output_hash": output_hash,
                    "metrics": result.get("metrics", {}),
                })

            from gcon.execution.replication import compare_results, build_execution_proof
            verify_cfg = job.get("verify") or {}
            tolerance = verify_cfg.get("tolerance", 0.02)
            comparison = compare_results(comparable, tolerance=tolerance)
            witnesses = [node.node_id for node, r in successes if r.get("status") == "success"]
            execution_proof = build_execution_proof(
                witnesses=witnesses,
                comparison=comparison,
                replica_group_id=job_id,
            )

            first_success_seen = False
            for node, result in successes:
                if result.get("status") != "success":
                    continue
                output_hash = per_node_output_hash[node.node_id]
                try:
                    receipt = self.verifier.create_receipt(
                        job_id, node.node_id, result, input_hash, output_hash,
                        attested_node_id=self._attested_node_id(node.node_id),
                    )
                except ValueError as mismatch_error:
                    # create_receipt refuses to sign when the claimed
                    # node_id disagrees with the mTLS-authenticated
                    # identity on file -- a real, expected-under-attack
                    # outcome, not a bug. Scoped to just this node: the
                    # other replicas in this group are unaffected and
                    # still get their own receipts.
                    print(
                        f"[WARN] Refused receipt for job '{job_id}' replica "
                        f"'{node.node_id}': {mismatch_error}"
                    )
                    self.event_bus.publish(Event(
                        timestamp=datetime.now(UTC),
                        event_type="POLICY_VIOLATION",
                        source="Coordinator",
                        payload={
                            "job_id": job_id,
                            "node_id": node.node_id,
                            "failed_checks": [str(mismatch_error)],
                        },
                    ))
                    continue
                # Sibling to "proof", not inside it -- see replication.py's
                # module docstring for why this is deliberately unsigned.
                receipt["execution_proof"] = execution_proof

                # Policy is evaluated per-replica (each node's own
                # reported metrics can differ -- e.g. one ran hotter or
                # slower than another even in agreement) and attached
                # directly to that replica's own receipt, the same way
                # execution_proof is: a receipt-level field describing
                # that specific node's run, not a single job-wide verdict.
                policy_report = self.policy_engine.evaluate(receipt)
                receipt["policy_report"] = policy_report

                is_primary = not first_success_seen
                first_success_seen = True
                self.receive_replica_receipt(job_id, receipt, is_primary=is_primary)
                if is_primary:
                    # Keep job["policy_report"] pointing at the primary
                    # replica's own evaluation, so every existing
                    # consumer that reads it off the job dict (rather
                    # than the receipt) still sees a real, non-fabricated
                    # result for verify-tagged jobs too.
                    job["policy_report"] = policy_report

                if not policy_report["trusted"]:
                    failed_checks = [
                        c["message"] for c in policy_report["checks"] if not c["passed"]
                    ]
                    self.event_bus.publish(Event(
                        timestamp=datetime.now(UTC),
                        event_type="POLICY_VIOLATION",
                        source="Coordinator",
                        payload={
                            "job_id": job_id,
                            "node_id": node.node_id,
                            "failed_checks": failed_checks,
                        },
                    ))

                # Confirmed safe (receipts.py's ReceiptRepository dedupes
                # on receipt_hash, not job_id -- each replica's receipt
                # has a distinct node_id/signature, so a distinct hash;
                # multiple replicas persisting under the same job_id is
                # exactly what list_for_job() and the UNIQUE(receipt_hash)
                # idempotency guarantee were already built to support).
                if self.control_plane is not None:
                    try:
                        self.control_plane.receipts.upload(
                            job_id=job_id,
                            payload=receipt,
                            receipt_hash=self.verifier.hash_data(receipt),
                            node_id=node.node_id,
                            signature=receipt["proof"]["signature"],
                        )
                    except Exception as persist_error:
                        print(
                            f"[WARN] Failed to persist replica receipt for "
                            f"'{job_id}' node '{node.node_id}': {persist_error!r}"
                        )

                self.event_bus.publish(Event(
                    timestamp=datetime.now(UTC),
                    event_type="RECEIPT_GENERATED",
                    source="Coordinator",
                    payload={
                        "job_id": job_id,
                        "node_id": node.node_id,
                        "replica_group_id": job_id,
                    },
                ))

            if not comparison["agree"]:
                self.event_bus.publish(Event(
                    timestamp=datetime.now(UTC),
                    event_type="EXECUTION_DISPUTED",
                    source="Coordinator",
                    payload={
                        "job_id": job_id,
                        "witnesses": witnesses,
                        "mismatches": comparison["mismatches"],
                    },
                ))
        except Exception as e:
            print(f"[WARN] Replicated receipt generation failed for '{job_id}': {e}")

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC),
            event_type="JOB_COMPLETED",
            source="Coordinator",
            payload={"job_id": job_id, "node_id": primary_node.node_id},
        ))
        self._advance_workflow(job_id, job, success=True)
        self._evict_completed_if_over_capacity()

    def _dispatch_webhook(self, job_id, job, event_type):
        """Enqueues (never blocks on) a webhook delivery for this
        job's terminal state -- see gcon.transport.webhooks. A no-op
        without a control_plane, same guard as _persist_job_status.
        `job` (self.jobs[job_id]) has no "job_id" key of its own (it's
        keyed by job_id in the dict, not self-describing), so job_id
        is passed in explicitly rather than read off `job`."""
        if self.control_plane is None:
            return
        try:
            from gcon.transport.webhooks import dispatch_job_event
            job_for_webhook = dict(job)
            job_for_webhook["job_id"] = job_id
            dispatch_job_event(self.control_plane, job_for_webhook, event_type)
        except Exception as e:
            print(f"[WARN] Failed to enqueue webhook for job '{job_id}': {e!r}")

    def _persist_job_status(self, job_id, job):
        """
        Write this job's terminal status back to the control-plane DB.

        Real, separate gap found while testing _evict_completed_if_
        over_capacity's DB fallback: control_plane.jobs.set_status()
        was previously called ONLY from restore_from_persistence's
        restart-reconciliation path -- never on a job's NORMAL
        completion. So the durable jobs table showed every job stuck
        at "pending" (whatever ensure_exists first wrote) forever,
        regardless of what actually happened, until/unless that
        coordinator process happened to restart. An evicted job's
        _load_job_from_control_plane() fallback was therefore silently
        wrong -- not merely reduced-fidelity (the known, documented
        gap on kind/requires/stages/etc.) but reporting a stale status
        a completed job never actually had. Called from both of
        _run_job's terminal exits (success/failure and the earlier
        dispatch-exception branch).
        """
        if self.control_plane is None:
            return
        try:
            self.control_plane.jobs.set_status(
                job_id, job["status"], result=job.get("result"), completed=True,
            )
        except Exception as e:
            print(f"[WARN] Failed to persist status for job '{job_id}': {e!r}")

    def _evict_completed_if_over_capacity(self):
        """
        Bound self.jobs/self.receipts' in-memory size. Called after
        every job reaches a terminal state (_run_job's every exit
        path). Only ever evicts jobs already in a terminal state
        (completed/failed/cancelled) -- a pending/running job is never
        touched no matter how old, since nothing else re-discovers an
        in-flight job once it's gone from here. Receipts are always
        eligible: a receipt only exists once its job is already done.
        Evicts oldest-first (dict insertion order) in one trim per
        call down to the configured cap, rather than one-in-one-out.

        Fidelity note, read before assuming an evicted entry is fully
        gone: receipts are safe to evict with no data loss -- the
        identical payload is already durably persisted (_run_job's
        control_plane.receipts.upload call, from the earlier
        signature-verification fix) and get_receipt_detail() falls
        back to the DB for a receipt_id it can't find in memory. Jobs
        are a narrower story: the durable jobs table only ever
        captures command/status/completed_at/result/created_by/
        workflow_id/org_id (see restore_from_persistence, which has
        had exactly this gap since before this fix, on every
        coordinator restart) -- NOT kind/requires/stages/
        dataset_artifacts/policy_report. So an evicted job's
        get_execution_detail() falls back to that same narrower shape
        restore_from_persistence already produces today -- this fix
        does not make that fidelity any worse, it just makes the same
        existing gap reachable via a cap instead of only via a
        restart. Fully preserving those fields across eviction would
        need the jobs table schema extended first (new columns, a
        migration) -- deliberately out of scope for what was asked as
        a memory-growth fix, not something to bundle in silently.
        """
        with self.jobs_lock:
            overflow = len(self.jobs) - self._max_jobs_in_memory
            if overflow > 0:
                terminal = [
                    jid for jid, j in self.jobs.items()
                    if j.get("status") in ("completed", "failed", "cancelled")
                ]
                for jid in terminal[:overflow]:
                    del self.jobs[jid]

        with self.receipts_lock:
            overflow = len(self.receipts) - self._max_receipts_in_memory
            if overflow > 0:
                for jid in list(self.receipts.keys())[:overflow]:
                    del self.receipts[jid]

    def _load_job_from_control_plane(self, job_id):
        """
        Cold-path fallback for a job evicted from self.jobs (see
        _evict_completed_if_over_capacity) or from before this
        coordinator's current process started. Returns the same
        reduced shape restore_from_persistence() already reconstructs
        on every restart -- command/status/completed_at/result/
        created_by/workflow_id/org_id only, NOT kind/requires/stages/
        dataset_artifacts/policy_report/artifacts (empty list). This
        is an existing, pre-dating-this-fix fidelity gap, not a new
        one -- see _evict_completed_if_over_capacity's docstring.
        Returns None if there's no control_plane, or no row for this
        job_id.
        """
        if self.control_plane is None:
            return None
        try:
            row = self.control_plane.jobs.get(job_id)
        except Exception:
            return None
        if row is None:
            return None
        return {
            "command": row["command"],
            "node_id": None,
            "status": row["status"],
            "artifacts": [],
            "created_at": row["submitted_at"],
            "completed_at": row.get("completed_at"),
            "result": row.get("result"),
            "created_by": row.get("created_by"),
            "workflow_id": row.get("workflow_id"),
            "org_id": row.get("org_id"),
        }

    def _load_receipt_from_control_plane(self, job_id=None, receipt_id=None):
        """
        Cold-path fallback for a receipt evicted from self.receipts
        (see _evict_completed_if_over_capacity), or from before this
        coordinator's current process started. Unlike jobs, this is
        the FULL, byte-identical receipt payload -- no fidelity loss
        -- since receipts are already completely, durably persisted
        (see _run_job's control_plane.receipts.upload call). Look up
        by exactly one of job_id (fast: list_for_job is indexed) or
        receipt_id (a full-table scan through each row's parsed
        payload -- there is no receipt_id index in the DB -- so this
        is deliberately only reached as a fallback for a cache miss,
        never on the hot path). Returns None if there's no
        control_plane, or nothing matches.
        """
        if self.control_plane is None:
            return None
        try:
            if job_id is not None:
                rows = self.control_plane.receipts.list_for_job(job_id)
                return rows[-1]["payload"] if rows else None
            for row in self.control_plane.receipts.list_all():
                if row["payload"].get("receipt_id") == receipt_id:
                    return row["payload"]
        except Exception:
            return None
        return None

    def scheduler_loop(self):
        """
        Continuously assign waiting jobs to idle nodes.

        This loop runs on a single daemon thread with no supervisor to
        restart it. RuntimeError is assign_job()'s expected "no
        available node right now" signal and is handled by simply
        requeuing the job and retrying later -- that is a normal,
        recoverable condition and must never kill this thread.

        Anything else (a bug in select_node/assign_job, corrupted
        internal state, etc.) is genuinely unexpected. It is
        deliberately NOT swallowed: health_service.check_coordinator()
        determines cluster health by checking
        `scheduler_thread.is_alive()`, so an unexpected exception here
        is allowed to propagate and kill the thread, making the
        failure observable to health monitoring instead of silently
        degrading job dispatch forever. The job is put back on the
        queue first so it isn't lost.
        """

        while not self._shutdown_event.is_set():

            if self.scheduler_paused:
                self._shutdown_event.wait(0.2)
                continue

            # HA gating: a standby coordinator (leader_elector set but
            # not currently leader) must never dispatch -- two
            # coordinators both assigning jobs off the same queue
            # against the same node pool is exactly the double-
            # dispatch race the atomic claim_best_idle_node() fix
            # closed for a single coordinator; it does nothing to
            # protect against a second *process* doing it. See
            # gcon.cluster.leader_election's module docstring. A
            # coordinator with no leader_elector (the default,
            # single-coordinator case) is unaffected -- always "leader".
            if self.leader_elector is not None and not self.leader_elector.is_leader:
                self._shutdown_event.wait(0.5)
                continue

            if self.job_queue.empty():
                self._shutdown_event.wait(0.1)
                continue
            
            if not self.scheduler.has_available_node():
                self._shutdown_event.wait(0.1)
                continue

            job_id = self.job_queue.get()
            print(f"[QUEUE] Dispatching {job_id}")
            print(f"[QUEUE] Remaining jobs: {self.job_queue.qsize()}")

            try:
                self.assign_job(job_id)
            except RuntimeError:
                # Expected, recoverable: "no available node right now".
                # Put the job back and try again on the next tick.
                self.job_queue.put(job_id)
            except Exception:
                # Genuinely unexpected (a bug in select_node/assign_job,
                # corrupted internal state, etc.). Do NOT swallow this:
                # health_service.check_coordinator() detects cluster
                # trouble via scheduler_thread.is_alive(), and a loop
                # that absorbs every exception can never be observed as
                # unhealthy even when something is seriously wrong. Put
                # the job back so it isn't lost, then let the exception
                # propagate and kill this thread so monitoring catches it.
                self.job_queue.put(job_id)
                raise

            # Deliberately no sleep here: as long as there's queued
            # work and an idle node to take it, loop straight back to
            # the top and dispatch again immediately. A fixed sleep
            # after every single dispatch previously capped throughput
            # at 1 job / tick (~20/s) regardless of how many nodes
            # were idle or how fast jobs actually ran, which meant a
            # burst of hundreds/thousands of queued jobs took far
            # longer to drain than the work itself required. The
            # 0.1s/0.2s waits above already throttle the loop whenever
            # there's genuinely nothing to do (empty queue, no idle
            # node, paused), so this can't spin hot on an empty queue.
            
    def health_check_loop(self):
        """
        Periodically check for nodes that have gone silent (missed
        their heartbeat window) and recover any jobs they were
        running. Runs continuously in the background so heartbeat
        loss is detected in real time, not just when a button is
        clicked.
        """
        while not self._shutdown_event.is_set():
            if self._shutdown_event.wait(3):
                break
            try:
                self.check_cluster_health()
            except Exception as e:
                print(f"[HEALTH] Health check loop error: {e}")

    def autoscale_loop(self):
        """
        Periodically compare pending job count against idle node
        count (see AutoScaler.check_scale) and scale the cluster up
        or down accordingly. Only started when GCON_AUTOSCALE_ENABLED
        is set -- see __init__.

        AutoScaler.scale_up() deliberately raises RuntimeError under
        any transport other than LocalTransport (see its own
        docstring): a coordinator process cannot fabricate a live
        network connection to a node that doesn't exist yet, and this
        codebase does not implement a real cloud/container
        provisioner. That's a permanent, structural fact about the
        current transport for the coordinator's whole lifetime, not a
        transient failure -- so rather than calling check_scale()
        every tick and catching (and re-logging) the same
        "unsupported" error forever, that capability is checked once,
        up front. If it can't actually scale anything, this logs once
        and returns without ever entering the polling loop: the
        manual scale_up/scale_down admin endpoints still work exactly
        as before (they raise the same clear, attributable error
        rather than silently no-op'ing), this just means the
        *automatic* path has nothing useful to do under this
        transport.
        """
        if not isinstance(self.communication.transport, LocalTransport):
            print(
                "[AUTOSCALER] GCON_AUTOSCALE_ENABLED is set, but this "
                "coordinator's transport does not support automatic node "
                "provisioning (see AutoScaler.scale_up's docstring) -- the "
                "automatic scaling loop will not run. Manual scale_up/"
                "scale_down via the dashboard/admin API are unaffected."
            )
            return

        interval = float(os.environ.get("GCON_AUTOSCALE_INTERVAL_SECONDS", "5.0"))
        while not self._shutdown_event.is_set():
            if self._shutdown_event.wait(interval):
                break
            try:
                self.autoscaler.check_scale()
            except Exception as e:
                # Mirrors health_check_loop's forgiving pattern, not
                # scheduler_loop's deliberate re-raise: an autoscaling
                # hiccup isn't a cluster-health signal the way a
                # scheduler_thread death is, so it shouldn't be able to
                # kill this thread over a transient issue.
                print(f"[AUTOSCALER] Autoscale loop error: {e}")

    def queue_job(self, job_id):
        """Add a job to the pending queue."""
        self.job_queue.put(job_id)   
        
    
    def deregister_agent(self, node_id):
        """
        Remove an agent from the running cluster.
        """

        node = self.registry.get_node(node_id)

        self.event_bus.publish(
            Event(
                timestamp=datetime.now(UTC),
                event_type=EventType.NODE_DEREGISTERED,
                source="Coordinator",
                payload={
                    "node_id":node.node_id,
                    "status": node.status
        },
    )
)
        self.registry.remove(node_id)

        print(f"Node '{node_id}' deregistered successfully.")

    # ------------------------------------------------------------
    # Scheduler control
    # ------------------------------------------------------------

    def pause_scheduler(self):
        """
        Stop assigning new jobs to nodes. Already-running jobs are
        unaffected and continue to completion.
        """
        self.scheduler_paused = True
        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="SCHEDULER_PAUSED",
            source="Coordinator", payload={},
        ))
        print("[SCHEDULER] Paused.")

    def resume_scheduler(self):
        """
        Resume assigning queued jobs to idle nodes.
        """
        self.scheduler_paused = False
        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="SCHEDULER_RESUMED",
            source="Coordinator", payload={},
        ))
        print("[SCHEDULER] Resumed.")

    def shutdown(self, timeout=5.0):
        """
        Stop this coordinator's background daemon threads
        (scheduler_loop, health_check_loop, autoscale_loop if
        enabled) cleanly.

        Without this there was no way to stop them short of process
        exit, so every constructed coordinator (e.g. one per test)
        kept running forever, accumulating threads and competing for
        the GIL/CPU with everything created afterward. Safe to call
        more than once. Does not touch already-running jobs; their
        worker threads (_run_job) are independent and short-lived, so
        they finish on their own.
        """
        self._shutdown_event.set()
        self.scheduler_thread.join(timeout=timeout)
        self.health_check_thread.join(timeout=timeout)
        if self.autoscale_thread is not None:
            self.autoscale_thread.join(timeout=timeout)
        if self.webhook_dispatcher is not None:
            self.webhook_dispatcher.stop()
        if getattr(self, "leader_elector", None) is not None:
            self.leader_elector.stop()
        print("[COORDINATOR] Shutdown complete.")

    # ------------------------------------------------------------
    # Node lifecycle control
    # ------------------------------------------------------------

    def drain_node(self, node_id):
        """
        Stop assigning new jobs to a node. Any job it's currently
        running is left to finish naturally.
        """
        self.registry.get_node(node_id)  # raises if missing
        self.registry.set_draining(node_id, True)
        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="NODE_DRAINING",
            source="Coordinator", payload={"node_id": node_id},
        ))
        print(f"[NODE] '{node_id}' is draining — no new jobs will be assigned.")

    def restart_worker(self, node_id):
        """
        Restart a worker in place: cancel any job it's currently
        running, then reset it to idle. The node keeps its identity
        and stays registered (unlike stop_worker, which removes it).
        """
        node = self.registry.get_node(node_id)
        info = self.registry.get_node_info(node_id)

        was_running = info["status"] == "busy"
        if was_running:
            self._cancel_node_job(node_id)

        self.registry.set_draining(node_id, False)
        node.status = "idle"
        self.registry.heartbeat(node_id, "idle", node.heartbeat()["timestamp"])

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="NODE_RESTARTED",
            source="Coordinator", payload={"node_id": node_id, "had_running_job": was_running},
        ))
        print(f"[NODE] '{node_id}' restarted.")

    def stop_worker(self, node_id):
        """
        Forcibly stop and remove a worker: cancel any job it's
        currently running, then deregister it from the cluster.
        """
        info = self.registry.get_node_info(node_id)
        if info["status"] == "busy":
            self._cancel_node_job(node_id)

        self.deregister_agent(node_id)
        print(f"[NODE] '{node_id}' stopped and removed.")

    def _cancel_node_job(self, node_id):
        """
        Find whatever job is currently running on a node and cancel
        it (kills the underlying subprocess).
        """
        with self.jobs_lock:
        
            for job_id, job in self.jobs.items():
                if job["node_id"] == node_id and job["status"] == "running":
                    job["cancel_requested"] = True
                    node = self.registry.get_node(node_id)
                    node.cancel()
                    return job_id
        return None

    # ------------------------------------------------------------
    # Job control
    # ------------------------------------------------------------

    def cancel_job(self, job_id):
        """
        Cancel a specific running job by killing its process.
        """
        if job_id not in self.jobs:
            raise ValueError(f"Job '{job_id}' does not exist.")

        job = self.jobs[job_id]
        if job["status"] != "running":
            raise ValueError(f"Job '{job_id}' is not running (status: {job['status']}).")

        job["cancel_requested"] = True
        node = self.registry.get_node(job["node_id"])
        killed = node.cancel()

        print(f"[JOB] Cancel requested for '{job_id}' (process killed: {killed}).")
        return killed

    def clear_queue(self):
        """
        Remove every job still waiting in the queue and mark them
        cancelled. Jobs already running are unaffected.
        """
        cleared = []
        while not self.job_queue.empty():
            job_id = self.job_queue.get()
            job = self.jobs.get(job_id)
            if job and job["status"] == "pending":
                job["status"] = "cancelled"
                job["completed_at"] = datetime.now(UTC).isoformat()
                cleared.append(job_id)

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="QUEUE_CLEARED",
            source="Coordinator", payload={"cleared_job_ids": cleared},
        ))
        print(f"[QUEUE] Cleared {len(cleared)} pending job(s).")
        return cleared

    def clear_failed_jobs(self):
        """
        Permanently drop every currently failed job (as opposed to
        retry_failed_jobs, which re-queues them for another attempt).
        Jobs in any other status are unaffected.

        This now deletes from BOTH the in-memory job set and (when a
        control_plane is attached) the durable jobs table. It used to
        only do the former -- which meant it could only ever clear
        whatever happened to still be resident in the bounded
        in-memory set at that moment, silently leaving anything
        beyond that bound (or already evicted from memory) untouched
        in the DB, where restore_from_persistence() would reload it
        right back on the next coordinator restart. "Clear Failed"
        deleting 200 of 900 failed jobs and having the rest reappear
        after a restart was that bug, not a capacity limit working as
        intended.
        """
        cleared = []

        with self.jobs_lock:
            for job_id, job in self.jobs.items():
                if job["status"] == "failed":
                    cleared.append(job_id)
            for job_id in cleared:
                del self.jobs[job_id]

        deleted_from_db = 0
        if self.control_plane is not None:
            try:
                deleted_from_db = self.control_plane.jobs.delete_by_status("failed")
            except Exception as e:
                print(f"[WARN] Failed to durably delete failed jobs from control plane: {e!r}")

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="FAILED_JOBS_CLEARED",
            source="Coordinator",
            payload={"cleared_job_ids": cleared, "deleted_from_db": deleted_from_db},
        ))
        print(
            f"[QUEUE] Cleared {len(cleared)} failed job(s) from memory, "
            f"{deleted_from_db} from the durable store."
        )
        return cleared

    def retry_failed_jobs(self):
        """
        Re-queue every currently failed job for another attempt.
        """
        retried = []

        with self.jobs_lock:
            for job_id, job in self.jobs.items():
                if job["status"] == "failed":
                    job["status"] = "pending"
                    job["node_id"] = None
                    job["completed_at"] = None
                    job.pop("cancel_requested", None)
                    self.queue_job(job_id)
                    retried.append(job_id)

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="FAILED_JOBS_RETRIED",
            source="Coordinator", payload={"job_ids": retried},
        ))
        print(f"[QUEUE] Retrying {len(retried)} failed job(s).")
        return retried

    def retry_job(self, job_id):
        """
        Re-queue a single job for another attempt. Unlike
        retry_failed_jobs (which only ever targets "failed" jobs),
        this also accepts a "pending" job that never got picked up by
        a worker -- the same node-unassigned symptom, just without an
        error attached -- since a stuck-pending job needs the exact
        same reset (clear any stale node/attempt state, push it back
        onto the queue) to get another real dispatch attempt rather
        than sitting wherever it already was in the queue ordering.
        """
        with self.jobs_lock:
            job = self.jobs.get(job_id)
            if job is None:
                raise ValueError(f"Job '{job_id}' does not exist.")
            if job["status"] not in ("failed", "pending"):
                raise ValueError(
                    f"Job '{job_id}' has status '{job['status']}'; only "
                    "'failed' or 'pending' jobs can be retried."
                )
            job["status"] = "pending"
            job["node_id"] = None
            job["completed_at"] = None
            job.pop("cancel_requested", None)
            self.queue_job(job_id)

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="JOB_RETRIED",
            source="Coordinator", payload={"job_id": job_id},
        ))
        print(f"[QUEUE] Retrying job '{job_id}'.")
        return job_id

    def clear_completed_jobs(self):
        """
        Remove completed jobs from the working set to declutter the
        dashboard. Running/pending/failed jobs are left alone.
        """
        with self.jobs_lock:
            cleared = [jid for jid, j in self.jobs.items() if j["status"] == "completed"]
            for job_id in cleared:
                del self.jobs[job_id]

        print(f"[JOBS] Cleared {len(cleared)} completed job(s).")
        return cleared
    
     
    def rediscover_nodes(self):
        """
        Re-check every node's heartbeat freshness right now (rather
        than waiting for the next periodic health check), marking
        any that have gone silent as offline and recovering their
        in-flight jobs.
        """
        offline_nodes = self.registry.check_node_health()
        for node_id in offline_nodes:
            print(f"Node '{node_id}' marked OFFLINE")
            self.recover_jobs(node_id)

        print(f"[DISCOVERY] Rediscovery complete. {len(offline_nodes)} node(s) newly offline.")
        return {
            "checked": len(self.registry.nodes),
            "newly_offline": offline_nodes,
        }

    # ------------------------------------------------------------
    # Receipts, snapshots, emergency control
    # ------------------------------------------------------------

    def verify_all_receipts(self):
        """
        Cryptographically verify every stored receipt's signed proof
        against the coordinator's verifier, using the real HMAC
        signature check (not a stub).
        """
        results = []
        with self.receipts_lock:
            receipts_snapshot = list(self.receipts.items())
        for receipt_id, receipt in receipts_snapshot:
            proof = receipt.get("proof", {})
            is_valid, message = self.verifier.validate_proof(proof)
            results.append({
                "receipt_id": receipt_id,
                "job_id": receipt.get("job_id"),
                "valid": is_valid,
                "message": message,
            })

        print(f"[VERIFY] Checked {len(results)} receipt(s).")
        return results

    def get_cluster_snapshot(self):
        """
        Return a full point-in-time dump of cluster state, for the
        "Snapshot Cluster" export.
        """
        return {
            "taken_at": datetime.now(UTC).isoformat(),
            "cluster_state": self.get_cluster_state(),
            "nodes": self.get_nodes(),
            "jobs": self.get_jobs(),
            "receipts": self.get_receipts(),
            "artifacts": self.get_artifacts(),
            "events": [
                {
                    "timestamp": e.timestamp.isoformat(),
                    "event_type": e.event_type,
                    "source": e.source,
                    "payload": e.payload,
                }
                for e in self.get_all_events()
            ],
        }

    def export_logs(self):
        """
        Collect stdout/stderr for every job that has actually run,
        for the "Export Logs" download.
        """
        lines = []
         
        with self.jobs_lock:
            jobs_snapshot = list(self.jobs.items())
        for job_id, job in jobs_snapshot:
            result = job.get("result")
            if not result:
                continue
            lines.append(f"===== {job_id} ({job['status']}) =====")
            lines.append(f"command: {job.get('command')}")
            lines.append(f"node: {job.get('node_id')}")
            if "stdout" in result:
                lines.append("--- stdout ---")
                lines.append(result.get("stdout") or "(empty)")
            if "stderr" in result:
                lines.append("--- stderr ---")
                lines.append(result.get("stderr") or "(empty)")
            if "message" in result:
                lines.append(f"error: {result['message']}")
            lines.append("")

        return "\n".join(lines) if lines else "No job output recorded yet."

    def emergency_stop(self):
        """
        Pause the scheduler and cancel every currently running job.
        Registered nodes are left in place (this is a stop, not a
        teardown) but no new work will be assigned until resumed.
        """
        self.pause_scheduler()

        cancelled = []
        with self.jobs_lock:
            jobs_snapshot = list(self.jobs.items())
        for job_id, job in jobs_snapshot:
            if job["status"] == "running":
                try:
                    self.cancel_job(job_id)
                    cancelled.append(job_id)
                except ValueError:
                    pass

        self.event_bus.publish(Event(
            timestamp=datetime.now(UTC), event_type="EMERGENCY_STOP",
            source="Coordinator", payload={"cancelled_job_ids": cancelled},
        ))
        print(f"[EMERGENCY] Stopped. Cancelled {len(cancelled)} running job(s).")
        return cancelled
        
    def get_pending_job_count(self):
        """
        Return the number of jobs waiting in the queue.
        """
        return self.job_queue.qsize()


    def get_idle_node_count(self):
        """
        Return the number of idle nodes currently available.
        """
        return len(self.registry.available_nodes())


    def get_total_node_count(self):
        """
        Return the total number of registered nodes.
        """
        return len(self.registry.list_nodes())


    def get_registered_nodes(self):
        """
        Return a list of registered node IDs.
        """
        return self.registry.list_nodes()
    
    def get_idle_nodes(self):
        """
        Return all currently idle node objects.
        """
        idle_nodes = []

        for info in self.registry.snapshot().values():
            if info["status"] == "idle":
                idle_nodes.append(info["node"])

        return idle_nodes
    
    def register_job_artifact(self, job_id, node_id, filepath):
        """
        Store and register an artifact produced by a completed job.

        Returns:
            artifact_id
        """

        stored_path = self.storage_manager.store_artifact(
            node_id,
            filepath
    )

        artifact_id = self.artifact_registry.register_artifact(
             stored_path
    )

        job = self.jobs.get(job_id)

        if job is not None:
            job.setdefault("artifacts", []).append(artifact_id)
            
        self.event_bus.publish(
            Event(
                timestamp=datetime.now(UTC),
                event_type="ARTIFACT_REGISTERED",
                source="StorageManager",
                payload={
                    "artifact_id": artifact_id
        },
    )
)

        return artifact_id
    
    def get_cluster_state(self):
        """
        Return a snapshot of the current cluster state, in the flat
        shape expected by the dashboard.
        """
        with self.jobs_lock:
            jobs_snapshot = list(self.jobs.values())
 
        return {
            "total_nodes": self.get_total_node_count(),
            "idle_nodes": self.get_idle_node_count(),
            "registered_node_count": len(self.get_registered_nodes()),
            "registered_nodes": self.get_registered_nodes(),
            "running_jobs": sum(
                1 for job in jobs_snapshot
                if job["status"] == "running"
            ),
            "completed_jobs": sum(
                1 for job in jobs_snapshot
                if job["status"] == "completed"
            ),
            "failed_jobs": sum(
                1 for job in jobs_snapshot
                if job["status"] == "failed"
            ),
        }

    def get_events(self,limit=20):
        """
        Return recent cluster events.
        """
        return self.event_bus.get_recent_events(limit)
    
    def get_all_events(self):
        """
        Return the full event history (used by analytics/diagnostics).
        """
        return self.event_bus.get_events()

    def submit_workflow(self, workflow):
        """
        Submit a workflow DAG for execution via the workflow engine.
        """
        return self.workflow_engine.submit_workflow(workflow)


    def get_workflows(self):
        """
        Return a summary of every workflow the engine knows about.
        Empty until a workflow has actually been submitted.
        """
        return [
            state.summary()
            for state in self.workflow_engine.states.values()
    ]


    def get_nodes(self, org_id=None):
        """
        Return a list of dicts describing every registered node, for
        use by presentation/dashboard clients. Uses getattr/get with
        defaults so a missing field never crashes the endpoint.

        `org_id` optionally filters down to nodes belonging to a
        single company -- used by the dashboard's Companies panel and
        by an org-scoped API key's view of its own fleet.
        """
        nodes = []
        
        for node_id, info in self.registry.snapshot().items():
            if org_id is not None and info.get("org_id") != org_id:
                continue
            nodes.append({
                "node_id": node_id,
                "status": info.get("status", "unknown"),
                "address": info.get("address"),
                "cpu": info.get("cpu", "N/A"),
                "memory": info.get("memory", "N/A"),
                "running_jobs": info.get("running_jobs", 0),
                "last_seen": (
                    info["last_seen"].isoformat()
                    if isinstance(info.get("last_seen"), datetime)
                    else info.get("last_seen", "N/A")
                ),
                "draining": info.get("draining", False),
                "org_id": info.get("org_id"),
            })

        return nodes

    def get_jobs_page(self, status=None, search=None, org_id=None, limit=50, offset=0):
        """
        Real server-side pagination for the Executions tab -- distinct
        from get_jobs() above, which only ever sees this coordinator's
        bounded in-memory job set (see restore_from_persistence's
        docstring for why that's bounded: unlimited history there
        would defeat the whole point of that bound). This queries the
        durable control-plane DB directly when one is attached, so
        "page 40 of failed jobs from three days ago" works correctly
        even though that job is long gone from memory.

        Returns (items, total_count). Falls back to paginating over
        the in-memory get_jobs() result only when there's no
        control_plane at all (LocalTransport / most tests) -- still
        correctly bounded to a single page's worth of dict work
        either way, just scoped to whatever this process currently
        holds instead of full history.
        """
        if self.control_plane is not None:
            rows, total = self.control_plane.jobs.search_paginated(
                status=status, search=search, org_id=org_id, limit=limit, offset=offset,
            )
            items = []
            for row in rows:
                # Enrich with what's only available in-memory (receipt
                # id, live node assignment) when this job is still
                # resident there; a job old enough to have been
                # evicted from memory just gets these as None -- the
                # full detail drawer (get_execution_detail) still has
                # the DB-fallback path for that case.
                live = self.jobs.get(row["job_id"])
                receipt = self.receipts.get(row["job_id"]) if live else None
                result = (live or {}).get("result") or {}
                items.append({
                    "job_id": row["job_id"],
                    "status": row["status"],
                    "node_id": (live or {}).get("node_id"),
                    "created_at": row["submitted_at"],
                    "completed_at": row.get("completed_at"),
                    "receipt_id": receipt.get("receipt_id", row["job_id"]) if receipt else None,
                    "created_by": row.get("created_by"),
                    "workflow_id": row.get("workflow_id"),
                    "org_id": row.get("org_id"),
                    "runtime_seconds": result.get("runtime_seconds"),
                })
            return items, total

        # No control_plane at all: fall back to slicing the in-memory
        # get_jobs() result.
        all_jobs = self.get_jobs(org_id=org_id, status=status)
        if search:
            needle = search.lower()
            all_jobs = [
                j for j in all_jobs
                if needle in j["job_id"].lower()
            ]
        total = len(all_jobs)
        return all_jobs[offset:offset + limit], total

    def get_job_status_counts(self):
        """Cheap grouped counts for the Executions tab's summary
        tiles -- see JobRepository.count_by_status's docstring for why
        this replaces computing them by filtering a fetched job list."""
        if self.control_plane is not None:
            counts = self.control_plane.jobs.count_by_status()
        else:
            counts = {}
            with self.jobs_lock:
                for job in self.jobs.values():
                    counts[job["status"]] = counts.get(job["status"], 0) + 1
        return {
            "total": sum(counts.values()),
            "queued": counts.get("pending", 0),
            "running": counts.get("running", 0),
            "completed": counts.get("completed", 0),
            "failed": counts.get("failed", 0),
            "cancelled": counts.get("cancelled", 0),
        }

    def get_jobs(self, created_by=None, org_id=None, status=None, limit=None):
        """
        Return a dashboard summary about all jobs, newest first.

        `created_by` optionally filters the result down to jobs
        submitted by a single user_id -- used by
        ManagementLayer.get_user_stats() to compute real, live
        per-user usage metrics instead of a permanently-zero counter.

        `org_id` optionally filters down to jobs submitted for a
        single company -- used by the dashboard's Companies panel and
        by an org-scoped API key's view of its own jobs.

        `status` optionally filters to a single job status (e.g.
        "failed", "pending"). `limit` caps how many jobs are
        returned. Both exist because the dashboard's jobs panel was
        previously handed every job ever submitted, unfiltered and
        unpaginated -- fine at low volume, unusable once a cluster
        has run a few hundred jobs and someone just wants to see
        what's currently failed or pending.
        """
        jobs = []
        has_filter = created_by is not None or org_id is not None or status is not None

        # Newest first: self.jobs is insertion-ordered (oldest first),
        # which buried exactly the jobs someone checking the dashboard
        # cares most about -- the ones that just ran -- at the bottom.
        if limit is not None and not has_filter:
            # Fast path: with no filter, the answer is exactly "the
            # last `limit` jobs in insertion order" -- no need to copy
            # every job in history into a new list first just to read
            # the most recent handful off the end of it. islice over
            # reversed(dict.items()) walks the dict directly and stops
            # after `limit`, so this is O(limit), not O(total jobs).
            # Still done under jobs_lock (not released early) because
            # taking only part of a live iterator over self.jobs while
            # another thread concurrently inserts (submit_job) would
            # raise "dictionary changed size during iteration" -- same
            # hazard the full-copy path below guards against, just
            # bounded to a slice instead of a whole-dict copy.
            with self.jobs_lock:
                jobs_snapshot = list(
                    itertools.islice(reversed(self.jobs.items()), limit)
                )
        else:
            # Filtered and/or unlimited: still need to search the full
            # history to guarantee every matching job is found, so
            # there's no way to bound this to less than O(total jobs)
            # without an index on status/created_by/org_id -- that's a
            # real design decision (what to index, memory cost of
            # maintaining it), not something to invent silently here.
            with self.jobs_lock:
                jobs_snapshot = list(self.jobs.items())
            jobs_snapshot = list(reversed(jobs_snapshot))

        for job_id, job in jobs_snapshot:
            if created_by is not None and job.get("created_by") != created_by:
                continue
            if org_id is not None and job.get("org_id") != org_id:
                continue
            if status is not None and job["status"] != status:
                continue
            receipt = self.receipts.get(job_id)
            result = job.get("result") or {}
            metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
            # Raw stdout: hashed into the receipt's output_hash (a
            # verification proof, not a readable result) but never
            # otherwise surfaced -- fine for jobs where only the side
            # effect mattered, useless for a job whose entire point is
            # a computed answer (e.g. an ML prediction). Capped at 8KB
            # so one runaway job can't bloat every /jobs response;
            # full output is still in job["result"] in-process.
            stdout = result.get("stdout")
            output = None
            if stdout is not None:
                output = stdout if len(stdout) <= 8192 else stdout[:8192] + "... (truncated)"
            jobs.append({
                "job_id": job_id,
                "status": job["status"],
                "node_id": job.get("node_id"),
                "created_at": job.get("created_at"),
                "completed_at": job.get("completed_at"),
                "receipt_id": receipt.get("receipt_id", job_id) if receipt else None,
                "artifacts": len(job.get("artifacts", [])),
                "created_by": job.get("created_by"),
                "workflow_id": job.get("workflow_id"),
                "org_id": job.get("org_id"),
                # Automatically measured wall-clock runtime for this
                # job (GCONAgent.execute_job), None until it finishes.
                # This is the real, always-available "compute usage"
                # signal -- distinct from the opt-in "usage" below,
                # which only exists if the job's own command chose to
                # report something (e.g. LLM token counts).
                "runtime_seconds": result.get("runtime_seconds"),
                # Opt-in usage report a job's own command wrote to
                # GCON_USAGE_REPORT_PATH (see GCONAgent.execute_job) --
                # None if the job hasn't finished yet, ran on a node
                # too old to capture it, or (most commonly) the job's
                # command didn't write one. Never fabricated.
                "usage": metrics.get("usage"),
                "output": output,
            })
            if limit is not None and len(jobs) >= limit:
                break
        return jobs

    def get_storage(self):
        """
        Return storage information.
        """
        return {
            "artifacts": self.artifact_registry.artifacts
        }

    def get_metrics(self):
        """
        Return cluster metrics.
        """
        collector = MetricsCollector(self)

        return {
            "nodes": collector.collect_node_metrics(),
            "jobs": collector.collect_job_metrics(),
        }
        
    def _commit_receipt_verification(self, job_id, receipt, is_valid):
        """
        Record a freshly-computed validate_proof() result for job_id
        into _receipt_verified_cache and keep the running
        _verified_receipt_count/_unverified_receipt_count totals in
        lockstep with it. Shared by get_receipts() and
        _drain_pending_receipt_verifications(), since both can end up
        being the first to verify a given job_id.

        `receipt` is the exact receipt object this is_valid result was
        computed for. The identity check against self.receipts[job_id]
        happens under the same lock acquisition as the cache write --
        not as a separate check beforehand -- so a receive_receipt()
        overwrite landing in the gap between "we finished verifying"
        and "we're about to cache the result" can't still get its
        stale result committed under the new receipt's job_id: this
        becomes a no-op instead, and the re-queued job_id (see
        receive_receipt) gets verified fresh on the next call.

        Returns True if this call actually committed the result,
        False if it was skipped -- either because another concurrent
        caller already had (a receipt's verification result is
        deterministic given its immutable proof, so it's safe to just
        skip rather than recompute or double-count), or because the
        receipt was superseded before we could commit.
        """
        with self.receipts_lock:
            if job_id in self._receipt_verified_cache:
                return False
            if self.receipts.get(job_id) is not receipt:
                return False
            self._receipt_verified_cache[job_id] = is_valid
            if is_valid:
                self._verified_receipt_count += 1
            else:
                self._unverified_receipt_count += 1

        # Persist outside the receipts_lock -- a DB write acquires
        # ControlPlaneDatabase's own lock, and there's no need to hold
        # both for its duration. Best-effort: a failure here doesn't
        # invalidate the in-memory result just committed above (which
        # is what every existing verified/unverified check in this
        # process actually reads), it only means the DB's `verified`
        # column -- used by search_paginated's WHERE verified=? filter
        # and count_by_verified's summary tiles -- stays stale for
        # this one receipt until the next successful write.
        if self.control_plane is not None:
            try:
                self.control_plane.receipts.mark_verified_by_job_id(job_id, is_valid)
            except Exception as e:
                print(f"[WARN] Failed to persist verification result for '{job_id}': {e!r}")

        return True

    def _drain_pending_receipt_verifications(self):
        """
        Verify only the receipts that have arrived (or been
        overwritten) since the last drain, instead of rescanning all
        of self.receipts to work out what isn't cached yet -- that
        scan was itself O(total receipts) even after
        _receipt_verified_cache eliminated the repeated HMAC cost, so
        health_check_loop's 3-second tick would still fall further and
        further behind as cumulative history grew. This is what that
        tick calls now, so its per-tick cost is O(receipts since the
        last tick), not O(receipts ever recorded).

        Returns the list of (job_id, receipt, is_valid) newly verified
        by this call, for health_check_loop's event-publishing.
        """
        with self.receipts_lock:
            pending = list(self._pending_receipt_ids)
            self._pending_receipt_ids.clear()
            receipts_by_id = {
                job_id: self.receipts.get(job_id) for job_id in pending
                if job_id not in self._receipt_verified_cache
            }

        newly_verified = []
        for job_id, receipt in receipts_by_id.items():
            if receipt is None:
                # Removed or reassigned again since we queued it; a
                # later receive_receipt() call already re-queued it,
                # so it'll be picked up on the next drain.
                continue
            if self.receipts.get(job_id) is not receipt:
                # Cheap early skip: already stale by the time we got
                # here, no need to spend a validate_proof() call on a
                # receipt we're not going to be able to commit anyway
                # (the authoritative check is in
                # _commit_receipt_verification below).
                continue
            is_valid, _ = self.verifier.validate_proof(receipt.get("proof", {}))
            if self._commit_receipt_verification(job_id, receipt, is_valid):
                newly_verified.append((job_id, receipt, is_valid))

        return newly_verified

    def get_receipts_page(self, verified=None, search=None, limit=50, offset=0):
        """Real server-side pagination for the Receipts tab -- see
        get_jobs_page's docstring, same reasoning. Returns (items,
        total_count)."""
        if self.control_plane is not None:
            rows, total = self.control_plane.receipts.search_paginated(
                verified=verified, search=search, limit=limit, offset=offset,
            )
            items = []
            for row in rows:
                payload = row.get("payload") or {}
                items.append({
                    "receipt_id": row["receipt_id"],
                    "job_id": row["job_id"],
                    "node_id": row.get("node_id"),
                    "created_at": row.get("uploaded_at"),
                    "verified": bool(row.get("verified")),
                    "status": (self.jobs.get(row["job_id"]) or {}).get("status", "failed"),
                })
            return items, total

        # No control_plane: fall back to slicing the in-memory
        # get_receipts() result (already bounded by
        # _max_receipts_in_memory, so still cheap even unfiltered).
        all_receipts = self.get_receipts()
        if verified is not None:
            all_receipts = [r for r in all_receipts if bool(r.get("verified")) == verified]
        if search:
            needle = search.lower()
            all_receipts = [
                r for r in all_receipts
                if needle in r.get("receipt_id", "").lower() or needle in r.get("job_id", "").lower()
            ]
        total = len(all_receipts)
        return all_receipts[offset:offset + limit], total

    def get_receipt_verification_counts(self):
        """Cheap grouped counts for the Receipts tab's (and the
        websocket bootstrap payload's) summary tiles -- see
        ReceiptRepository.count_by_verified's docstring for why this
        replaces PresentationLayer.get_receipts_summary's previous
        approach of building the entire receipt list just to sum()
        a boolean over it."""
        if self.control_plane is not None:
            return self.control_plane.receipts.count_by_verified()
        with self.receipts_lock:
            total = len(self.receipts)
            verified = self._verified_receipt_count
        return {"total": total, "verified": verified, "unverified": total - verified}

    def get_receipts(self):
        """
        Return a dashboard-friendly summary of all receipts.

        `verified` is the real signed-proof HMAC check (the same check
        `verify_all_receipts` uses), computed once per receipt and
        cached in _receipt_verified_cache rather than re-run on every
        call -- a receipt's proof cannot change after it's stored (see
        receive_receipt), so re-verifying an unchanged receipt again
        is wasted work, not extra safety. The cache is invalidated
        whenever a job_id's receipt is actually replaced, so a
        genuinely new proof is still verified fresh. This call is
        still O(total receipts) to build the snapshot/result list, but
        no longer does an HMAC check per receipt on every call --
        previously the dominant cost at scale, since this is invoked
        every 3s by health_check_loop regardless of how much of the
        receipt history is actually new.
        """
        receipts = []

        # Two lock acquisitions total for this whole call -- not one
        # per receipt. Taking the lock inside the per-item loop below
        # (an earlier version of this fix did exactly that) trades the
        # HMAC cost for ~N RLock acquire/release pairs instead, which
        # profiled as the new dominant cost at scale: a plain dict
        # snapshot copy is cheap enough to just take once up front.
        with self.receipts_lock:
            receipts_snapshot = list(self.receipts.items())
            cache_snapshot = dict(self._receipt_verified_cache)

        new_results = {}
        for job_id, receipt in receipts_snapshot:
            if job_id in cache_snapshot:
                continue
            is_valid, _ = self.verifier.validate_proof(receipt.get("proof", {}))
            new_results[job_id] = (receipt, is_valid)

        for job_id, (receipt, is_valid) in new_results.items():
            self._commit_receipt_verification(job_id, receipt, is_valid)

        for job_id, receipt in receipts_snapshot:

            is_valid = cache_snapshot.get(job_id)
            if is_valid is None:
                is_valid = new_results[job_id][1]

            receipts.append({
                # self.receipts is keyed by job_id (see receive_receipt),
                # but the receipt's own real identity is its receipt_id
                # field — a content hash — so that is what's displayed
                # and what get_receipt_detail() looks callers up by.
                "receipt_id": receipt.get("receipt_id", job_id),
                "job_id": receipt.get("job_id", job_id),
                "status": receipt.get("status", "unknown"),
                "created_at": receipt.get("issued_at", "N/A"),
                "verified": is_valid,
        })

        return receipts

    def _resolve_artifacts(self, artifact_ids):
        """
        Shared artifact-resolution used by both get_receipt_detail and
        get_execution_detail, so the two views can never disagree
        about what an execution actually produced.
        """
        artifacts = []
        for artifact_id in artifact_ids:
            artifact = self.artifact_registry.get_artifact(artifact_id)
            if artifact:
                artifacts.append({
                    "artifact_id": artifact.artifact_id,
                    "filename": artifact.filename,
                    "sha256": artifact.sha256,
                    "size": artifact.size,
                    "uploaded_at": artifact.uploaded_at,
                })
        return artifacts

    def get_receipt_detail(self, receipt_id):
        """
        Return the full record for a single receipt, for the Receipt
        Explorer: proof (hash/signature/timestamp/metrics), live
        verification, the execution it attests to, and the artifacts
        that execution produced. Returns None if not found.

        Reuses the job and artifact registries rather than storing a
        second copy of this data on the receipt itself.
        """
        with self.receipts_lock:
            receipts_snapshot = list(self.receipts.values())

        receipt = None
        for candidate in receipts_snapshot:
            if candidate.get("receipt_id") == receipt_id:
                receipt = candidate
                break

        if receipt is None:
            receipt = self._load_receipt_from_control_plane(receipt_id=receipt_id)

        if receipt is None:
            return None

        proof = receipt.get("proof", {})
        is_valid, message = self.verifier.validate_proof(proof)

        job_id = receipt.get("job_id")
        with self.jobs_lock:
            job = self.jobs.get(job_id)
        if job is None:
            job = self._load_job_from_control_plane(job_id) or {}

        artifacts = self._resolve_artifacts(job.get("artifacts", []))

        # Real, human-readable proof of what actually ran -- e.g. a
        # GPU job's own printed "device: Tesla T4" / measured gflops /
        # loss values -- was being hashed into output_hash but never
        # otherwise surfaced anywhere in the dashboard (same 8KB cap
        # as get_jobs()'s "output" field, so one runaway job's stdout
        # can't bloat this response).
        job_result = job.get("result") or {}
        stdout = job_result.get("stdout")
        output = None
        if stdout is not None:
            output = stdout if len(stdout) <= 8192 else stdout[:8192] + "... (truncated)"

        # For a verify-tagged job, every replica's own independently-
        # signed receipt lives in self.replica_receipts (see
        # _run_replicated_job/receive_replica_receipt) -- this receipt
        # is only ever one of them (the primary, or whichever one this
        # receipt_id resolves to). Surface the sibling list so the
        # Receipt Inspector can show the full witness set, not just
        # this one node's view of it.
        with self.replica_receipts_lock:
            replica_list = list(self.replica_receipts.get(job_id, []))
        replicas_summary = None
        if replica_list:
            replicas_summary = [
                {
                    "receipt_id": r.get("receipt_id"),
                    "node_id": r.get("agent_id"),
                    "output_hash": r.get("output_hash"),
                    "policy_trusted": (r.get("policy_report") or {}).get("trusted"),
                }
                for r in replica_list
            ]

        # Per-replica policy_report (attached directly to this receipt
        # by _run_replicated_job) takes precedence over the job-level
        # one, which for a verify-tagged job only ever reflects the
        # primary replica -- a non-primary receipt has its own real
        # evaluation and shouldn't silently show someone else's.
        policy_report = receipt.get("policy_report", job.get("policy_report"))

        return {
            "receipt_id": receipt.get("receipt_id"),
            "job_id": job_id,
            "agent_id": receipt.get("agent_id"),
            "status": receipt.get("status", "unknown"),
            "issued_at": receipt.get("issued_at"),
            "input_hash": receipt.get("input_hash"),
            "output_hash": receipt.get("output_hash"),
            "output": output,
            "verified": is_valid,
            "verification_message": message,
            "kind": job.get("kind", "command"),
            "stages_expected": (job.get("stages") or {}).get("expected"),
            # PolicyEngine.evaluate()'s report, or None if this receipt
            # predates policy wiring / policy evaluation itself failed
            # (see the [WARN] log -- deliberately not fabricated as
            # trusted here).
            "policy": policy_report,
            # None for an ordinary single-node job. For a verify-tagged
            # job, the comparison result across all replicas (see
            # gcon.execution.replication) -- deliberately NOT part of
            # the signed "proof" below; see replication.py's module
            # docstring for why.
            "execution_proof": receipt.get("execution_proof"),
            "replicas": replicas_summary,
            "proof": {
                "algorithm": "HMAC-SHA256",
                "gpu": proof.get("gpu"),
                "runtime_seconds": proof.get("runtime_seconds"),
                "timestamp": proof.get("timestamp"),
                "metrics": proof.get("metrics", {}),
                # Present (possibly partial -- see GCONAgent.execute_job)
                # only for "staged" jobs; already inside metrics (agent_
                # daemon.py folds it in before upload) but pulled up to
                # the top level here since it's proof content in its own
                # right, not just another metric among cpu/memory/gpu.
                "stages": proof.get("metrics", {}).get("stages", []),
                "signature": proof.get("signature"),
            },
            "execution": {
                "node_id": job.get("node_id"),
                "created_at": job.get("created_at"),
                "completed_at": job.get("completed_at"),
                "status": job.get("status"),
            },
            "artifacts": artifacts,
        }

    def get_execution_detail(self, job_id):
        """
        Return the full lifecycle record for a single execution, for
        the Executions page: the job itself, the artifacts it
        produced, and — if one exists — its receipt's live
        verification. Returns None if the job isn't found.

        A job only ever reaches "receipt generated" by actually
        having an entry in self.receipts keyed to its job_id; there
        is no separate flag to go stale.
        """
        with self.jobs_lock:
            job = self.jobs.get(job_id)

        if job is None:
            job = self._load_job_from_control_plane(job_id)

        if job is None:
            return None

        with self.receipts_lock:
            receipts_snapshot = list(self.receipts.values())

        receipt = None
        for candidate in receipts_snapshot:
            if candidate.get("job_id") == job_id:
                receipt = candidate
                break

        if receipt is None and self.control_plane is not None:
            receipt = self._load_receipt_from_control_plane(job_id=job_id)

        verified = None
        verification_message = None
        if receipt is not None:
            verified, verification_message = self.verifier.validate_proof(receipt.get("proof", {}))

        artifacts = self._resolve_artifacts(job.get("artifacts", []))

        return {
            "job_id": job_id,
            "status": job.get("status"),
            "node_id": job.get("node_id"),
            "created_at": job.get("created_at"),
            "completed_at": job.get("completed_at"),
            "artifacts": artifacts,
            "receipt_id": receipt.get("receipt_id") if receipt else None,
            "verified": verified,
            "verification_message": verification_message,
        }

    def get_node_summary(self):
        """
        Return a live breakdown of registered nodes by status, for the
        dashboard's Node Summary widget. Always derived from the
        current registry snapshot — never stored.
        """
        nodes = self.get_nodes()

        summary = {"total": len(nodes), "idle": 0, "busy": 0, "offline": 0, "draining": 0}

        for node in nodes:
            status = node.get("status", "unknown")
            if status in summary:
                summary[status] += 1
            if node.get("draining"):
                summary["draining"] += 1

        return summary
    
    def get_artifacts(self):
        """
        Return a dashboard-friendly summary of all artifacts.
        """

        artifacts = []

        for artifact in self.artifact_registry.list_artifacts():
            artifacts.append({
                "artifact_id": artifact.artifact_id,
                "filename": artifact.filename,
                "sha256": artifact.sha256,
                "size": artifact.size,
                "uploaded_at": artifact.uploaded_at,
        })

        return artifacts
    def get_cluster_status(self):
        """
        Return an overall summary of the cluster.
        """
        jobs = self.get_jobs()
        nodes = self.get_nodes()
        receipts = self.get_receipts()
        artifacts = self.get_artifacts()

        return {
            "total_nodes": len(nodes),
            "online_nodes": sum(
                1 for node in nodes
                if node["status"] in ("idle", "running", "online")
        ),
            "total_jobs": len(jobs),
            "running_jobs": sum(
                1 for job in jobs
                if job["status"] == "running"
        ),
            "completed_jobs": sum(
                1 for job in jobs
                if job["status"] == "completed"
        ),
            "total_receipts": len(receipts),
            "total_artifacts": len(artifacts),
    }
        
        
    def get_cluster_health(self):
        """
        Return overall cluster health, computed from real subsystem
        state (coordinator queue, node registry, receipts, storage
        disk, API latency) rather than a bare percentage. See
        HealthService for how each branch is derived.
        """
        health = self.health_service.compute()
        cluster = self.get_cluster_state()

        checks = health["checks"]

        return {
            # Overall cluster health
            "state": health["state"],
            "score": health["score"],
            "reason": health["reason"],
            "reasons": health["reasons"],
            "computed_at": health["computed_at"],

            # Per-branch detail, for the Health Inspector drill-down
            "checks": checks,

            # Kept for callers of the old shape (navbar badge, etc.)
            "services": {
                "coordinator": "online" if checks["coordinator"]["healthy"] else "degraded",
                "cluster": health["state"],
                "event_system": "running",
                "storage": "connected" if checks["storage"]["healthy"] else "degraded",
            },

            # The single most significant unhealthy branch right now,
            # for the Trust & Health panel's "Last detected issue" line.
            "last_issue": self.health_service.last_detected_issue(health),

            # Useful summary metrics
            "metrics": {
                "total_nodes": cluster["total_nodes"],
                "running_jobs": cluster["running_jobs"],
                "completed_jobs": cluster["completed_jobs"],
                "failed_jobs": cluster["failed_jobs"],
            },
        }

    def get_health_details(self):
        """
        Return the full health source-tree for the Health Inspector
        drill-down view (one entry per branch, each with its own
        metrics and explanation).
        """
        return self.health_service.compute()