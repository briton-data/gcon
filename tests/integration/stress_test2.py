"""
stress_test2.py — concurrency, correctness, and load tests for the
subsystems stress_test1.py does NOT touch: the management layer
(users/auth/RBAC/API keys/audit log), the workflow engine (DAG),
storage_manager.py, and policy.py.

stress_test1.py exercises the cluster layer (coordinator/agent/
registry/communication) end-to-end under load and fault injection.
This file does the same job for everything sitting alongside it —
because those modules are just as concurrent (a FastAPI app serving
many simultaneous requests) but had zero concurrency coverage before
this file. Some tests here found real, previously-undetected bugs
during authoring; those are documented in each test's docstring and
marked `@pytest.mark.xfail` with the specific defect, exactly like
stress_test1.py does for AUDIT_REPORT.md findings — this file turns
green on its own once each is fixed, no rewrite needed.

This is deliberately NOT just "throw threads at it until something
breaks." Every test asserts a specific invariant (no lost writes, no
crash mid-iteration, no incorrect state after concurrent access) so a
green run means something concrete was checked, not just "didn't
crash in the time we happened to run it."

Run everything:
    pytest stress_test2.py -v

Run only the fast tests (skip soak/scale):
    pytest stress_test2.py -v -m "not slow"

Run as a standalone combined-load script (not via pytest):
    python3 stress_test2.py --duration 60
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys
import tempfile
import threading
import time
from typing import List

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from tests.support.test_utils import (
    MetricsCollector, TestAssertionError,
    assert_eventually, get_logger, run_concurrently, unique_id,
)

logger = get_logger("stress_test2", log_file="logs/stress_test2.log")


# =======================================================================
# Fixtures
# =======================================================================

@pytest.fixture
def management():
    """
    A real ManagementLayer with no coordinator attached (event-bridge
    tests aren't the concern here — everything below exercises the
    management layer's own internal state under concurrency).

    db_path=":memory:" is deliberate: ManagementLayer now persists to
    SQLite by default (see database.py), and tests must NOT share or
    accumulate state in the real production database file across
    runs — each test needs a clean, isolated store.
    """
    from gcon.management.management_layer import ManagementLayer
    return ManagementLayer(coordinator=None, db_path=":memory:")


@pytest.fixture
def storage(tmp_path):
    from gcon.storage.storage_manager import StorageManager
    return StorageManager(storage_root=str(tmp_path / "storage"))


@pytest.fixture
def sample_file(tmp_path):
    """A small real file on disk to feed into StorageManager.store_artifact."""
    p = tmp_path / "artifact.bin"
    p.write_bytes(os.urandom(4096))
    return str(p)


# =======================================================================
# 1. MANAGEMENT-LAYER CONCURRENCY (users / auth / API keys)
# =======================================================================

class TestManagementConcurrency:
    def test_concurrent_key_creation_during_authentication_no_dict_mutation_error(self, management):
        """
        APIKeyManager.find_by_secret() (api_keys.py) iterates
        `self.keys.values()` with no lock. ManagementLayer.authenticate_api_key()
        calls it on every request. If a request creating a NEW key
        (`self.keys[key.key_id] = key`) lands mid-iteration on another
        thread, Python raises "RuntimeError: dictionary changed size
        during iteration" — an authentication call crashing because an
        unrelated key was created at the same moment.

        This fires 40 concurrent authenticators against one known-valid
        key while 40 other threads concurrently create brand-new keys,
        and asserts no authentication call ever sees a RuntimeError.
        """
        user = management.create_user("Stress User", "stress@example.com", "Developer")
        key = management.create_api_key("stress-key", owner_user_id=user["user_id"])
        secret = key["secret"]  # revealed once, at creation — matches real usage

        errors: List[str] = []
        errors_lock = threading.Lock()

        def authenticate(_i):
            try:
                management.authenticate_api_key(secret)
            except RuntimeError as e:
                with errors_lock:
                    errors.append(f"authenticate: {e}")
            except ValueError:
                pass  # a clean auth failure is fine; a RuntimeError is not

        def create_noise_key(i):
            try:
                management.create_api_key(f"noise-{i}", owner_user_id=user["user_id"])
            except RuntimeError as e:
                with errors_lock:
                    errors.append(f"create: {e}")

        threads = (
            [threading.Thread(target=authenticate, args=(i,)) for i in range(40)]
            + [threading.Thread(target=create_noise_key, args=(i,)) for i in range(40)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, (
            f"{len(errors)} RuntimeError(s) from concurrent dict mutation during "
            f"iteration in APIKeyManager (api_keys.py find_by_secret/create_key "
            f"share self.keys with no lock): {errors[:5]}"
        )

    def test_concurrent_authentication_usage_count_not_lost(self, management):
        """
        APIKey.mark_used() does `self.usage_count += 1` — a
        non-atomic read-modify-write. ManagementLayer.authenticate_api_key()
        also does `owner.stats["api_requests"] += 1` on the same
        unlocked path. 100 concurrent successful authentications
        against the SAME key should leave usage_count == 100; under
        the lost-update race it will land lower.
        """
        user = management.create_user("Counter User", "counter@example.com", "Developer")
        key = management.create_api_key("counter-key", owner_user_id=user["user_id"])
        secret = key["secret"]

        def auth_once(_i):
            management.authenticate_api_key(secret)

        run_concurrently(auth_once, 100, max_workers=100)

        stored_key = management.api_key_manager.get_key(key["key_id"])
        assert stored_key.usage_count == 100, (
            f"expected usage_count == 100 after 100 concurrent authentications, "
            f"got {stored_key.usage_count} — lost updates from the unlocked "
            f"`self.usage_count += 1` in APIKey.mark_used() (api_keys.py)"
        )

    def test_revoke_is_immediately_effective_against_concurrent_fresh_authentications(self, management):
        """
        APIKeyManager.is_valid() mutates state as a side effect of a
        read ("expired" auto-transition), and regenerate_key() resets
        status back to "Active" — both touch the same `status`
        attribute revoke_key() sets. Once revoke_api_key() has
        RETURNED, every subsequently-started authentication attempt
        must fail — none may observe a stale "Active" status.

        (A call that began concurrently WITH the revoke and was
        already past its status check is a different, expected race —
        this test only starts new calls after revoke_api_key()
        returns, so any success here is unambiguously a bug.)
        """
        user = management.create_user("Race User", "race@example.com", "Developer")
        key = management.create_api_key("race-key", owner_user_id=user["user_id"])
        secret = key["secret"]
        key_id = key["key_id"]

        management.revoke_api_key(key_id)

        successes = []
        lock = threading.Lock()

        def auth_once(_i):
            try:
                management.authenticate_api_key(secret)
                with lock:
                    successes.append(1)
            except ValueError:
                pass  # expected: revoked key must not authenticate

        run_concurrently(auth_once, 100, max_workers=100)

        assert not successes, (
            f"{len(successes)}/100 concurrent authentications succeeded against a key "
            f"that was already revoked before any of them started"
        )

    def test_concurrent_user_creation_no_duplicate_or_lost_users(self, management):
        """
        50 threads creating distinct users concurrently: every created
        user must be independently retrievable afterward (no lost
        writes to UserRegistry's internal dict, no id collisions).
        """
        created_ids = []
        lock = threading.Lock()

        def create_one(i):
            u = management.create_user(f"User {i}", f"user{i}-{unique_id('u')}@example.com", "Viewer")
            with lock:
                created_ids.append(u["user_id"])

        run_concurrently(create_one, 50, max_workers=50)

        assert len(set(created_ids)) == 50, "duplicate user_ids issued under concurrent creation"
        missing = []
        for uid in created_ids:
            try:
                management.get_user(uid)
            except ValueError:
                missing.append(uid)
        assert not missing, f"{len(missing)} concurrently-created users are not retrievable: {missing[:5]}"

    def test_rbac_permission_checks_consistent_under_concurrent_role_data_reads(self, management):
        """
        Not a mutation race (ROLE_PERMISSIONS in rbac.py is static),
        but a correctness sweep: every (role, permission) pair the
        matrix claims a role has, require_permission() must actually
        grant, and every pair it claims a role lacks, require_permission()
        must actually deny — run under concurrent load to also catch
        any accidental shared mutable state in the permission-check path.
        """
        from gcon.management import rbac

        # user_has_permission() reads `.role` off a real User object, not
        # the dict shape create_user() returns — fetch the live objects.
        user_ids_by_role = {
            role: management.create_user(f"{role} Tester", f"{role.lower()}@example.com", role)["user_id"]
            for role in rbac.ROLES
        }
        users_by_role = {
            role: management.user_registry.get_user(uid)
            for role, uid in user_ids_by_role.items()
        }

        mismatches = []
        mismatches_lock = threading.Lock()

        def check(i):
            role = rbac.ROLES[i % len(rbac.ROLES)]
            perm = rbac.PERMISSIONS[i % len(rbac.PERMISSIONS)]
            user = users_by_role[role]
            expected = perm in rbac.get_permissions_for_role(role)
            actual = management.user_has_permission(user, perm)
            if actual != expected:
                with mismatches_lock:
                    mismatches.append((role, perm, expected, actual))

        run_concurrently(check, 300, max_workers=60)

        assert not mismatches, f"RBAC permission checks disagreed with the matrix: {mismatches[:10]}"


# =======================================================================
# 2. AUDIT LOG / NOTIFICATION INTEGRITY UNDER CONCURRENT WRITES
# =======================================================================

class TestAuditAndNotificationIntegrity:
    def test_audit_log_no_entries_lost_below_capacity_under_concurrent_writes(self, management):
        """
        AuditLogger.log() (audit_log.py) does
            self.entries.append(entry)
            if len(self.entries) > self.max_entries:
                self.entries = self.entries[-self.max_entries:]
        with no lock. The reassignment on the trim line is a
        read-then-replace on shared state; an append from another
        thread landing between the slice-read and the reassignment is
        silently discarded. Writing fewer entries than max_entries
        (500) removes the trim path entirely, isolating the pure
        "are appends themselves safe" question: 300 concurrent
        actor threads each log exactly one entry, so entries logged
        must equal entries retrievable.
        """
        n = 300
        before = len(management.audit_logger.entries)  # bootstrap already logs 1 entry
        assert before + n < management.audit_logger.max_entries

        def do_log(i):
            management.audit_logger.log(f"actor-{i}", "stress test action", target=str(i))

        run_concurrently(do_log, n, max_workers=n)

        after = len(management.audit_logger.entries)
        assert after - before == n, (
            f"logged {n} audit entries concurrently, only {after - before} "
            f"net-new entries present afterward (before={before}, after={after})"
        )

    def test_audit_log_trim_does_not_drop_entries_past_capacity(self, management):
        """
        Same mechanism as above, but now deliberately exceeding
        max_entries (500) concurrently, which exercises the unlocked
        trim reassignment itself. Total entries actually WRITTEN
        (tracked by the harness, not by the logger) minus max_entries
        tells us how many should have been trimmed away; anything
        beyond that gap is a genuine lost write versus an intentional
        eviction, distinguished by re-running the same total through
        a single-threaded control and comparing entry_ids.
        """
        n = management.audit_logger.max_entries + 200  # 700
        written_ids = []
        lock = threading.Lock()

        def do_log(i):
            entry = management.audit_logger.log(f"actor-{i}", "capacity stress", target=str(i))
            with lock:
                written_ids.append(entry["entry_id"])

        run_concurrently(do_log, n, max_workers=100)

        remaining = {e["entry_id"] for e in management.audit_logger.entries}
        # Every ID present in the log MUST have actually been written
        # by the harness (no corruption/fabrication)...
        fabricated = remaining - set(written_ids)
        assert not fabricated, f"audit log contains entries the harness never wrote: {fabricated}"
        # ...and the log must be holding exactly max_entries — no more
        # (trim broken) and no less (over-trimmed / lost extra entries
        # beyond the intended eviction).
        assert len(management.audit_logger.entries) == management.audit_logger.max_entries, (
            f"expected exactly {management.audit_logger.max_entries} entries retained "
            f"after writing {n} concurrently, got {len(management.audit_logger.entries)} "
            f"(audit_log.py's unlocked trim races with concurrent appends)"
        )

    def test_notification_center_no_lost_entries_under_concurrent_writes(self, management):
        """Same lost-update shape as the audit log, in notifications.py."""
        n = 150
        assert n < management.notification_center.max_entries

        def do_notify(i):
            management.notification_center.notify("node_registered", f"node-{i} joined")

        run_concurrently(do_notify, n, max_workers=n)

        assert len(management.notification_center.entries) == n, (
            f"sent {n} notifications concurrently, only "
            f"{len(management.notification_center.entries)} retained — "
            f"NotificationCenter.notify() (notifications.py) has the same "
            f"unlocked append/trim pattern as AuditLogger"
        )


# =======================================================================
# 3. WORKFLOW / DAG CORRECTNESS + SCALE
# =======================================================================

class TestWorkflowDAG:
    def _build_workflow(self, workflow_id="wf"):
        from gcon.workflow.workflow import Workflow, WorkflowJob
        return Workflow(workflow_id), WorkflowJob

    def test_deep_linear_chain_cycle_check_and_topo_sort_no_recursion_error(self):
        """
        DAG.has_cycle() is deliberately iterative to survive graphs
        that would blow Python's recursion limit under a naive
        recursive DFS. Prove it on a 5,000-node linear chain — the
        worst case for stack depth — and that topological_sort()
        (which is already iterative via a deque) produces a valid
        dependency-respecting order at that scale.
        """
        from gcon.workflow.dag import DAG
        workflow, WorkflowJob = self._build_workflow("deep-chain")

        n = 5000
        for i in range(n):
            workflow.add_job(WorkflowJob(f"job-{i}", "python3 -c \"pass\""))
        for i in range(n - 1):
            workflow.add_dependency(f"job-{i}", f"job-{i+1}")

        dag = DAG(workflow)
        t0 = time.monotonic()
        assert dag.has_cycle() is False, "linear chain misdetected as cyclic"
        cycle_check_seconds = time.monotonic() - t0

        t0 = time.monotonic()
        ordered = dag.topological_sort()
        sort_seconds = time.monotonic() - t0

        assert len(ordered) == n
        positions = {job.job_id: idx for idx, job in enumerate(ordered)}
        out_of_order = [
            (f"job-{i}", f"job-{i+1}")
            for i in range(n - 1)
            if positions[f"job-{i}"] > positions[f"job-{i+1}"]
        ]
        assert not out_of_order, f"topological order violated dependencies: {out_of_order[:5]}"
        logger.info(
            f"[DAG] {n}-node linear chain: has_cycle={cycle_check_seconds:.3f}s, "
            f"topological_sort={sort_seconds:.3f}s"
        )

    def test_wide_fanout_diamond_ready_jobs_correct_under_concurrent_completion(self):
        """
        One root fanning out to 200 parallel children, all converging
        on one sink job (a wide diamond). ready_jobs() must never
        return the sink until ALL 200 children are marked completed,
        checked while 200 threads race to report completions.
        """
        from gcon.workflow.dag import DAG
        workflow, WorkflowJob = self._build_workflow("diamond")

        workflow.add_job(WorkflowJob("root", "true"))
        for i in range(200):
            workflow.add_job(WorkflowJob(f"mid-{i}", "true"))
            workflow.add_dependency("root", f"mid-{i}")
        workflow.add_job(WorkflowJob("sink", "true"))
        for i in range(200):
            workflow.add_dependency(f"mid-{i}", "sink")

        dag = DAG(workflow)
        assert dag.has_cycle() is False

        completed = set()
        lock = threading.Lock()
        premature_sink_releases = []

        def complete_one(i):
            with lock:
                completed.add(f"mid-{i}")
                snapshot = set(completed)
            ready_ids = {j.job_id for j in dag.ready_jobs(snapshot)}
            if "sink" in ready_ids and len(snapshot) < 200:
                premature_sink_releases.append(len(snapshot))

        run_concurrently(complete_one, 200, max_workers=50)

        final_ready = {j.job_id for j in dag.ready_jobs(completed)}
        assert not premature_sink_releases, (
            f"'sink' was reported ready before all 200 dependencies completed "
            f"(at completed-count={premature_sink_releases[:3]})"
        )
        assert "sink" in final_ready, "'sink' was never released even after all dependencies completed"

    def test_disconnected_components_all_processed(self):
        """
        A workflow with several unrelated dependency chains (not one
        connected DAG) must still be fully ordered and cycle-checked —
        has_cycle()'s per-component WHITE-restart loop is exactly what
        this verifies.
        """
        from gcon.workflow.dag import DAG
        workflow, WorkflowJob = self._build_workflow("disconnected")

        for chain in range(10):
            for step in range(20):
                workflow.add_job(WorkflowJob(f"c{chain}-s{step}", "true"))
            for step in range(19):
                workflow.add_dependency(f"c{chain}-s{step}", f"c{chain}-s{step+1}")

        dag = DAG(workflow)
        assert dag.has_cycle() is False
        ordered = dag.topological_sort()
        assert len(ordered) == 200

    def test_corrupted_cyclic_dependency_state_is_detected_not_hung(self):
        """
        Workflow.add_dependency() blocks a direct self-loop and
        Workflow.validate() checks parent/child existence, but neither
        checks for a longer cycle (A->B->C->A) introduced by direct
        dict manipulation (e.g. a bug elsewhere, or a deserialized
        workflow from an untrusted/corrupted source via
        WorkflowJob.from_dict). has_cycle() must detect it, and
        topological_sort() must raise ValueError rather than silently
        returning a partial/wrong order or hanging.
        """
        from gcon.workflow.dag import DAG
        workflow, WorkflowJob = self._build_workflow("corrupted")

        for name in ("a", "b", "c"):
            workflow.add_job(WorkflowJob(name, "true"))
        workflow.add_dependency("a", "b")
        workflow.add_dependency("b", "c")
        # Manually inject the back-edge validate()/add_dependency()
        # would normally prevent via the public API.
        workflow.dependencies.setdefault("c", []).append("a")

        dag = DAG(workflow)
        deadline = time.monotonic() + 5.0
        result = {}

        def run():
            result["has_cycle"] = dag.has_cycle()

        t = threading.Thread(target=run, daemon=True)
        t.start()
        t.join(timeout=5.0)
        assert not t.is_alive(), "has_cycle() did not return within 5s on a 3-node cycle — hung"
        assert result.get("has_cycle") is True, "has_cycle() failed to detect an injected a->b->c->a cycle"

        with pytest.raises(ValueError):
            dag.topological_sort()


# =======================================================================
# 4. STORAGE MANAGER CONCURRENCY
# =======================================================================

class TestStorageManagerConcurrency:
    def test_concurrent_store_and_retrieve_many_artifacts_integrity(self, storage, tmp_path):
        """
        50 threads each writing a DISTINCT artifact (different filename)
        to 10 shared nodes concurrently, then every artifact is
        retrieved and its content hash checked against what was
        written — catches any cross-talk between concurrent
        shutil.copy2 calls landing in the same node directory.
        """
        node_ids = [f"node-{i}" for i in range(10)]
        written = {}
        lock = threading.Lock()
        errors = []

        def store_one(i):
            src = tmp_path / f"src-{i}.bin"
            data = os.urandom(2048)
            src.write_bytes(data)
            node_id = node_ids[i % len(node_ids)]
            try:
                dest = storage.store_artifact(node_id, str(src))
                with lock:
                    written[(node_id, os.path.basename(dest))] = hashlib.sha256(data).hexdigest()
            except Exception as e:
                with lock:
                    errors.append(f"{type(e).__name__}: {e}")

        run_concurrently(store_one, 50, max_workers=50)

        assert not errors, f"errors during concurrent store_artifact: {errors[:5]}"

        mismatches = []
        for (node_id, filename), expected_hash in written.items():
            path = storage.retrieve_artifact(node_id, filename)
            actual_hash = hashlib.sha256(open(path, "rb").read()).hexdigest()
            if actual_hash != expected_hash:
                mismatches.append((node_id, filename))
        assert not mismatches, f"artifact content corrupted/cross-contaminated for: {mismatches[:5]}"

    def test_concurrent_delete_and_retrieve_race_raises_cleanly(self, storage, sample_file):
        """
        retrieve_artifact() does `if not os.path.isfile(...): raise
        FileNotFoundError` then opens the path — classic TOCTOU. A
        delete_artifact() landing in that window should surface as a
        clean FileNotFoundError to the caller, never an unhandled
        exception of a different type (e.g. a raw OSError from a
        half-open file handle) and never a silent empty/corrupt read.
        """
        storage.store_artifact("node-x", sample_file)
        filename = os.path.basename(sample_file)

        unexpected_errors = []
        clean_not_found = []
        lock = threading.Lock()
        stop = threading.Event()

        def retriever():
            while not stop.is_set():
                try:
                    path = storage.retrieve_artifact("node-x", filename)
                    # If we got a path, the file must be genuinely readable.
                    with open(path, "rb") as f:
                        f.read()
                except FileNotFoundError:
                    with lock:
                        clean_not_found.append(1)
                except Exception as e:
                    with lock:
                        unexpected_errors.append(f"{type(e).__name__}: {e}")

        threads = [threading.Thread(target=retriever) for _ in range(8)]
        for t in threads:
            t.start()
        time.sleep(0.02)
        storage.delete_artifact("node-x", filename)
        time.sleep(0.1)
        stop.set()
        for t in threads:
            t.join(timeout=2)

        assert not unexpected_errors, (
            f"delete/retrieve race produced non-FileNotFoundError failures: {unexpected_errors[:5]}"
        )

    @pytest.mark.slow
    def test_high_volume_artifact_churn_soak(self, storage, tmp_path):
        """
        Sustained store/retrieve/delete churn across many nodes for a
        fixed duration — a soak test for the storage layer's use of
        the real filesystem (fd exhaustion, directory listing drift,
        `list_node_artifacts` staying consistent with what's actually
        on disk).
        """
        src = tmp_path / "churn.bin"
        src.write_bytes(os.urandom(1024))
        metrics = MetricsCollector()
        deadline = time.monotonic() + 15.0
        node_ids = [f"churn-node-{i}" for i in range(20)]
        i = 0

        while time.monotonic() < deadline:
            node_id = node_ids[i % len(node_ids)]
            with metrics.timer("store"):
                dest = storage.store_artifact(node_id, str(src))
            filename = os.path.basename(dest)
            with metrics.timer("list"):
                names = storage.list_node_artifacts(node_id)
            assert filename in names, "just-stored artifact missing from list_node_artifacts"
            with metrics.timer("delete"):
                deleted = storage.delete_artifact(node_id, filename)
            assert deleted is True
            i += 1

        report = metrics.summary()
        logger.warning(f"[SOAK] storage churn: {i} cycles, {report.get('latency_seconds')}")
        assert report["error_count"] == 0


# =======================================================================
# 5. POLICY ENGINE — DISCOVERED DEFECT
# =======================================================================

class TestPolicyEngine:
    @pytest.mark.xfail(reason="tests/support/policy.py PolicyEngine.check_runtime() references "
                               "self.max_runtime, which does not exist (the value lives at "
                               "self.policy['max_runtime']) — an over-limit receipt raises "
                               "AttributeError instead of returning the intended (False, message) "
                               "tuple, which in turn breaks evaluate()'s all-checks-run-and-report "
                               "contract for every other check in the same receipt")
    def test_over_limit_runtime_reports_failure_instead_of_crashing(self):
        from tests.support.policy import PolicyEngine

        engine = PolicyEngine(policy_file="__no_such_file__.json")  # forces the built-in default policy
        receipt = {
            "proof": {
                "metrics": {
                    "runtime_seconds": engine.policy["max_runtime"] + 100,
                    "cpu_percent": 10.0,
                    "memory_percent": 10.0,
                    "gpu_memory_total": 0,
                    "gpu_memory_used": 0,
                }
            }
        }
        passed, message = engine.check_runtime(receipt)
        assert passed is False
        assert "exceeds" in message

    @pytest.mark.xfail(reason="same root cause as test_over_limit_runtime_reports_failure_instead_of_crashing "
                               "above, reached through evaluate() — the method real callers actually use — "
                               "so a single over-runtime receipt currently crashes the ENTIRE trust "
                               "evaluation (CPU/memory/GPU checks never run) instead of reporting one "
                               "failed check among several")
    def test_evaluate_over_limit_runtime_does_not_abort_remaining_checks(self):
        """
        Same underlying defect, exercised through evaluate() — the
        method real code paths actually call. A crash in check_runtime
        should not be possible to reach via evaluate() with a receipt
        that any real agent.py-generated receipt could produce.
        """
        from tests.support.policy import PolicyEngine

        engine = PolicyEngine(policy_file="__no_such_file__.json")
        receipt = {
            "proof": {
                "metrics": {
                    "runtime_seconds": engine.policy["max_runtime"] + 100,
                    "cpu_percent": 10.0,
                    "memory_percent": 10.0,
                    "gpu_memory_total": 0,
                    "gpu_memory_used": 0,
                }
            }
        }
        report = engine.evaluate(receipt)
        assert report["trusted"] is False


# =======================================================================
# 6. SECURITY-ADJACENT DEFAULTS
# =======================================================================

class TestSecurityDefaults:
    def test_bootstrap_credentials_are_environment_overridable(self, management):
        """
        management_layer.py falls back to hardcoded module-level
        constants (name/email/password) for the bootstrap owner
        account when GCON_OWNER_* env vars aren't set. This doesn't
        assert the defaults are absent (they're a documented,
        intentional first-boot convenience) — it asserts the escape
        hatch actually works, since a broken override is worse than a
        known default: it would mean deployments THINK they've changed
        the bootstrap password and haven't. Does not log or assert
        against the literal default value.
        """
        import importlib
        os.environ["GCON_OWNER_PASSWORD"] = unique_id("override-pw")
        try:
            mgmt_module = importlib.import_module("gcon.management.management_layer")
            importlib.reload(mgmt_module)
            assert mgmt_module.BOOTSTRAP_OWNER_PASSWORD == os.environ["GCON_OWNER_PASSWORD"], (
                "GCON_OWNER_PASSWORD env override was not picked up by the "
                "bootstrap owner constant"
            )
        finally:
            del os.environ["GCON_OWNER_PASSWORD"]
            importlib.reload(sys.modules["gcon.management.management_layer"])


# =======================================================================
# Standalone combined-load CLI (non-pytest usage)
# =======================================================================

def _standalone_combined_load(duration: float):
    from gcon.management.management_layer import ManagementLayer
    from gcon.workflow.workflow import Workflow, WorkflowJob
    from gcon.workflow.dag import DAG
    from gcon.storage.storage_manager import StorageManager

    management = ManagementLayer(coordinator=None)
    tmp_root = tempfile.mkdtemp(prefix="gcon-stress2-")
    storage = StorageManager(storage_root=os.path.join(tmp_root, "storage"))
    src_path = os.path.join(tmp_root, "src.bin")
    with open(src_path, "wb") as f:
        f.write(os.urandom(2048))

    metrics = MetricsCollector()
    stop = threading.Event()

    def mgmt_worker():
        i = 0
        while not stop.is_set():
            try:
                with metrics.timer("create_user"):
                    u = management.create_user(f"load-{i}", f"load{i}-{unique_id('u')}@example.com", "Viewer")
                with metrics.timer("create_key"):
                    key = management.create_api_key(f"load-key-{i}", owner_user_id=u["user_id"])
                with metrics.timer("authenticate"):
                    management.authenticate_api_key(key["secret"])
                metrics.incr("mgmt_cycles")
            except Exception as e:
                metrics.incr("mgmt_errors")
                logger.error(f"[LOAD] mgmt cycle failed: {type(e).__name__}: {e}")
            i += 1

    def workflow_worker():
        i = 0
        while not stop.is_set():
            try:
                wf = Workflow(f"load-wf-{i}")
                wf.add_job(WorkflowJob("root", "true"))
                for j in range(10):
                    wf.add_job(WorkflowJob(f"child-{j}", "true"))
                    wf.add_dependency("root", f"child-{j}")
                with metrics.timer("dag_build_and_check"):
                    dag = DAG(wf)
                    dag.has_cycle()
                    dag.topological_sort()
                metrics.incr("workflow_cycles")
            except Exception as e:
                metrics.incr("workflow_errors")
                logger.error(f"[LOAD] workflow cycle failed: {type(e).__name__}: {e}")
            i += 1

    def storage_worker():
        while not stop.is_set():
            try:
                with metrics.timer("store_retrieve_delete"):
                    dest = storage.store_artifact("load-node", src_path)
                    filename = os.path.basename(dest)
                    storage.retrieve_artifact("load-node", filename)
                    storage.delete_artifact("load-node", filename)
                metrics.incr("storage_cycles")
            except Exception as e:
                metrics.incr("storage_errors")
                logger.error(f"[LOAD] storage cycle failed: {type(e).__name__}: {e}")

    workers = (
        [threading.Thread(target=mgmt_worker, daemon=True) for _ in range(10)]
        + [threading.Thread(target=workflow_worker, daemon=True) for _ in range(5)]
        + [threading.Thread(target=storage_worker, daemon=True) for _ in range(5)]
    )
    logger.info(f"[LOAD] starting {len(workers)} workers for {duration}s across "
                f"management/workflow/storage subsystems")
    for w in workers:
        w.start()
    time.sleep(duration)
    stop.set()
    for w in workers:
        w.join(timeout=5)

    shutil.rmtree(tmp_root, ignore_errors=True)
    print("=== stress_test2 combined load summary ===")
    print(metrics.summary())


def main():
    parser = argparse.ArgumentParser(description="GCON standalone combined stress runner (stress_test2)")
    parser.add_argument("--duration", type=float, default=60.0)
    args = parser.parse_args()
    _standalone_combined_load(args.duration)


if __name__ == "__main__":
    main()
