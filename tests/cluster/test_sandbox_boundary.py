"""
The sandbox boundary is "declared AND verified execution capability", not who
owns the worker:

  worker                               sandboxed   customer jobs?
  GCON-managed / customer w/ sandbox      yes        yes
  customer worker, unsandboxed            no         no
  internal trusted worker                 no         internal jobs only

No Docker daemon is needed: a stub agent that declares `sandboxed = True`
stands in for a container worker (the unit under test is the coordinator's
decision), and the coordinator-side evidence check is driven with results the
test signs itself.
"""
import json
import time

import pytest

pytestmark = pytest.mark.real_sandbox

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution import docker_executor
from gcon.execution.agent import GCONAgent
from gcon.execution.worker_identity import (
    build_attestation_payload, ensure_node_keypair, sign_attestation,
)
from gcon.persistence.control_plane import ControlPlane
from gcon.transport.remote_node import RemoteNodeProxy


class SandboxedAgent(GCONAgent):
    sandboxed = True


def _agent(cls, node_id, org_id=None):
    agent = cls(node_id=node_id)
    agent.org_id = org_id
    return agent


def _wait_for(predicate, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def trusted_coordinator(monkeypatch, tmp_path):
    # The most permissive deployment setting there is.
    monkeypatch.setenv("GCON_SANDBOX_POLICY", "trusted")
    plane = ControlPlane(path=str(tmp_path / "cp.db"))
    coord = GCONCoordinator(control_plane=plane)
    yield coord
    coord.shutdown()
    plane.close()


def _status(coord, job_id):
    return coord.jobs[job_id]["status"]


class TestWhoMayRunWhat:
    def test_a_public_job_never_runs_on_an_unsandboxed_worker_even_under_trusted(self, trusted_coordinator):
        coord = trusted_coordinator
        coord.register_agent(GCONAgent(node_id="raw"))          # org-less, unsandboxed
        coord.submit_job("pub-1", "echo hi", sandbox_required=True)
        time.sleep(1.5)
        assert _status(coord, "pub-1") == "pending"
        assert coord.jobs["pub-1"].get("node_id") is None

    def test_an_internal_job_does_run_on_an_unsandboxed_worker_under_trusted(self, trusted_coordinator):
        coord = trusted_coordinator
        coord.register_agent(GCONAgent(node_id="raw"))
        coord.submit_job("int-1", "echo hi")                    # no org, not public
        assert _wait_for(lambda: _status(coord, "int-1") == "completed")

    def test_a_customer_workers_unsandboxed_node_gets_no_customer_jobs(self, trusted_coordinator):
        coord = trusted_coordinator
        coord.register_agent(_agent(GCONAgent, "acme-raw", "acme"))
        coord.submit_job("acme-1", "echo hi", org_id="acme")
        time.sleep(1.5)
        assert _status(coord, "acme-1") == "pending"

    def test_a_customer_worker_with_a_sandbox_does_get_its_own_jobs(self, trusted_coordinator):
        coord = trusted_coordinator
        coord.register_agent(_agent(SandboxedAgent, "acme-box", "acme"))
        coord.submit_job("acme-2", "echo hi", org_id="acme", sandbox_required=True)
        assert _wait_for(lambda: _status(coord, "acme-2") == "completed")

    def test_the_requirement_survives_a_restart_for_org_jobs(self, monkeypatch, tmp_path):
        monkeypatch.setenv("GCON_SANDBOX_POLICY", "trusted")
        db = str(tmp_path / "cp.db")
        first = GCONCoordinator(control_plane=ControlPlane(path=db))
        first.submit_job("acme-3", "echo hi", org_id="acme")    # no worker: stays pending
        first.shutdown()
        again = GCONCoordinator(control_plane=ControlPlane(path=db))
        try:
            again.register_agent(_agent(GCONAgent, "acme-raw", "acme"))
            time.sleep(1.5)
            assert again.jobs["acme-3"]["status"] == "pending"
        finally:
            again.shutdown()


# ---- the coordinator checks what the worker SIGNED about each public job ------

class _FakeTransport:
    pass


@pytest.fixture
def evidence_env(trusted_coordinator, tmp_path):
    coord = trusted_coordinator
    key, public_pem = ensure_node_keypair(str(tmp_path), "remote-1")
    return coord, key, public_pem


def _result(key, *, job_id="j1", node_id="remote-1", backend="docker", sign_with=None):
    payload = build_attestation_payload(
        job_id=job_id, attempt_id="a1", node_id=node_id, job_spec_hash="h",
        output_hash="o", status="success", timestamp="t", execution_backend=backend,
    )
    return {
        "status": "success", "stdout": "secret output",
        "worker_attestation_payload_json": json.dumps(payload, sort_keys=True),
        "worker_attestation_signature": sign_attestation(sign_with or key, payload),
    }


def _node(coord, public_pem):
    node = RemoteNodeProxy("remote-1", _FakeTransport(), sandboxed=True)
    coord.registry.register(node)
    # What the Register RPC stores for a real worker.
    coord.control_plane.nodes.upsert("remote-1", "host-1", status="idle", ed25519_public_key=public_pem)
    return node


class TestSignedEvidence:
    JOB = {"require_sandbox": True, "trace_id": None}

    def test_a_docker_attestation_signed_by_the_registered_key_is_accepted(self, evidence_env):
        coord, key, pem = evidence_env
        node = _node(coord, pem)
        res = coord._enforce_sandbox_evidence("j1", dict(self.JOB), node, _result(key))
        assert res["status"] == "success" and node.sandboxed is True

    @pytest.mark.parametrize("make", [
        lambda key: {k: v for k, v in _result(key).items() if not k.startswith("worker_attestation")},
        lambda key: _result(key, backend="subprocess"),
        lambda key: _result(key, job_id="someone-elses-job"),
        lambda key: _result(key, node_id="another-node"),
    ], ids=["no attestation", "subprocess backend", "other job", "other node"])
    def test_anything_short_of_a_matching_docker_attestation_is_rejected(self, evidence_env, make):
        coord, key, pem = evidence_env
        node = _node(coord, pem)
        res = coord._enforce_sandbox_evidence("j1", dict(self.JOB), node, make(key))
        assert res["status"] == "failed" and res["stdout"] == ""
        assert "Sandbox requirement not met" in res["error"]
        assert node.sandboxed is False                      # demoted ...
        assert coord.registry.get_node_info("remote-1").get("quarantined") is True   # ... and quarantined

    def test_an_attestation_signed_by_a_different_key_is_rejected(self, evidence_env, tmp_path):
        coord, key, pem = evidence_env
        (tmp_path / "other").mkdir()
        other, _ = ensure_node_keypair(str(tmp_path / "other"), "remote-1")
        node = _node(coord, pem)
        res = coord._enforce_sandbox_evidence("j1", dict(self.JOB), node, _result(key, sign_with=other))
        assert res["status"] == "failed"

    def test_a_job_that_did_not_require_a_sandbox_is_not_checked(self, evidence_env):
        coord, key, pem = evidence_env
        node = _node(coord, pem)
        coord._sandbox_policy = "trusted"
        res = coord._enforce_sandbox_evidence("j1", {"require_sandbox": False}, node, {"status": "success"})
        assert res["status"] == "success"


# ---- the worker's own startup probe ------------------------------------------

class _Proc:
    def __init__(self, out, code=0, err=""):
        self.stdout, self.returncode, self.stderr = out, code, err


GOOD = "CapEff:\t0000000000000000\nNoNewPrivs:\t1\nNETDEVS=lo\nIO_WRITABLE=1\nPROBE_DONE\n"


class TestWorkerProbe:
    def _run(self, monkeypatch, tmp_path, proc, network="none"):
        monkeypatch.setenv("GCON_JOB_IO_ROOT", str(tmp_path))
        monkeypatch.setenv("GCON_TLS_CERT_DIR", str(tmp_path / "certs"))
        monkeypatch.setattr(docker_executor.subprocess, "run", lambda *a, **k: proc)
        return docker_executor.verify_sandbox(image="python:3.12-slim", network=network)

    def test_a_properly_isolated_container_passes(self, monkeypatch, tmp_path):
        verdict = self._run(monkeypatch, tmp_path, _Proc(GOOD))
        assert verdict["ok"], verdict

    @pytest.mark.parametrize("out,failed", [
        (GOOD.replace("0000000000000000", "00000000a80425fb"), "no_capabilities"),
        (GOOD.replace("NoNewPrivs:\t1", "NoNewPrivs:\t0"), "no_new_privileges"),
        (GOOD.replace("NETDEVS=lo", "NETDEVS=eth0 lo"), "network_isolated"),
        (GOOD.replace("IO_WRITABLE=1\n", ""), "io_dir_writable"),
        (GOOD.replace("PROBE_DONE", "CRED_VISIBLE=/etc/gcon/certs\nPROBE_DONE"), "credentials_not_visible"),
    ])
    def test_each_missing_property_fails_the_probe(self, monkeypatch, tmp_path, out, failed):
        verdict = self._run(monkeypatch, tmp_path, _Proc(out))
        assert not verdict["ok"] and f"sandbox check failed: {failed}" in verdict["failures"]

    def test_a_container_that_cannot_run_fails_the_probe(self, monkeypatch, tmp_path):
        verdict = self._run(monkeypatch, tmp_path, _Proc("", 125, "Cannot connect to the Docker daemon"))
        assert not verdict["ok"] and "Docker daemon" in verdict["failures"][0]

    def test_network_is_not_required_to_be_isolated_when_the_operator_allows_it(self, monkeypatch, tmp_path):
        verdict = self._run(monkeypatch, tmp_path, _Proc(GOOD.replace("NETDEVS=lo", "NETDEVS=eth0 lo")), network="bridge")
        assert verdict["ok"], verdict


class TestCredentialsNeverMounted:
    def test_a_mount_overlapping_the_cert_dir_is_refused(self, monkeypatch, tmp_path):
        certs = tmp_path / "certs"
        certs.mkdir()
        monkeypatch.setenv("GCON_TLS_CERT_DIR", str(certs))
        for bad in (str(certs), str(certs / "inner"), str(tmp_path)):
            with pytest.raises(PermissionError):
                docker_executor.assert_mount_excludes_credentials(bad)

    def test_a_sibling_directory_is_fine(self, monkeypatch, tmp_path):
        monkeypatch.setenv("GCON_TLS_CERT_DIR", str(tmp_path / "certs"))
        (tmp_path / "io").mkdir()
        docker_executor.assert_mount_excludes_credentials(str(tmp_path / "io"))

    def test_the_run_command_mounts_only_the_job_io_dir_and_nothing_privileged(self, monkeypatch, tmp_path):
        monkeypatch.setenv("GCON_TLS_CERT_DIR", str(tmp_path / "certs"))
        io = tmp_path / "io"
        io.mkdir()
        cmd = docker_executor.build_docker_run_command("j1", "echo hi", "img", str(io))
        mounts = [cmd[i + 1] for i, a in enumerate(cmd) if a in ("-v", "--volume")]
        assert mounts == [f"{io}:/gcon_io"]
        assert "--privileged" not in cmd and "docker.sock" not in " ".join(cmd)
        assert cmd[cmd.index("--cap-drop") + 1] == "ALL" and "no-new-privileges" in cmd
        assert cmd[cmd.index("--network") + 1] == "none"
