"""
GCON_JOB_RUN_AS_USER: run raw-subprocess jobs as a separate unprivileged OS
user, so a job can't read the worker's own private keys off disk (they are
0600 files owned by the worker). Without it a job runs as the worker itself
and can -- enough to impersonate the node.

Two layers:
  * Configuration checks run anywhere (the OS calls are patched) and pin the
    fail-closed behavior: a setup that can't actually deliver the separation
    refuses to start, rather than quietly running jobs as the worker.
  * Behavior tests run real jobs as a real second user. They need root and
    `useradd`, so they are skipped otherwise.
"""
import os
import shutil
import subprocess
import uuid

import pytest

from gcon.execution.agent import GCONAgent
from gcon.execution.docker_executor import job_io_path


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("GCON_JOB_RUN_AS_USER", raising=False)
    monkeypatch.delenv("GCON_EXECUTION_BACKEND", raising=False)


class TestConfigurationFailsClosed:
    def test_unset_changes_nothing(self):
        agent = GCONAgent("n")
        assert agent.job_user is None
        assert agent._job_popen_identity == {} and agent._job_env_identity == {}

    def test_refuses_on_windows(self, monkeypatch):
        agent = GCONAgent("n")
        monkeypatch.setattr(os, "name", "nt")
        with pytest.raises(RuntimeError, match="Linux/macOS"):
            agent._configure_job_user("someone")

    def test_refuses_when_the_worker_is_not_root(self, monkeypatch):
        agent = GCONAgent("n")
        monkeypatch.setattr(os, "geteuid", lambda: 1000)
        with pytest.raises(RuntimeError, match="start as root"):
            agent._configure_job_user("someone")

    def test_refuses_an_unknown_user(self, monkeypatch):
        agent = GCONAgent("n")
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        with pytest.raises(RuntimeError, match="no such user"):
            agent._configure_job_user("no-such-user-" + uuid.uuid4().hex[:8])

    def test_refuses_root_as_the_job_user(self, monkeypatch):
        agent = GCONAgent("n")
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        with pytest.raises(RuntimeError, match="is root"):
            agent._configure_job_user("root")

    def test_a_bad_setting_stops_the_worker_at_startup_not_at_job_time(self, monkeypatch):
        monkeypatch.setenv("GCON_JOB_RUN_AS_USER", "no-such-user-" + uuid.uuid4().hex[:8])
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        with pytest.raises(RuntimeError):
            GCONAgent("n")

    def test_ignored_with_the_docker_backend(self, monkeypatch, caplog):
        monkeypatch.setenv("GCON_EXECUTION_BACKEND", "docker")
        monkeypatch.setenv("GCON_JOB_RUN_AS_USER", "whoever")
        agent = GCONAgent("n")  # must not raise: docker jobs aren't run as a host user
        assert agent.job_user is None and agent._job_popen_identity == {}


needs_root = pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() != 0 or shutil.which("useradd") is None,
    reason="needs root and useradd to create a real second user",
)


@pytest.fixture
def job_user():
    name = "gcontest" + uuid.uuid4().hex[:6]
    subprocess.run(["useradd", "-m", "-s", "/bin/sh", name], check=True, capture_output=True)
    yield name
    subprocess.run(["userdel", "-r", name], capture_output=True)


@pytest.fixture
def key_dir(tmp_path_factory):
    """A directory the job user CAN traverse, holding a private key file and a
    readable control file -- so a denial can only be down to the file's own
    permissions, not an unreadable parent directory."""
    import tempfile
    path = tempfile.mkdtemp(prefix="gcon-keys-")
    os.chmod(path, 0o755)
    private = os.path.join(path, "agent-node.key.pem")
    readable = os.path.join(path, "public-note.txt")
    for file, mode in ((private, 0o600), (readable, 0o644)):
        with open(file, "w") as handle:
            handle.write("SENTINEL")
        os.chmod(file, mode)
    yield path
    shutil.rmtree(path, ignore_errors=True)


@needs_root
class TestJobsRunAsTheSeparateUser:
    def _agent(self, monkeypatch, user):
        monkeypatch.setenv("GCON_JOB_RUN_AS_USER", user)
        return GCONAgent("separated")

    def test_the_job_is_not_root_and_has_its_own_home(self, monkeypatch, job_user):
        agent = self._agent(monkeypatch, job_user)
        out = agent.execute_job("j1", 'echo "$(id -un)|$HOME|$USER"', timeout=20)["stdout"].strip()
        assert out == f"{job_user}|/home/{job_user}|{job_user}"

    def test_the_workers_private_key_is_unreadable_to_the_job(self, monkeypatch, job_user, key_dir):
        agent = self._agent(monkeypatch, job_user)
        script = (
            f'cat "{key_dir}/public-note.txt" >/dev/null 2>&1 && echo control=readable || echo control=DENIED; '
            f'cat "{key_dir}/agent-node.key.pem" >/dev/null 2>&1 && echo key=READABLE || echo key=denied'
        )
        out = agent.execute_job("j2", script, timeout=20)["stdout"]
        assert "control=readable" in out   # the directory itself is reachable
        assert "key=denied" in out         # the 0600 file is not

    def test_without_the_setting_the_same_job_can_read_the_key(self, key_dir):
        """The control for the test above: this is the hole being closed."""
        agent = GCONAgent("plain")
        out = agent.execute_job("j3", f'cat "{key_dir}/agent-node.key.pem"', timeout=20)["stdout"]
        assert "SENTINEL" in out

    def test_usage_reports_still_work(self, monkeypatch, job_user):
        agent = self._agent(monkeypatch, job_user)
        usage = job_io_path("usage", "j4", ".json")
        result = agent.execute_job(
            "j4", 'echo \'{"gpu_seconds": 2}\' > "$GCON_USAGE_REPORT_PATH"; echo done',
            timeout=20, usage_report_path=usage,
        )
        assert result["status"] == "success" and "done" in result["stdout"]
        # The job (as the other user) wrote the file, and the agent read it back.
        assert result["usage"] == {"gpu_seconds": 2}

    def test_report_directory_is_shared_with_that_user_only(self, monkeypatch, job_user):
        agent = self._agent(monkeypatch, job_user)
        agent.execute_job("j5", "true", timeout=20)
        stat = os.stat(os.path.dirname(job_io_path("usage", "j5", ".json")))
        assert stat.st_mode & 0o777 == 0o770 and stat.st_gid == agent._job_io_gid

    def test_timeout_still_kills_the_whole_job(self, monkeypatch, job_user):
        import time
        agent = self._agent(monkeypatch, job_user)
        started = time.time()
        result = agent.execute_job("j6", "sleep 30", timeout=2)
        assert result["status"] == "timeout" and time.time() - started < 15

    def test_gcon_variables_are_still_scrubbed(self, monkeypatch, job_user):
        monkeypatch.setenv("GCON_AGENT_KEY_B64", "SENTINEL")
        agent = self._agent(monkeypatch, job_user)
        assert "SENTINEL" not in agent.execute_job("j7", 'echo "[$GCON_AGENT_KEY_B64]"', timeout=20)["stdout"]
