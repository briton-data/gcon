"""
Hardening of the docker execution backend.

1. The container mounts only a dedicated per-user job-IO directory -- not the
   whole system temp dir, which used to expose everything else in /tmp to the
   job.
2. Report-file names built from a caller-supplied job_id can't escape that
   directory (job_id is an arbitrary string: it may contain "/" or "..").
3. The container runs with no Linux capabilities, no privilege escalation and
   a process cap; a non-root user is available as an opt-in.

No Docker daemon is needed: command construction is checked directly, and the
agent's docker branch is driven with a stand-in Popen that records the argv it
would have executed.
"""
import os
import stat

import pytest

from gcon.execution import docker_executor
from gcon.execution.agent import GCONAgent
from gcon.execution.docker_executor import (
    CONTAINER_MOUNT,
    build_docker_run_command,
    job_io_dir,
    job_io_path,
)


@pytest.fixture
def temp_root(tmp_path, monkeypatch):
    """Point the system temp dir at an isolated directory."""
    monkeypatch.setattr(docker_executor.tempfile, "gettempdir", lambda: str(tmp_path))
    return tmp_path


def _cmd(**kw):
    return build_docker_run_command("job-1", "echo hi", "python:3.12-slim", "/tmp/io", **kw)


def _pair(cmd, flag):
    """The value that follows `flag`, asserting the flag is present."""
    assert flag in cmd, f"{flag} missing from {cmd}"
    return cmd[cmd.index(flag) + 1]


class TestContainerFlags:
    def test_capabilities_are_dropped_and_escalation_blocked_by_default(self):
        cmd = _cmd()
        assert _pair(cmd, "--cap-drop") == "ALL"
        assert _pair(cmd, "--security-opt") == "no-new-privileges"

    def test_process_cap_defaults_to_512(self):
        assert _pair(_cmd(), "--pids-limit") == "512"

    def test_process_cap_is_configurable_and_can_be_unlimited(self):
        assert _pair(_cmd(pids_limit="128"), "--pids-limit") == "128"
        assert _pair(_cmd(pids_limit="-1"), "--pids-limit") == "-1"
        assert "--pids-limit" not in _cmd(pids_limit=None)

    def test_non_root_user_is_opt_in(self):
        assert "--user" not in _cmd()
        assert _pair(_cmd(user="1000:1000"), "--user") == "1000:1000"

    def test_every_flag_comes_before_the_image(self):
        """Anything after the image is handed to the container's command, not
        to docker -- a hardening flag placed there would silently do nothing."""
        cmd = _cmd(memory_limit="2g", cpu_limit="1", network="none", gpus="all",
                   pids_limit="64", user="1000", extra_env={"A": "b"})
        image_at = cmd.index("python:3.12-slim")
        for flag in ("--cap-drop", "--security-opt", "--pids-limit", "--user",
                     "--memory", "--cpus", "--network", "--gpus", "-v", "-e"):
            assert cmd.index(flag) < image_at, flag
        assert cmd[image_at:] == ["python:3.12-slim", "sh", "-c", "echo hi"]


class TestJobIoDirectory:
    def test_created_private_and_reused(self, temp_root):
        path = job_io_dir()
        assert os.path.dirname(path) == str(temp_root)
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o700
        assert job_io_dir() == path  # idempotent

    def test_is_named_per_os_user(self, temp_root):
        expected = f"gcon-job-io-{os.getuid()}" if hasattr(os, "getuid") else "gcon-job-io-"
        assert os.path.basename(job_io_dir()).startswith(expected)

    def test_a_directory_owned_by_someone_else_is_refused(self, temp_root, monkeypatch):
        """In a shared /tmp another user could pre-create the name to squat it."""
        path = job_io_dir()
        real_uid = os.stat(path).st_uid
        real_stat = os.stat
        monkeypatch.setattr(
            os, "stat",
            lambda p, *a, **k: type("S", (), {"st_uid": real_uid + 1, "st_mode": real_stat(p).st_mode})()
            if str(p) == path else real_stat(p, *a, **k),
        )
        with pytest.raises(PermissionError, match="owned by another user"):
            job_io_dir()

    def test_a_symlink_in_its_place_is_refused(self, temp_root):
        target = temp_root / "elsewhere"
        target.mkdir()
        link = temp_root / os.path.basename(job_io_dir())
        link.rmdir()
        link.symlink_to(target)
        with pytest.raises(PermissionError, match="not a plain directory"):
            job_io_dir()

    def test_a_plain_file_in_its_place_is_refused(self, temp_root):
        name = temp_root / os.path.basename(job_io_dir())
        name.rmdir()
        name.write_text("x")
        with pytest.raises((PermissionError, FileExistsError)):
            job_io_dir()


class TestReportPathsCannotEscape:
    HOSTILE = ["../../etc/passwd", "a/../../x", "/etc/cron.d/x", "..", "a\\..\\b", "", "job 1;rm -rf /", "x" * 3 + "/../.."]

    @pytest.mark.parametrize("job_id", HOSTILE)
    def test_stays_inside_the_io_directory(self, temp_root, job_id):
        for kind, suffix in (("usage", ".json"), ("stages", ".jsonl")):
            path = job_io_path(kind, job_id, suffix)
            assert os.path.dirname(path) == job_io_dir()
            assert os.path.realpath(path).startswith(os.path.realpath(job_io_dir()) + os.sep)

    def test_ordinary_ids_keep_the_same_file_names_as_before(self, temp_root):
        assert os.path.basename(job_io_path("usage", "job-42", ".json")) == "gcon-usage-job-42.json"
        assert os.path.basename(job_io_path("stages", "job-42", ".jsonl")) == "gcon-stages-job-42.jsonl"

    def test_ids_that_had_to_be_altered_cannot_collide(self, temp_root):
        a, b, plain = (job_io_path("usage", i, ".json") for i in ("a/b", "a-b", "a b"))
        assert len({a, b, plain}) == 3

    def test_deterministic(self, temp_root):
        assert job_io_path("usage", "x/y", ".json") == job_io_path("usage", "x/y", ".json")


class _FakeProcess:
    returncode = 0
    pid = 0

    def communicate(self, timeout=None):
        return "", ""

    def poll(self):
        return 0


class TestAgentDockerBranch:
    def _run(self, monkeypatch, temp_root, **env):
        monkeypatch.setenv("GCON_EXECUTION_BACKEND", "docker")
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        seen = {}

        def fake_popen(command, *a, **k):
            # The agent also shells out to nvidia-smi for GPU sampling; only
            # the docker invocation is what's under test.
            if isinstance(command, list) and command and command[0] == "docker":
                seen["command"] = command
            return _FakeProcess()

        monkeypatch.setattr("gcon.execution.agent.subprocess.Popen", fake_popen)
        agent = GCONAgent("docker-node")
        usage = job_io_path("usage", "job-9", ".json")
        agent.execute_job("job-9", "echo hi", timeout=5, usage_report_path=usage)
        return seen["command"], usage

    def test_only_the_dedicated_directory_is_mounted(self, monkeypatch, temp_root):
        command, _ = self._run(monkeypatch, temp_root)
        mount = _pair(command, "-v")
        host_side, container_side = mount.rsplit(":", 1)
        assert container_side == CONTAINER_MOUNT
        # A private directory for THIS run, inside the dedicated job-IO
        # directory -- not the shared job-IO directory itself (which every
        # job on the worker would see) and not the whole temp dir.
        assert os.path.dirname(host_side) == job_io_dir()
        assert os.path.basename(host_side).startswith("run-")
        assert host_side != str(temp_root)

    def test_report_path_is_remapped_into_the_container(self, monkeypatch, temp_root):
        command, _usage = self._run(monkeypatch, temp_root)
        env_flags = [command[i + 1] for i, c in enumerate(command) if c == "-e"]
        # The report file lives in the run's private directory (see execute_job).
        assert f"GCON_USAGE_REPORT_PATH={CONTAINER_MOUNT}/usage.json" in env_flags

    def test_hardening_flags_reach_the_real_command(self, monkeypatch, temp_root):
        command, _ = self._run(monkeypatch, temp_root)
        assert _pair(command, "--cap-drop") == "ALL"
        assert _pair(command, "--security-opt") == "no-new-privileges"
        assert _pair(command, "--pids-limit") == "512"
        assert "--user" not in command

    def test_environment_overrides_are_honored(self, monkeypatch, temp_root):
        command, _ = self._run(
            monkeypatch, temp_root,
            GCON_JOB_DOCKER_PIDS_LIMIT="64", GCON_JOB_DOCKER_USER="1000:1000",
        )
        assert _pair(command, "--pids-limit") == "64"
        assert _pair(command, "--user") == "1000:1000"


class TestNetworkIsDeniedByDefault:
    """Docker's own default bridge gives a job outbound internet and, on a
    cloud host, the instance-metadata endpoint (169.254.169.254) and its
    credentials. A sandbox shouldn't hand that to arbitrary job code."""

    def test_the_command_always_states_a_network_and_it_is_none_by_default(self):
        assert _pair(_cmd(), "--network") == "none"

    def test_an_operator_can_opt_in_to_a_network(self):
        assert _pair(_cmd(network="bridge"), "--network") == "bridge"
        assert _pair(_cmd(network="my-jobs-net"), "--network") == "my-jobs-net"

    @pytest.mark.parametrize("blank", ["", "   "])
    def test_a_blank_setting_means_none_not_dockers_default(self, blank):
        assert _pair(_cmd(network=blank), "--network") == "none"

    @pytest.mark.parametrize("host", ["host", "HOST", " Host "])
    def test_host_networking_is_refused(self, host):
        with pytest.raises(ValueError, match="not allowed"):
            _cmd(network=host)

    def test_network_flag_comes_before_the_image(self):
        cmd = _cmd()
        assert cmd.index("--network") < cmd.index("python:3.12-slim")


class TestAgentNetworkSetting:
    def test_defaults_to_none(self, monkeypatch):
        monkeypatch.delenv("GCON_JOB_DOCKER_NETWORK", raising=False)
        assert GCONAgent("n").docker_network == "none"

    def test_blank_env_var_is_none(self, monkeypatch):
        monkeypatch.setenv("GCON_JOB_DOCKER_NETWORK", "")
        assert GCONAgent("n").docker_network == "none"

    def test_opt_in_is_honored(self, monkeypatch):
        monkeypatch.setenv("GCON_JOB_DOCKER_NETWORK", "bridge")
        assert GCONAgent("n").docker_network == "bridge"

    def test_host_stops_the_worker_at_startup(self, monkeypatch):
        monkeypatch.setenv("GCON_JOB_DOCKER_NETWORK", "host")
        with pytest.raises(ValueError, match="not allowed"):
            GCONAgent("n")

    def test_the_real_docker_command_carries_it(self, monkeypatch, temp_root):
        monkeypatch.setenv("GCON_EXECUTION_BACKEND", "docker")
        seen = {}

        def fake_popen(command, *a, **k):
            if isinstance(command, list) and command and command[0] == "docker":
                seen["command"] = command
            return _FakeProcess()

        monkeypatch.setattr("gcon.execution.agent.subprocess.Popen", fake_popen)
        GCONAgent("docker-node").execute_job("job-n", "echo hi", timeout=5)
        assert _pair(seen["command"], "--network") == "none"


class TestWindowsHostPaths:
    """A Windows worker's paths use backslashes and drive letters; a plain
    string-prefix test against "<dir>/" never matched them, so docker mode
    raised on every job there."""

    WIN_DIR = r"C:\Users\me\AppData\Local\Temp\gcon-job-io-me"

    def _env(self, cmd):
        return [cmd[i + 1] for i, c in enumerate(cmd) if c == "-e"]

    def test_windows_paths_are_remapped_into_the_container(self):
        cmd = build_docker_run_command(
            "j", "echo hi", "img", self.WIN_DIR,
            usage_report_path=self.WIN_DIR + r"\gcon-usage-j.json",
            stage_report_path=self.WIN_DIR + r"\gcon-stages-j.jsonl",
        )
        env = self._env(cmd)
        assert f"GCON_USAGE_REPORT_PATH={CONTAINER_MOUNT}/gcon-usage-j.json" in env
        assert f"GCON_STAGE_REPORT_PATH={CONTAINER_MOUNT}/gcon-stages-j.jsonl" in env

    def test_the_windows_directory_is_passed_to_docker_unchanged(self):
        cmd = build_docker_run_command("j", "x", "img", self.WIN_DIR)
        assert _pair(cmd, "-v") == f"{self.WIN_DIR}:{CONTAINER_MOUNT}"

    def test_unc_and_mixed_separators(self):
        unc = build_docker_run_command(
            "j", "x", "img", r"\\srv\share\io", usage_report_path=r"\\srv\share\io\u.json")
        assert f"GCON_USAGE_REPORT_PATH={CONTAINER_MOUNT}/u.json" in self._env(unc)
        mixed = build_docker_run_command(
            "j", "x", "img", self.WIN_DIR, usage_report_path="C:/Users/me/AppData/Local/Temp/gcon-job-io-me/u.json")
        assert f"GCON_USAGE_REPORT_PATH={CONTAINER_MOUNT}/u.json" in self._env(mixed)

    def test_windows_drive_letter_case_does_not_matter(self):
        cmd = build_docker_run_command(
            "j", "x", "img", self.WIN_DIR, usage_report_path=self.WIN_DIR.lower() + r"\u.json")
        assert f"GCON_USAGE_REPORT_PATH={CONTAINER_MOUNT}/u.json" in self._env(cmd)

    def test_posix_paths_are_unchanged(self):
        cmd = build_docker_run_command("j", "x", "img", "/tmp/io", usage_report_path="/tmp/io/u.json")
        assert f"GCON_USAGE_REPORT_PATH={CONTAINER_MOUNT}/u.json" in self._env(cmd)

    @pytest.mark.parametrize("directory,report", [
        (WIN_DIR, r"C:\Users\me\AppData\Local\Temp\other\u.json"),       # a sibling directory
        (WIN_DIR, WIN_DIR + r"\..\escape.json"),                              # traversal
        (WIN_DIR, WIN_DIR),                                                     # the directory itself
        ("/tmp/io", "/tmp/io/../x.json"),
        ("/tmp/io", "/tmp/iox/u.json"),                                         # shares only a string prefix
        ("/tmp/io", "/tmp/io"),
    ])
    def test_paths_outside_the_directory_are_rejected(self, directory, report):
        with pytest.raises(ValueError, match="not under host_temp_dir"):
            build_docker_run_command("j", "x", "img", directory, usage_report_path=report)
