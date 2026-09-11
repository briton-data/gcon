"""
Unit tests for gcon.execution.docker_executor -- the command-building
and container-control functions used by the opt-in Docker execution
backend (see docker_executor.py's module docstring and agent.py's
GCON_EXECUTION_BACKEND).

These test command CONSTRUCTION and control-call CORRECTNESS without
needing a real Docker daemon (subprocess.run is mocked for
stop_container/kill_container) -- this sandbox has no Docker
installed, so these are the tests that can run here. A real `docker
run` end-to-end pass (actual container execution, actual bind-mount
read-back) still needs to be run once against a host that has Docker,
before trusting this in production -- flagged explicitly, not
papered over.
"""
from unittest.mock import patch, MagicMock

import pytest

from gcon.execution.docker_executor import (
    CONTAINER_MOUNT,
    build_docker_run_command,
    container_name_for_job,
    kill_container,
    stop_container,
)


class TestContainerNameForJob:
    def test_simple_job_id_gets_prefixed(self):
        assert container_name_for_job("job-42") == "gcon-job-job-42"

    def test_unsafe_characters_are_sanitized(self):
        name = container_name_for_job("job/with spaces:and:colons")
        assert name.startswith("gcon-job-")
        # Docker container names only allow [a-zA-Z0-9_.-]
        import re
        assert re.match(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$", name)

    def test_same_job_id_always_produces_same_name(self):
        """Deterministic naming is what lets stop_container/kill_container
        find the right container by job_id alone, without a separate
        name registry."""
        assert container_name_for_job("job-abc") == container_name_for_job("job-abc")

    def test_empty_job_id_still_produces_a_valid_name(self):
        name = container_name_for_job("")
        import re
        assert re.match(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$", name)


class TestBuildDockerRunCommand:
    def test_basic_command_structure(self):
        cmd = build_docker_run_command(
            job_id="job-1", job_script="echo hi", image="python:3.12-slim",
            host_temp_dir="/tmp",
        )
        assert cmd[:3] == ["docker", "run", "--rm"]
        assert "--name" in cmd
        assert cmd[cmd.index("--name") + 1] == "gcon-job-job-1"
        assert cmd[-3:] == ["python:3.12-slim", "sh", "-c"] or cmd[-4:] == ["python:3.12-slim", "sh", "-c", "echo hi"]
        assert cmd[-1] == "echo hi"
        assert "-v" in cmd
        mount_arg = cmd[cmd.index("-v") + 1]
        assert mount_arg == f"/tmp:{CONTAINER_MOUNT}"

    def test_resource_limits_are_included_when_given(self):
        cmd = build_docker_run_command(
            job_id="job-1", job_script="echo hi", image="python:3.12-slim",
            host_temp_dir="/tmp", memory_limit="2g", cpu_limit="1.5",
        )
        assert "--memory" in cmd
        assert cmd[cmd.index("--memory") + 1] == "2g"
        assert "--cpus" in cmd
        assert cmd[cmd.index("--cpus") + 1] == "1.5"

    def test_resource_limits_omitted_when_not_given(self):
        cmd = build_docker_run_command(
            job_id="job-1", job_script="echo hi", image="python:3.12-slim",
            host_temp_dir="/tmp",
        )
        assert "--memory" not in cmd
        assert "--cpus" not in cmd

    def test_network_and_gpus_flags(self):
        cmd = build_docker_run_command(
            job_id="job-1", job_script="echo hi", image="python:3.12-slim",
            host_temp_dir="/tmp", network="none", gpus="all",
        )
        assert cmd[cmd.index("--network") + 1] == "none"
        assert cmd[cmd.index("--gpus") + 1] == "all"

    def test_usage_and_stage_report_paths_remapped_into_container(self):
        cmd = build_docker_run_command(
            job_id="job-42", job_script="echo hi", image="python:3.12-slim",
            host_temp_dir="/tmp",
            usage_report_path="/tmp/gcon-usage-job-42.json",
            stage_report_path="/tmp/gcon-stages-job-42.jsonl",
        )
        env_flags = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-e"]
        assert f"GCON_USAGE_REPORT_PATH={CONTAINER_MOUNT}/gcon-usage-job-42.json" in env_flags
        assert f"GCON_STAGE_REPORT_PATH={CONTAINER_MOUNT}/gcon-stages-job-42.jsonl" in env_flags

    def test_report_path_outside_host_temp_dir_raises(self):
        """A report path the caller derived from somewhere other than
        the bind-mounted host_temp_dir can't be remapped into the
        container at all -- this must fail loudly, not silently drop
        usage/stage reporting."""
        with pytest.raises(ValueError, match="host_temp_dir"):
            build_docker_run_command(
                job_id="job-1", job_script="echo hi", image="python:3.12-slim",
                host_temp_dir="/tmp",
                usage_report_path="/some/other/dir/usage.json",
            )

    def test_extra_env_vars_are_passed_through(self):
        cmd = build_docker_run_command(
            job_id="job-1", job_script="echo hi", image="python:3.12-slim",
            host_temp_dir="/tmp", extra_env={"FOO": "bar"},
        )
        env_flags = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-e"]
        assert "FOO=bar" in env_flags

    def test_no_usage_or_stage_path_means_no_env_flags_for_them(self):
        cmd = build_docker_run_command(
            job_id="job-1", job_script="echo hi", image="python:3.12-slim",
            host_temp_dir="/tmp",
        )
        env_flags = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "-e"]
        assert not any("GCON_USAGE_REPORT_PATH" in f for f in env_flags)
        assert not any("GCON_STAGE_REPORT_PATH" in f for f in env_flags)

    def test_shell_command_stays_one_argv_element_not_re_split(self):
        """The job's shell command (with &&, |, etc.) must reach `sh
        -c` as ONE argv element, matching agent.py's existing
        documented behavior for the non-docker path -- re-splitting it
        on the host would silently break shell operators."""
        cmd = build_docker_run_command(
            job_id="job-1", job_script="echo a && echo b | grep a",
            image="python:3.12-slim", host_temp_dir="/tmp",
        )
        assert cmd[-1] == "echo a && echo b | grep a"


class TestStopAndKillContainer:
    """subprocess.run mocked -- verifies the exact docker command
    issued, not real container control (no Docker daemon in this
    sandbox)."""

    def test_stop_container_calls_docker_stop_with_correct_name(self):
        with patch("gcon.execution.docker_executor.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = stop_container("job-42", timeout_seconds=7)
            assert result is True
            args = mock_run.call_args[0][0]
            assert args[:2] == ["docker", "stop"]
            assert "--time" in args
            assert args[args.index("--time") + 1] == "7"
            assert args[-1] == "gcon-job-job-42"

    def test_kill_container_calls_docker_kill_with_correct_name(self):
        with patch("gcon.execution.docker_executor.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = kill_container("job-42")
            assert result is True
            args = mock_run.call_args[0][0]
            assert args == ["docker", "kill", "gcon-job-job-42"]

    def test_stop_container_returns_false_on_docker_cli_error(self):
        with patch("gcon.execution.docker_executor.subprocess.run") as mock_run:
            mock_run.side_effect = OSError("docker: command not found")
            assert stop_container("job-1") is False

    def test_stop_container_returns_false_on_timeout(self):
        import subprocess as real_subprocess
        with patch("gcon.execution.docker_executor.subprocess.run") as mock_run:
            mock_run.side_effect = real_subprocess.TimeoutExpired(cmd="docker stop", timeout=5)
            assert stop_container("job-1") is False
