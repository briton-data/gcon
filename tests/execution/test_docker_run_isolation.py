"""Every docker run gets its own private directory.

The container used to mount one host directory shared by every job a worker
ever ran, so a file one job left in /gcon_io was visible to the next -- state
carried between jobs. On a worker shared by several customers that is a
cross-tenant leak. There is no Docker daemon in the test environment, so the
command builder is replaced with a local command that acts on whatever
directory WOULD have been mounted; everything else is the real agent path.
"""
import json
import os
import sys

import pytest

from gcon.execution import docker_executor
from gcon.execution.agent import GCONAgent


@pytest.fixture
def agent(monkeypatch, tmp_path):
    monkeypatch.setenv("GCON_EXECUTION_BACKEND", "docker")
    monkeypatch.setenv("GCON_JOB_IO_ROOT", str(tmp_path))
    monkeypatch.delenv("GCON_JOB_RUN_AS_USER", raising=False)
    mounts = []

    def fake_builder(job_id, job_script, image, host_temp_dir, usage_report_path=None,
                     stage_report_path=None, **_kw):
        mounts.append({"dir": host_temp_dir, "usage": usage_report_path, "stages": stage_report_path})
        code = (
            "import os, json, sys\n"
            f"d = {host_temp_dir!r}\n"
            f"usage = {usage_report_path!r}\n"
            f"mode = {job_script!r}\n"
            "if mode == 'write':\n"
            "    open(os.path.join(d, 'leak.txt'), 'w').write('customer A secret')\n"
            "    os.makedirs(os.path.join(d, 'sub'), exist_ok=True)\n"
            "    open(os.path.join(d, 'sub', 'more.txt'), 'w').write('x')\n"
            "    if usage: json.dump({'gpu_seconds': 3}, open(usage, 'w'))\n"
            "else:\n"
            "    print('SEEN=' + json.dumps(sorted(os.listdir(d))))\n"
        )
        return [sys.executable, "-c", code]

    monkeypatch.setattr(docker_executor, "build_docker_run_command", fake_builder)
    a = GCONAgent("pool-node")
    a._mounts = mounts
    a._base = str(tmp_path)
    return a


def test_a_file_one_job_leaves_is_not_visible_to_the_next(agent):
    first = agent.execute_job("job-a", "write", timeout=20)
    second = agent.execute_job("job-b", "list", timeout=20)
    assert first["status"] == "success" and second["status"] == "success"
    assert "SEEN=[]" in second["stdout"], second["stdout"]
    assert agent._mounts[0]["dir"] != agent._mounts[1]["dir"]


def test_the_shared_job_io_directory_is_never_the_mount(agent):
    agent.execute_job("job-a", "list", timeout=20)
    mounted = agent._mounts[0]["dir"]
    assert os.path.realpath(mounted) != os.path.realpath(docker_executor.job_io_dir())
    assert os.path.realpath(mounted).startswith(os.path.realpath(docker_executor.job_io_dir()) + os.sep)


def test_the_run_directory_is_deleted_after_the_job(agent):
    agent.execute_job("job-a", "write", timeout=20)
    assert not os.path.exists(agent._mounts[0]["dir"])
    assert os.listdir(docker_executor.job_io_dir()) == []


def test_the_run_directory_is_deleted_even_when_the_job_fails(agent, monkeypatch):
    seen = []

    def failing_builder(job_id, job_script, image, host_temp_dir, **_kw):
        seen.append(host_temp_dir)
        return [sys.executable, "-c", "import sys; sys.exit(3)"]

    monkeypatch.setattr(docker_executor, "build_docker_run_command", failing_builder)
    result = agent.execute_job("job-x", "anything", timeout=20)
    assert result["status"] != "success"
    assert seen and not os.path.exists(seen[0])


def test_usage_reports_still_reach_the_result(agent):
    usage = docker_executor.job_io_path("usage", "job-a", ".json")
    result = agent.execute_job("job-a", "write", timeout=20, usage_report_path=usage)
    assert result["usage"] == {"gpu_seconds": 3}
    # the report was written inside the private run directory, not the shared one
    assert os.path.dirname(agent._mounts[0]["usage"]) == agent._mounts[0]["dir"]


def test_the_subprocess_backend_is_untouched(monkeypatch, tmp_path):
    monkeypatch.delenv("GCON_EXECUTION_BACKEND", raising=False)
    monkeypatch.setenv("GCON_JOB_IO_ROOT", str(tmp_path))
    result = GCONAgent("plain").execute_job("j", "echo hi", timeout=20)
    assert result["status"] == "success" and "hi" in result["stdout"]
    assert not os.path.exists(os.path.join(str(tmp_path), "run-"))
