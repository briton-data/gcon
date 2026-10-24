"""
What "clean" means on a hosted worker: no job container left, the job directory empty, and a failure to verify
either is reported as a failure -- never as clean.
"""
import os
import subprocess

import pytest

from gcon.execution import sanitize
from gcon.execution.docker_executor import job_io_dir


class FakeDocker:
    """Stands in for the docker CLI. `containers` are the names `docker ps -a` lists."""

    def __init__(self, containers=(), ps_fails=False, rm_works=True):
        self.containers = list(containers)
        self.ps_fails = ps_fails
        self.rm_works = rm_works
        self.calls = []

    def __call__(self, args, timeout=60):
        self.calls.append(args)
        if args[0] == "ps":
            if self.ps_fails:
                return subprocess.CompletedProcess(args, 1, "", "Cannot connect to the Docker daemon")
            return subprocess.CompletedProcess(args, 0, "\n".join(self.containers) + "\n", "")
        if args[0] == "rm":
            if self.rm_works:
                self.containers = [c for c in self.containers if c != args[-1]]
            return subprocess.CompletedProcess(args, 0, "", "")
        return subprocess.CompletedProcess(args, 0, "", "")


@pytest.fixture
def io_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_JOB_IO_ROOT", str(tmp_path))
    return job_io_dir()


def _litter(io_dir):
    run = os.path.join(io_dir, "run-abc123")
    os.makedirs(os.path.join(run, "sub"))
    open(os.path.join(run, "sub", "customer-data.csv"), "w").write("secret")
    open(os.path.join(io_dir, "gcon-usage-job1.json"), "w").write("{}")
    open(os.path.join(io_dir, "gcon-stages-job1.jsonl"), "w").write("{}")


def test_it_removes_leftover_job_containers_and_files_and_verifies(io_dir, monkeypatch):
    docker = FakeDocker(["gcon-job-a", "unrelated-db", "gcon-job-b"])
    monkeypatch.setattr(sanitize, "_docker", docker)
    _litter(io_dir)
    ok, detail = sanitize.sanitize_host(docker_image="python:3.12-slim")
    assert ok, detail
    assert os.listdir(io_dir) == []
    assert docker.containers == ["unrelated-db"]                 # only GCON's own job containers are touched
    assert ["rm", "-f", "unrelated-db"] not in docker.calls


def test_a_machine_that_is_already_clean_says_so(io_dir, monkeypatch):
    monkeypatch.setattr(sanitize, "_docker", FakeDocker())
    assert sanitize.sanitize_host()[0] is True


def test_a_worker_that_cannot_ask_docker_cannot_say_it_is_clean(io_dir, monkeypatch):
    monkeypatch.setattr(sanitize, "_docker", FakeDocker(ps_fails=True))
    ok, detail = sanitize.sanitize_host()
    assert ok is False and "docker ps failed" in detail


def test_a_container_that_will_not_go_is_a_failure(io_dir, monkeypatch):
    monkeypatch.setattr(sanitize, "_docker", FakeDocker(["gcon-job-stuck"], rm_works=False))
    ok, detail = sanitize.sanitize_host()
    assert ok is False and "could not be removed" in detail


def test_files_the_worker_cannot_delete_are_removed_by_the_helper_container(io_dir, monkeypatch):
    class Docker(FakeDocker):
        def __call__(self, args, timeout=60):
            if args[0] == "run":                                  # the helper does what root in a container can
                self.calls.append(args)
                for entry in os.scandir(io_dir):
                    os.unlink(entry.path)
                return subprocess.CompletedProcess(args, 0, "", "")
            return super().__call__(args, timeout)

    docker = Docker()
    monkeypatch.setattr(sanitize, "_docker", docker)
    monkeypatch.setattr(sanitize, "_remove_dir_contents", lambda path: None)     # the worker's own user cannot
    open(os.path.join(io_dir, "root-owned.bin"), "w").write("x")
    ok, detail = sanitize.sanitize_host(docker_image="img")
    helper = next(c for c in docker.calls if c[0] == "run")
    assert ok, detail
    assert "--network" in helper and helper[helper.index("--network") + 1] == "none"
    assert f"{io_dir}:/gcon_clean" in helper                       # it mounts only the job directory


def test_files_nothing_can_delete_are_a_failure(io_dir, monkeypatch):
    monkeypatch.setattr(sanitize, "_docker", FakeDocker())
    monkeypatch.setattr(sanitize, "_remove_dir_contents", lambda path: None)
    open(os.path.join(io_dir, "stuck.bin"), "w").write("x")
    ok, detail = sanitize.sanitize_host(docker_image=None)
    assert ok is False and "remain" in detail


def test_it_refuses_to_clean_under_a_running_job(io_dir, monkeypatch):
    docker = FakeDocker(["gcon-job-live"])
    monkeypatch.setattr(sanitize, "_docker", docker)
    ok, detail = sanitize.sanitize_host(running_jobs=1)
    assert ok is False and "still running" in detail and docker.calls == []
