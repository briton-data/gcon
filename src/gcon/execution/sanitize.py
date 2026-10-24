"""
Clean a GCON-hosted worker between customers.

A hosted worker is leased to one customer at a time (see gcon.cluster.worker_leases). When a lease ends the
coordinator asks the worker to clean itself, and the worker goes back to the pool only if it says it did. This
module is what "clean" means on the worker. It removes everything a customer's jobs could have left on the machine:

1. Containers. Every job runs in a container named ``gcon-job-<job id>``. They are started with --rm, so a normal
   run leaves none behind; a crash or a killed worker can. Any left over are force-removed (a still-running one is
   a customer process on the machine).
2. Files. The job IO directory (``job_io_dir()``) holds each run's directory and report files. All of it is removed.
   A container that ran as root can leave files the worker's own user cannot delete; those are removed by a
   short-lived helper container that mounts only that directory and has no network.

It then CHECKS: no job container remains and the IO directory is empty. If either is untrue it says so and the
coordinator keeps the worker out of the pool. A success is never reported on trust.

What it deliberately does not do: touch images (the image is the operator's, never a customer's), touch anything
outside the job IO directory, or run while a job is running (it refuses).
"""
import logging
import os
import shutil
import subprocess
from typing import List, Optional, Tuple

from gcon.execution.docker_executor import job_io_dir

logger = logging.getLogger(__name__)

JOB_CONTAINER_PREFIX = "gcon-job-"


def _docker(args: List[str], timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout, check=False)


def list_job_containers() -> List[str]:
    """Names of every job container on this host, running or not. Raises RuntimeError if Docker cannot be asked:
    a worker that cannot tell what is on it cannot say it is clean."""
    try:
        out = _docker(["ps", "-a", "--format", "{{.Names}}"], timeout=30)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not ask Docker what is running: {exc!r}") from None
    if out.returncode != 0:
        raise RuntimeError(f"docker ps failed: {(out.stderr or '').strip()[:200]}")
    return [n for n in out.stdout.split() if n.startswith(JOB_CONTAINER_PREFIX)]


def _remove_dir_contents(path: str) -> None:
    for entry in os.scandir(path):
        try:
            if entry.is_dir(follow_symlinks=False):
                shutil.rmtree(entry.path, ignore_errors=True)
            else:
                os.unlink(entry.path)
        except OSError:
            pass


def _remove_as_container_root(path: str, image: str) -> None:
    """Delete files a container running as root left behind. The helper mounts ONLY `path`, has no network, and
    exists for this one command. It needs the default capabilities: root without them cannot enter a 0700
    directory owned by another uid."""
    _docker(["run", "--rm", "--network", "none", "--security-opt", "no-new-privileges",
             "-v", f"{path}:/gcon_clean", image, "sh", "-c",
             "find /gcon_clean -mindepth 1 -delete"], timeout=120)


def sanitize_host(docker_image: Optional[str] = None, running_jobs: int = 0) -> Tuple[bool, str]:
    """Returns (ok, detail). ok is True only when verified clean."""
    if running_jobs:
        return False, f"{running_jobs} job(s) still running; cleaning refused"
    problems: List[str] = []

    # 1. containers
    try:
        names = list_job_containers()
        for name in names:
            _docker(["rm", "-f", name], timeout=60)
        left = list_job_containers()
        if left:
            problems.append(f"{len(left)} job container(s) could not be removed")
    except RuntimeError as exc:
        problems.append(str(exc))

    # 2. files
    try:
        io_dir = job_io_dir()
        _remove_dir_contents(io_dir)
        if os.listdir(io_dir) and docker_image:
            try:
                _remove_as_container_root(io_dir, docker_image)
            except (OSError, subprocess.TimeoutExpired) as exc:
                problems.append(f"the helper that removes root-owned files failed: {exc!r}")
        remaining = os.listdir(io_dir)
        if remaining:
            problems.append(f"{len(remaining)} item(s) remain in the job directory")
    except (OSError, PermissionError) as exc:
        problems.append(f"the job directory could not be cleaned: {exc!r}")

    if problems:
        logger.error("Host clean-up incomplete: %s", "; ".join(problems))
        return False, "; ".join(problems)
    return True, "removed leftover job containers and files; verified"
