"""
Docker-backed job isolation.

GCONAgent.execute_job() has always run a job's command as a raw host
subprocess (subprocess.Popen), with the agent's own privileges and
full filesystem access -- no sandboxing at all. That's a real
multi-tenant risk on any worker that ever runs more than one
customer's jobs (see agent.py's module docstring and ROADMAP.md
Section 3). This module is the opt-in Docker-backed alternative:
run the job's command inside a container instead of directly on the
host.

Deliberately scoped as pure, host-independent command-building and
container-control functions -- no subprocess execution of the actual
job happens in this file. agent.py's execute_job() still owns the
subprocess.Popen()/communicate()/timeout/kill orchestration exactly
as before; the only thing this module changes is WHAT gets run
(`docker run ...` instead of the job's raw command) and how the
running container gets stopped. This keeps every already-tested piece
of execute_job()'s behavior (stdout/stderr capture, timeout handling,
process-group kill semantics, the stage-report-polling thread, GPU
sampling) working completely unchanged for jobs run this way -- only
the command list itself and the kill path differ.

Backward compatible by default: nothing in agent.py calls into this
module unless GCON_EXECUTION_BACKEND=docker is set (see agent.py's
__init__). subprocess execution stays the default so existing local
dev setups, tests, and any worker without a Docker daemon keep working
exactly as before.

Known gap, stated plainly rather than implied: this gives real
process/filesystem isolation (container namespaces + cgroups) and,
with a network restriction set, network isolation too -- a
meaningfully stronger boundary than a raw host subprocess. It is not
as strong as a microVM (Firecracker/gVisor) against a kernel-level
container-escape exploit. For where GCON is right now (opening up
early access, worker fleet fully operator-controlled, not yet
accepting untrusted third-party worker hardware), this is the right
tradeoff of real protection against realistic risk vs. operational
complexity -- not a claim that it's the strongest isolation that
exists.
"""
import logging
import re
import subprocess
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

CONTAINER_MOUNT = "/gcon_io"

# Docker container names must match [a-zA-Z0-9][a-zA-Z0-9_.-]*.
# job_id has no such constraint anywhere else in GCON (it's just a
# caller-supplied string key), so this sanitizes rather than assumes.
_UNSAFE_NAME_CHARS = re.compile(r"[^a-zA-Z0-9_.-]")


def container_name_for_job(job_id: str) -> str:
    """
    Deterministic, sanitized container name for a job -- same job_id
    always produces the same name, which is what lets cancel()/
    timeout-handling issue `docker stop <name>` without having to
    separately track the name anywhere beyond what they already have
    (the job_id they were called with).
    """
    safe = _UNSAFE_NAME_CHARS.sub("-", job_id)
    if not safe or not (safe[0].isalnum()):
        safe = f"j-{safe}" if safe else "job"
    return f"gcon-job-{safe}"


def build_docker_run_command(
    job_id: str,
    job_script: str,
    image: str,
    host_temp_dir: str,
    usage_report_path: Optional[str] = None,
    stage_report_path: Optional[str] = None,
    memory_limit: Optional[str] = None,
    cpu_limit: Optional[str] = None,
    network: Optional[str] = None,
    gpus: Optional[str] = None,
    extra_env: Optional[Dict[str, str]] = None,
) -> List[str]:
    """
    Builds the full `docker run ...` argv list for one job. Returns a
    plain list (not a shell string) -- agent.py passes this straight
    to subprocess.Popen with shell=False, the same way it already
    handles the .py-file case, so there's no second layer of shell
    quoting to get wrong on top of the job's own command.

    The job's actual command is still run through `sh -c` INSIDE the
    container, not split into argv on the host -- this preserves the
    existing, already-documented behavior that JobSubmitRequest.command
    is a real shell command (supports &&, |, ;, etc.), just moved one
    layer in.

    usage_report_path/stage_report_path (see agent.py's execute_job
    docstring) are expected to be absolute paths under host_temp_dir --
    that's the only directory this function bind-mounts into the
    container (as CONTAINER_MOUNT), so the container-side env vars are
    computed by re-rooting those paths under CONTAINER_MOUNT rather
    than mounting each file individually. A path outside host_temp_dir
    is a caller bug (today's only callers -- local_transport.py/
    agent_daemon.py -- always derive these paths from
    tempfile.gettempdir(), i.e. host_temp_dir itself), so this raises
    rather than silently mounting nothing and leaving a job's usage/
    stage reporting mysteriously broken.
    """
    if not host_temp_dir.endswith("/"):
        host_temp_dir_norm = host_temp_dir + "/"
    else:
        host_temp_dir_norm = host_temp_dir

    def _container_path(host_path: Optional[str], label: str) -> Optional[str]:
        if host_path is None:
            return None
        if not host_path.startswith(host_temp_dir_norm):
            raise ValueError(
                f"{label}={host_path!r} is not under host_temp_dir="
                f"{host_temp_dir!r} -- can't remap it into the container's "
                f"{CONTAINER_MOUNT} bind mount. This should never happen "
                f"with GCON's own callers (they always derive these paths "
                f"from tempfile.gettempdir()); if you're calling "
                f"execute_job() directly with a custom path, keep it under "
                f"the same temp dir the agent bind-mounts."
            )
        relative = host_path[len(host_temp_dir_norm):]
        return f"{CONTAINER_MOUNT}/{relative}"

    container_usage_path = _container_path(usage_report_path, "usage_report_path")
    container_stage_path = _container_path(stage_report_path, "stage_report_path")

    cmd = [
        "docker", "run", "--rm",
        "--name", container_name_for_job(job_id),
        "-v", f"{host_temp_dir}:{CONTAINER_MOUNT}",
    ]
    if memory_limit:
        cmd += ["--memory", memory_limit]
    if cpu_limit:
        cmd += ["--cpus", cpu_limit]
    if network:
        cmd += ["--network", network]
    if gpus:
        cmd += ["--gpus", gpus]
    if container_usage_path:
        cmd += ["-e", f"GCON_USAGE_REPORT_PATH={container_usage_path}"]
    if container_stage_path:
        cmd += ["-e", f"GCON_STAGE_REPORT_PATH={container_stage_path}"]
    for key, value in (extra_env or {}).items():
        cmd += ["-e", f"{key}={value}"]

    cmd += [image, "sh", "-c", job_script]
    return cmd


def stop_container(job_id: str, timeout_seconds: int = 5) -> bool:
    """
    `docker stop` (graceful, sends SIGTERM then SIGKILL after
    `timeout_seconds`) the container for this job, by the same
    deterministic name build_docker_run_command's `--name` used.

    Deliberately NOT relying on killing the local `docker run` CLI
    process to stop the container: `docker run` in the foreground
    forwards SIGTERM to the container, but a hard SIGKILL of the CLI
    process (which is what subprocess.Process.kill() sends) can't be
    forwarded -- the CLI dies instantly and the container is orphaned,
    still running, unless something explicitly stops it by name. This
    is that explicit stop.

    Returns True if `docker stop` itself ran without error (the
    container may already have exited on its own, e.g. the job
    finished normally right before a timeout would have fired --
    `docker stop` on an already-stopped/removed container is a
    harmless no-op, not an error worth surfacing).
    """
    name = container_name_for_job(job_id)
    try:
        subprocess.run(
            ["docker", "stop", "--time", str(timeout_seconds), name],
            capture_output=True, timeout=timeout_seconds + 10, check=False,
        )
        return True
    except (subprocess.TimeoutExpired, OSError) as e:
        logger.error(f"docker stop failed for container {name}: {e}")
        return False


def kill_container(job_id: str) -> bool:
    """Immediate `docker kill` (SIGKILL, no grace period) -- used from
    the same place agent.py's non-docker path already sends a hard
    kill (cancel()'s force path, TimeoutExpired), so the two backends'
    "kill it now" semantics stay equivalent rather than docker mode
    quietly becoming slower to actually stop a job."""
    name = container_name_for_job(job_id)
    try:
        subprocess.run(
            ["docker", "kill", name], capture_output=True, timeout=15, check=False,
        )
        return True
    except (subprocess.TimeoutExpired, OSError) as e:
        logger.error(f"docker kill failed for container {name}: {e}")
        return False
