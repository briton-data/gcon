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
network isolation too (no network unless an operator opts in) -- a
meaningfully stronger boundary than a raw host subprocess. It is not
as strong as a microVM (Firecracker/gVisor) against a kernel-level
container-escape exploit. For where GCON is right now (opening up
early access, worker fleet fully operator-controlled, not yet
accepting untrusted third-party worker hardware), this is the right
tradeoff of real protection against realistic risk vs. operational
complexity -- not a claim that it's the strongest isolation that
exists.
"""
import hashlib
import logging
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import PurePosixPath, PureWindowsPath
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


def validate_network(network: str) -> str:
    """
    Job containers have NO network unless an operator chose one ("none" is
    the default). "host" is refused outright: it puts the container in the
    worker's own network namespace, so the job could reach every service on
    the host's localhost -- exactly what the sandbox exists to prevent.
    """
    if not network or not network.strip():
        return "none"
    network = network.strip()
    if network.lower() == "host":
        raise ValueError(
            "GCON_JOB_DOCKER_NETWORK=host is not allowed: it gives job containers the "
            "worker's own network namespace. Use 'none' (default) or a bridge network."
        )
    return network


# Directories holding this worker's own credentials (mTLS cert/key, attestation
# key). run_worker registers its cert dir at startup; GCON_TLS_CERT_DIR and the
# container default are always included.
_CREDENTIAL_DIRS: List[str] = []


def register_credential_dir(path: Optional[str]) -> None:
    if path and path not in _CREDENTIAL_DIRS:
        _CREDENTIAL_DIRS.append(path)


def _credential_dirs() -> List[str]:
    dirs = list(_CREDENTIAL_DIRS)
    dirs.append(os.environ.get("GCON_TLS_CERT_DIR") or "/etc/gcon/certs")
    return [d for d in dirs if d]


def assert_mount_excludes_credentials(host_dir: str) -> None:
    """
    The only host path a job container may see is the job IO directory. Refuse
    -- loudly, before any container starts -- if that directory is, contains or
    sits inside this worker's credential directory, so a misconfiguration (e.g.
    the cert dir pointed at the temp dir) can never hand the node's private
    keys to customer code.
    """
    real = os.path.normcase(os.path.realpath(host_dir))
    for cred in _credential_dirs():
        cred_real = os.path.normcase(os.path.realpath(cred))
        if (
            real == cred_real
            or real.startswith(cred_real + os.sep)
            or cred_real.startswith(real + os.sep)
        ):
            raise PermissionError(
                f"Refusing to mount {host_dir!r} into a job container: it overlaps the "
                f"worker's credential directory {cred!r}."
            )


def _current_user_id() -> str:
    """A stable per-OS-user token for naming the job IO directory."""
    if hasattr(os, "getuid"):
        return str(os.getuid())
    import getpass
    return _UNSAFE_NAME_CHARS.sub("-", getpass.getuser())


def job_io_dir() -> str:
    """
    The one directory a job's report files (usage / stage reports) live in
    -- and, for the docker backend, the ONLY host directory mounted into the
    container.

    This used to be the whole system temp dir (tempfile.gettempdir()), so a
    containerised job could read and modify everything else in /tmp: other
    software's files, other jobs' leftovers, anything a co-tenant process left
    there. A dedicated directory means the container sees only its own report
    files (the agent deletes them around every run).

    Per-OS-user name, mode 0700, and created here on first use. In a shared
    /tmp another user could pre-create the name to squat it, so an existing
    directory is only accepted if it is a real directory (not a symlink) owned
    by us.
    """
    # GCON_JOB_IO_ROOT: where the job IO directory lives. A worker that itself
    # runs inside a container and drives a Docker daemon outside it MUST set
    # this to a directory that exists at the SAME path on the daemon's side
    # (a shared volume), because `-v <path>:/gcon_io` is resolved by the
    # daemon, not by the worker. Unset: the system temp dir, as before.
    root = os.environ.get("GCON_JOB_IO_ROOT") or tempfile.gettempdir()
    path = os.path.join(root, f"gcon-job-io-{_current_user_id()}")
    os.makedirs(path, mode=0o700, exist_ok=True)
    if os.path.islink(path) or not os.path.isdir(path):
        raise PermissionError(f"{path!r} exists but is not a plain directory; refusing to use it for job IO.")
    if hasattr(os, "getuid") and os.stat(path).st_uid != os.getuid():
        raise PermissionError(f"{path!r} is owned by another user; refusing to use it for job IO.")
    return path


def new_run_dir() -> str:
    """
    A fresh, private (0700) directory for ONE run, created inside
    job_io_dir() and named uniquely every time.

    job_io_dir() itself is shared by every job a worker ever runs. Mounting
    it into the container let one job leave files (or read another job's
    report files) for the next -- state carried between jobs, which on a
    worker shared by several customers is a cross-tenant leak. Each run now
    mounts only its own directory; nothing is ever mounted twice.
    """
    return tempfile.mkdtemp(prefix="run-", dir=job_io_dir())


def remove_run_dir(path: Optional[str]) -> None:
    """
    Delete a run directory. Best effort: files a container running as root
    created in a subdirectory may be undeletable by the worker's own user.
    That is only host disk litter, never a leak: the name is unique, so no
    later job is ever given this directory.
    """
    if not path:
        return
    shutil.rmtree(path, ignore_errors=True)
    if os.path.exists(path):
        logger.warning("Could not fully remove job run directory %s; it will not be reused.", path)


def _safe_job_id_component(job_id: str) -> str:
    """
    job_id is an arbitrary caller-supplied string -- it can contain "/" or
    "..". Embedded verbatim in a file name it could steer where the agent
    creates, reads and deletes report files. Reduce it to filename-safe
    characters; if anything had to change, append a short hash of the
    original so two different ids can never map to the same file.
    """
    safe = _UNSAFE_NAME_CHARS.sub("-", job_id)
    if safe != job_id or not safe:
        digest = hashlib.sha256(job_id.encode("utf-8")).hexdigest()[:8]
        safe = f"{safe or 'job'}-{digest}"
    return safe


def job_io_path(kind: str, job_id: str, suffix: str) -> str:
    """Path of one of a job's report files, inside job_io_dir() and safe
    against any job_id. `kind` is "usage" or "stages"."""
    return os.path.join(job_io_dir(), f"gcon-{kind}-{_safe_job_id_component(job_id)}{suffix}")


def build_docker_run_command(
    job_id: str,
    job_script: str,
    image: str,
    host_temp_dir: str,
    usage_report_path: Optional[str] = None,
    stage_report_path: Optional[str] = None,
    memory_limit: Optional[str] = None,
    cpu_limit: Optional[str] = None,
    network: str = "none",
    gpus: Optional[str] = None,
    extra_env: Optional[Dict[str, str]] = None,
    pids_limit: Optional[str] = "512",
    user: Optional[str] = None,
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
    # Host paths use the HOST's conventions -- on a Windows worker they are
    # C:\Users\...\file.json with backslashes, which a plain string
    # prefix test against "<dir>/" never matches (docker mode raised on every
    # job there). pathlib's pure path classes compare them correctly for
    # either flavour, on any OS the code happens to run on.
    assert_mount_excludes_credentials(host_temp_dir)

    path_cls = PureWindowsPath if re.match(r"^(?:[A-Za-z]:|\\\\)", host_temp_dir) else PurePosixPath

    def _container_path(host_path: Optional[str], label: str) -> Optional[str]:
        if host_path is None:
            return None
        try:
            relative = path_cls(host_path).relative_to(path_cls(host_temp_dir))
            if not relative.parts or ".." in relative.parts:
                raise ValueError("not a file strictly inside the directory")
        except ValueError:
            raise ValueError(
                f"{label}={host_path!r} is not under host_temp_dir="
                f"{host_temp_dir!r} -- can't remap it into the container's "
                f"{CONTAINER_MOUNT} bind mount. This should never happen "
                f"with GCON's own callers (they always derive these paths "
                f"from tempfile.gettempdir()); if you're calling "
                f"execute_job() directly with a custom path, keep it under "
                f"the same temp dir the agent bind-mounts."
            ) from None
        return f"{CONTAINER_MOUNT}/{relative.as_posix()}"

    container_usage_path = _container_path(usage_report_path, "usage_report_path")
    container_stage_path = _container_path(stage_report_path, "stage_report_path")

    cmd = [
        "docker", "run", "--rm",
        "--name", container_name_for_job(job_id),
        "-v", f"{host_temp_dir}:{CONTAINER_MOUNT}",
        # Hardening that costs a compute job nothing: it gets no Linux
        # capabilities (a job that needs one -- chown, raw sockets, apt
        # installs -- must run on the subprocess backend instead) and can
        # never gain privileges through setuid binaries.
        "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
    ]
    if pids_limit:
        # Bounds a fork bomb to the container. "-1" means unlimited.
        cmd += ["--pids-limit", str(pids_limit)]
    if user:
        # Opt-in: run as a non-root uid[:gid] instead of the image default
        # (usually root). Not the default because the image's HOME and most
        # of its filesystem are then unwritable, which breaks jobs that
        # write outside /gcon_io or install packages.
        cmd += ["--user", user]
    if memory_limit:
        cmd += ["--memory", memory_limit]
    if cpu_limit:
        cmd += ["--cpus", cpu_limit]
    # Always explicit -- never left to Docker's default bridge, which gives a
    # job outbound internet and, on a cloud host, the instance-metadata
    # endpoint (169.254.169.254) and its credentials.
    cmd += ["--network", validate_network(network)]
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


_PROBE_SCRIPT = (
    "grep -E '^(CapEff|NoNewPrivs):' /proc/self/status; "
    "echo NETDEVS=$(ls /sys/class/net 2>/dev/null | tr '\\n' ' '); "
    "test -w " + CONTAINER_MOUNT + " && echo IO_WRITABLE=1; "
    "{creds}"
    "echo PROBE_DONE"
)


def verify_sandbox(
    image: str,
    network: str = "none",
    user: Optional[str] = None,
    memory_limit: Optional[str] = None,
    cpu_limit: Optional[str] = None,
    pids_limit: Optional[str] = "512",
    timeout_seconds: int = 120,
) -> Dict[str, Any]:
    """
    Proves, by running one throwaway container built by the SAME
    build_docker_run_command every job uses, that this worker's sandbox
    actually has the properties GCON promises -- before the worker is allowed
    to say it is sandboxed.

    Checked from inside the container: no Linux capabilities (CapEff == 0),
    no-new-privileges set, no network interface besides loopback when the
    network is "none", the job IO directory is writable, and none of this
    worker's credential directories are visible. Also fails if the Docker
    daemon is unreachable or the image cannot be run.

    Returns {"ok": bool, "failures": [...], "checks": {...}}; never raises.
    This is the worker verifying ITSELF. The coordinator separately checks the
    worker's signed per-job statement (GCONCoordinator._enforce_sandbox_evidence).
    """
    failures: List[str] = []
    checks: Dict[str, Any] = {}
    try:
        io_dir = job_io_dir()
        creds = "".join(f"test -e '{d}' && echo CRED_VISIBLE={d}; " for d in _credential_dirs())
        probe_job = f"gcon-sandbox-probe-{os.getpid()}"
        cmd = build_docker_run_command(
            job_id=probe_job,
            job_script=_PROBE_SCRIPT.replace("{creds}", creds),
            image=image, host_temp_dir=io_dir, network=network, user=user,
            memory_limit=memory_limit, cpu_limit=cpu_limit, pids_limit=pids_limit,
        )
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds, check=False)
    except subprocess.TimeoutExpired:
        return {"ok": False, "failures": ["probe container timed out"], "checks": checks}
    except Exception as e:  # docker CLI missing, mount refused, ...
        return {"ok": False, "failures": [f"could not run the probe container: {e}"], "checks": checks}

    out = proc.stdout or ""
    if proc.returncode != 0 or "PROBE_DONE" not in out:
        detail = (proc.stderr or out).strip().splitlines()[-1:] or ["no output"]
        return {"ok": False, "failures": [f"probe container failed (exit {proc.returncode}): {detail[0]}"], "checks": checks}

    values: Dict[str, str] = {}
    for line in out.splitlines():
        if line.startswith(("CapEff:", "NoNewPrivs:")):
            key, _, val = line.partition(":")
            values[key] = val.strip()
        elif line.startswith("NETDEVS="):
            values["NETDEVS"] = line.split("=", 1)[1].strip()
    checks["no_capabilities"] = values.get("CapEff", "").strip("0") == "" and "CapEff" in values
    checks["no_new_privileges"] = values.get("NoNewPrivs") == "1"
    devs = set(values.get("NETDEVS", "").split())
    checks["network_isolated"] = (devs <= {"lo"}) if network == "none" else True
    checks["io_dir_writable"] = "IO_WRITABLE=1" in out
    checks["credentials_not_visible"] = "CRED_VISIBLE=" not in out
    for name, ok in checks.items():
        if not ok:
            failures.append(f"sandbox check failed: {name}")
    return {"ok": not failures, "failures": failures, "checks": checks}


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
