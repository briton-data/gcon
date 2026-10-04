# Sandboxed workers

Who can run what is decided by **declared and verified execution capability**, not by who owns the machine.

| Worker | Sandboxed | Can receive customer (public API / organization) jobs? |
|---|---|---|
| GCON-managed worker | yes | yes |
| Customer worker with a supported sandbox | yes | yes (their own organization's jobs) |
| Customer worker, unsandboxed | no | **no** |
| Internal trusted worker | no | **internal jobs only**, and only with `GCON_SANDBOX_POLICY=trusted` on the coordinator |

(A job is still only dispatched to a worker of its own organization; sharing one pool across organizations is a separate decision.)

## How "sandboxed" is established

1. **The worker proves it to itself.** At startup, a worker with `GCON_EXECUTION_BACKEND=docker` runs a throwaway container built the same way every job's is, and checks from inside it: no Linux capabilities, `no-new-privileges`, no network interface but loopback (when the network is `none`), a writable job IO directory, and none of the worker's credential directories visible. If any check fails, or Docker can't be reached, the worker **exits** instead of starting.
2. **It declares it.** Only after the probe passes does it register with `sandbox=docker` and `sandbox_verified=1`. A worker that sends only the first (an older build) counts as unsandboxed.
3. **The coordinator checks the receipt of every public job.** The worker signs, per job, which backend it used. If a job required a sandbox and the result is missing that signed statement, doesn't verify against the node's registered key, is about another job/node, or says anything other than `docker`, the result is rejected, the job fails, and the node is **quarantined** until staff clear it.

Limit: a worker that lies inside its own signature can't be caught by this. It makes the lie attributable to that node's key; it can't prove what a stranger's machine really ran.

## The worker image

`docker/Dockerfile` ships the Docker CLI and defaults to the docker backend. It does not contain a daemon: point it at one with `DOCKER_HOST`. `docker/docker-compose.worker.yml` runs a private `docker:dind` daemon next to it.

- `GCON_JOB_IO_ROOT` (default `/var/lib/gcon/jobio` in the image) must be a volume shared **at the same path** with the daemon, because `-v <path>:/gcon_io` is resolved by the daemon.
- The worker's cert/key directory is never mounted into job containers. The executor refuses to start a job if the job IO directory overlaps it.
- Don't mount the host's `/var/run/docker.sock` into the worker unless you accept that a compromised worker then controls the host. The compose file avoids that by giving the worker its own daemon.
- Prefer a rootless Docker daemon, and block the cloud metadata address (`169.254.169.254`) on the host.

## Operator reference (workers and coordinator)

Everything below is for whoever runs workers or a coordinator. Clients and API users never need it: from the API's side, a job simply runs sandboxed.

### Turning sandboxing on

Set `GCON_EXECUTION_BACKEND=docker` in the worker's environment (Docker must be installed where the worker runs, or use the worker image above).

- **Network.** Job containers have **no network by default**. A job that needs the internet (e.g. downloading a dataset) needs `GCON_JOB_DOCKER_NETWORK=bridge`. On a cloud host, also block the instance-metadata endpoint (`iptables -I DOCKER-USER -d 169.254.169.254 -j DROP`), which otherwise hands the host's cloud credentials to job code. `host` networking is refused.
- **Container hardening.** Job containers run with no Linux capabilities, no privilege escalation and a 512-process cap (`GCON_JOB_DOCKER_PIDS_LIMIT`, `-1` for unlimited), and see only their own report-file directory rather than the host's `/tmp`. They run as the image's default user; set `GCON_JOB_DOCKER_USER=1000:1000` to run as non-root if your image and job allow it.
- **Setting variables.** `export NAME=value` on Linux and macOS, `$env:NAME="value"` in PowerShell.

### Coordinator policy: `GCON_SANDBOX_POLICY`

- `required` (default): jobs go only to sandboxed workers; an unsandboxed worker gets none.
- `trusted`: unsandboxed workers may receive **internal** jobs, meaning jobs submitted by the operator's own code. A job submitted through the public API, or belonging to an organization, is never dispatched to an unsandboxed worker under either setting.

### A worker that must run unsandboxed

Only for internal infrastructure, and only with `GCON_SANDBOX_POLICY=trusted`. Start it as root on Linux/macOS and set `GCON_JOB_RUN_AS_USER=<an unprivileged account>`: jobs then run as that user and cannot read the worker's private keys. That protects the worker's identity only; it is not a sandbox.

### Coordinator and key hygiene

- Never expose the coordinator directly: put it behind TLS (`GCON_FORCE_HTTPS=1` or a reverse proxy).
- Encrypt the disk that holds the coordinator's `data/` folder.
- Keep certificates and signing keys private (file permissions `0600`).

See [DEPLOYMENT.md](DEPLOYMENT.md) and [SECURITY.md](../SECURITY.md) for the full detail.
