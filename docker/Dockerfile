# The Docker CLI, copied from the official image (a static binary). The worker
# only needs the CLI: it talks to a Docker daemon over DOCKER_HOST and runs every
# customer job in a container THERE. See docs/WORKER_SANDBOX.md.
FROM docker:27.5.1-cli AS docker-cli

FROM python:3.12.11-slim

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

# This image is a SANDBOXED worker: jobs run in containers, never as raw
# subprocesses next to the node's keys. run_worker verifies that at startup
# (a probe container) and exits if it cannot -- it will not quietly fall back.
ENV GCON_EXECUTION_BACKEND=docker
# `-v <path>:/gcon_io` is resolved by the Docker DAEMON, not by this container,
# so this directory must be a volume shared at the SAME path with the daemon.
ENV GCON_JOB_IO_ROOT=/var/lib/gcon/jobio
RUN mkdir -p /var/lib/gcon/jobio

WORKDIR /app

# Worker-only dependencies (not requirements.txt, which also holds the coordinator,
# the ML demo jobs and the test tools a worker never uses).
COPY requirements-worker.txt .
RUN pip install --no-cache-dir -r requirements-worker.txt

COPY src/ src/
COPY scripts/run_worker.py scripts/run_worker.py
COPY docker/entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh

# Proves the container's own status listener (started by entrypoint.sh) is answering.
# The worker itself is PID 1, so if it exits the container exits; this check does NOT
# detect a worker that is running but stuck -- that needs a heartbeat from the worker.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s' % os.environ.get('PORT','8080'), timeout=3).read()"

# Runs as root on purpose, for now. The job-IO directory is created by the worker and
# handed to job containers that run with every capability dropped, so a non-root worker
# also needs the job containers run as the same uid and the shared volume owned by it
# (see docs/BUILD_LOG.md). That has to be tested against a real Docker daemon first.
ENTRYPOINT ["./entrypoint.sh"]
