# The Docker CLI, copied from the official image (a static binary). The worker
# only needs the CLI: it talks to a Docker daemon over DOCKER_HOST and runs every
# customer job in a container THERE. See docs/WORKER_SANDBOX.md.
FROM docker:27-cli AS docker-cli

FROM python:3.12-slim

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

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY scripts/run_worker.py scripts/run_worker.py
COPY docker/entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
