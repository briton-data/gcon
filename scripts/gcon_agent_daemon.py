#!/usr/bin/env python3
"""
Entry point for running a GCON Agent Daemon on a worker machine.

Usage:
    python scripts/gcon_agent_daemon.py \
        --node-id worker-01 \
        --coordinator coordinator.internal:50051 \
        --cert-dir /etc/gcon/certs

The certificate directory must contain (or be able to derive, via the
shared CA, from) a certificate for this exact --node-id -- see
`scripts/generate_dev_certs.py` for provisioning a development
cluster's CA and per-node certificates ahead of time.
"""

import argparse
import logging
import signal
import sys

sys.path.insert(0, "src")

from gcon.execution.agent import GCONAgent
from gcon.transport.agent_daemon import AgentDaemon


def main():
    parser = argparse.ArgumentParser(description="Run a GCON agent daemon")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--coordinator", required=True, help="host:port of the coordinator")
    parser.add_argument("--cert-dir", required=True)
    parser.add_argument("--hostname", default=None)
    parser.add_argument(
        "--capability", action="append", default=[],
        metavar="KEY=VALUE", help="repeatable, e.g. --capability gpu=A100",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    capabilities = {}
    for item in args.capability:
        if "=" not in item:
            parser.error(f"--capability must be KEY=VALUE, got: {item}")
        key, value = item.split("=", 1)
        capabilities[key] = value

    agent = GCONAgent(node_id=args.node_id)
    daemon = AgentDaemon(
        node_id=args.node_id,
        coordinator_address=args.coordinator,
        cert_dir=args.cert_dir,
        agent=agent,
        hostname=args.hostname,
        capabilities=capabilities,
    )

    def handle_signal(signum, frame):
        daemon.stop(reason=f"received signal {signum}")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    daemon.run_forever()


if __name__ == "__main__":
    main()
