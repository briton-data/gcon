#!/usr/bin/env python3
"""
GCON agent entry point.

Starts an agent, connects it to a coordinator over mTLS, and runs
jobs dispatched to it. Unlike the old version of this script, every
identifying value below comes from CLI args / env vars, not a
hardcoded constant -- so you can actually run more than one real
worker against a real coordinator without editing this file.

Usage:
    python scripts/run_worker.py \
        --node-id worker-01 \
        --coordinator coordinator.example.com:50051 \
        --cert-dir /etc/gcon/certs \
        --org-id acme-corp

Env var equivalents (useful for systemd/containers):
    GCON_NODE_ID
    GCON_COORDINATOR_ADDRESS
    GCON_TLS_CERT_DIR
    GCON_ORG_ID

GPU capability is auto-detected via GCONAgent.detect_gpu() (GPUtil,
falls back to "Unknown GPU" if no GPU / GPUtil isn't installed) --
it is not hardcoded, so this reports whatever hardware is actually
on the machine it runs on.
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, "src")

from gcon.execution.agent import GCONAgent
from gcon.transport.agent_daemon import AgentDaemon


def main():
    parser = argparse.ArgumentParser(description="Start a GCON agent")
    parser.add_argument(
        "--node-id",
        default=os.environ.get("GCON_NODE_ID"),
        help="Unique node id for this agent (required; must be unique across the cluster)",
    )
    parser.add_argument(
        "--coordinator",
        default=os.environ.get("GCON_COORDINATOR_ADDRESS"),
        help="Coordinator gRPC address, host:port (required, e.g. 10.0.1.5:50051)",
    )
    parser.add_argument(
        "--cert-dir",
        default=os.environ.get("GCON_TLS_CERT_DIR", "/etc/gcon/certs"),
        help="Directory holding this node's cert + the shared CA cert "
             "(from generate_dev_certs.py). Default: /etc/gcon/certs",
    )
    parser.add_argument(
        "--org-id",
        default=os.environ.get("GCON_ORG_ID"),
        help="Which company/organization this (dedicated) node belongs to. "
             "Optional -- omit for a shared/unassigned node -- but required "
             "for a node to show up under a company on the dashboard's "
             "Companies panel or be counted in that org's usage.",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("GCON_LOG_LEVEL", "INFO"),
    )
    args = parser.parse_args()

    if not args.node_id:
        parser.error("--node-id is required (or set GCON_NODE_ID)")
    if not args.coordinator:
        parser.error("--coordinator is required (or set GCON_COORDINATOR_ADDRESS)")

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("gcon.run_worker")

    agent = GCONAgent(node_id=args.node_id)

    # Real detected hardware, not a hardcoded claim -- this is what
    # ends up in the node's registered capabilities and, downstream,
    # in job receipts.
    gpu_info = agent.detect_gpu()
    capabilities = {"gpu": gpu_info.get("gpu_name", "Unknown GPU")}
    if args.org_id:
        # "org_id" is a reserved capability key, not a real hardware
        # capability -- the coordinator's Register handler
        # (grpc_transport.py) special-cases it: pulled out into the
        # node's own org_id column, never stored alongside genuine
        # capabilities like gpu=A100 in node_capabilities.
        capabilities["org_id"] = args.org_id

    logger.info(
        "Starting agent '%s' -> coordinator %s (cert dir: %s, org: %s, detected gpu: %s)",
        args.node_id, args.coordinator, args.cert_dir, args.org_id or "(none)", capabilities["gpu"],
    )

    daemon = AgentDaemon(
        node_id=args.node_id,
        coordinator_address=args.coordinator,
        cert_dir=args.cert_dir,
        agent=agent,
        capabilities=capabilities,
    )
    daemon.run_forever()


if __name__ == "__main__":
    main()
