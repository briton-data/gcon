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
        --org-id acme-corp \
        --capability gpu=A100 --capability ram_gb=128

Env var equivalents (useful for systemd/containers):
    GCON_NODE_ID
    GCON_COORDINATOR_ADDRESS
    GCON_TLS_CERT_DIR
    GCON_ORG_ID

GPU capability is auto-detected via GCONAgent.detect_gpu() (GPUtil,
falls back to "Unknown GPU" if no GPU / GPUtil isn't installed) --
it is not hardcoded, so this reports whatever hardware is actually
on the machine it runs on. Any --capability flag with the same key
(e.g. --capability gpu=...) overrides the auto-detected value.

Handles SIGTERM/SIGINT by calling AgentDaemon.stop() with the signal
as the reason, so an operator- or orchestrator-initiated shutdown (a
service manager stopping the unit, a container getting SIGTERM) is
recorded as a deliberate stop rather than surfacing as the node going
silently offline.
"""

import argparse
import logging
import os
import signal
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
        "--hostname",
        default=os.environ.get("GCON_HOSTNAME"),
        help="Override the hostname this node reports to the coordinator "
             "(e.g. for a container whose auto-detected hostname isn't "
             "externally reachable/meaningful). Default: the machine's "
             "actual hostname (socket.gethostname()).",
    )
    parser.add_argument(
        "--tls-sni-override",
        default=os.environ.get("GCON_TLS_SNI_OVERRIDE"),
        help="Override the hostname used for TLS SNI/certificate "
             "verification (e.g. 'bore.pub') without changing "
             "--coordinator. Needed when the coordinator is reached "
             "through a proxy (e.g. Railway's TCP proxy) whose "
             "hostname isn't in the server cert's SAN, but a name "
             "that IS in the SAN still routes to it. Leave unset for "
             "local/dev runs where --coordinator's host already "
             "matches the cert.",
    )
    parser.add_argument(
        "--enroll-token",
        default=os.environ.get("GCON_ENROLL_TOKEN"),
        help="Shared bootstrap secret for first-boot self-enrollment "
             "(must match the coordinator's GCON_ENROLL_TOKEN). Only "
             "used when --cert-dir has no cert yet for this node_id -- "
             "ignored on every later run once a cert exists. The same "
             "value is meant to be baked into every worker's "
             "provisioning image; it is not a per-node secret.",
    )
    parser.add_argument(
        "--enroll-address",
        default=os.environ.get("GCON_ENROLL_ADDRESS"),
        help="host:port for the coordinator's plaintext enroll port "
             "(only used during first-boot self-enrollment). This is "
             "NOT --coordinator's port -- behind a proxy like "
             "Railway's, the enroll port is exposed as its own "
             "separate external address. Defaults to --coordinator "
             "if unset (fine for a direct, unproxied connection).",
    )
    parser.add_argument(
        "--capability", action="append", default=[],
        metavar="KEY=VALUE",
        help="Repeatable, e.g. --capability ram_gb=128. Merged with the "
             "auto-detected gpu capability; a --capability gpu=... here "
             "overrides the auto-detected value.",
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
    # gpu_memory_total_mb / cpu_cores: real, already-collected numbers
    # (detect_gpu() and os.cpu_count() respectively) that used to be
    # measured and then simply discarded here -- Scheduler.select_node's
    # `requires: {"min_vram_gb": ..., "min_cpu_cores": ...}` matching
    # for "resourced" jobs needs these to actually have something to
    # compare against. Only set gpu_memory_total_mb when a real GPU
    # was detected (memory_total is legitimately 0/absent otherwise --
    # never reported as a fabricated 0 that could pass a min_vram_gb
    # check it shouldn't).
    if gpu_info.get("memory_total"):
        capabilities["gpu_memory_total_mb"] = str(gpu_info["memory_total"])
    capabilities["cpu_cores"] = str(os.cpu_count() or 0)
    for item in args.capability:
        if "=" not in item:
            parser.error(f"--capability must be KEY=VALUE, got: {item}")
        key, value = item.split("=", 1)
        capabilities[key] = value
    if args.org_id:
        # "org_id" is a reserved capability key, not a real hardware
        # capability -- the coordinator's Register handler
        # (grpc_transport.py) special-cases it: pulled out into the
        # node's own org_id column, never stored alongside genuine
        # capabilities like gpu=A100 in node_capabilities.
        capabilities["org_id"] = args.org_id

    logger.info(
        "Starting agent '%s' -> coordinator %s (cert dir: %s, org: %s, "
        "hostname: %s, capabilities: %s)",
        args.node_id, args.coordinator, args.cert_dir, args.org_id or "(none)",
        args.hostname or "(auto)", capabilities,
    )

    daemon = AgentDaemon(
        node_id=args.node_id,
        coordinator_address=args.coordinator,
        cert_dir=args.cert_dir,
        agent=agent,
        hostname=args.hostname,
        capabilities=capabilities,
        sni_override=args.tls_sni_override,
        enroll_token=args.enroll_token,
        enroll_address=args.enroll_address,
    )

    def handle_signal(signum, frame):
        logger.info("received signal %s, shutting down", signum)
        daemon.stop(reason=f"received signal {signum}")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    daemon.run_forever()


if __name__ == "__main__":
    main()
