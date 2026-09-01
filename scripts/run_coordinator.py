#!/usr/bin/env python3
"""
Main GCON coordinator entry point.

Starts the mTLS gRPC transport (coordinator <-> agents, see
gcon.transport.tls / grpc_transport.py: mutual auth, per-RPC peer-
certificate identity check), wires it into the scheduler via
CommunicationManager, and serves the dashboard + versioned public API
in the same process -- both already carrying the rest of the security
integration (security-headers middleware, rate-limited login, RBAC on
every management route, API-key/scope auth on /api/v1, opt-in CORS,
optional HTTPS termination off the same CA) from web_server.py and
api_v1.py.

Usage:
    python scripts/run_coordinator.py [--db PATH] [--log-level LEVEL]

Relevant environment variables (see gcon.transport.config for the
full transport list):
    GCON_TLS_CERT_DIR      shared CA / cert directory (default keys/grpc)
    GCON_GRPC_HOST/PORT    gRPC transport bind address (default 0.0.0.0:50051)
    GCON_DASHBOARD_HOST/PORT  dashboard/API bind address (default 127.0.0.1:8000)
    GCON_FORCE_HTTPS       1 to terminate TLS on the dashboard/API too,
                           enable HSTS, and mark the session cookie Secure
    GCON_API_CORS_ORIGINS  comma-separated origins allowed to call /api/v1
                           from a browser (unset = no CORS, API-key-only
                           SDK/server clients are unaffected either way)
    GCON_HA_LEASE_TTL_SECONDS  lease TTL for --ha leader election
                           (default 10s; see gcon.cluster.leader_election)
"""

import argparse
import logging
import os
import sys
from datetime import datetime, UTC

sys.path.insert(0, "src")

from gcon.persistence.control_plane import ControlPlane
from gcon.transport.config import TransportConfig
from gcon.transport.grpc_transport import GrpcTransport
from gcon.transport.remote_node import RemoteNodeProxy
from gcon.cluster.coordinator import GCONCoordinator
from gcon.cluster.communication import CommunicationManager
from gcon.cluster.leader_election import LeaderElector, default_holder_id
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer


def main():
    parser = argparse.ArgumentParser(description="Run the GCON coordinator + dashboard/API")
    parser.add_argument("--db", default=None,
                         help="control-plane sqlite path (default: <data-dir>/gcon_control_plane.db)")
    parser.add_argument("--data-dir", default=None,
                         help="shared directory for both the control-plane DB and the "
                              "identity/session DB (default: $GCON_DATA_DIR or 'data'); "
                              "--db overrides just the control-plane path")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--ha", action="store_true",
        help="run as one of several coordinators sharing --db, using lease-based "
             "leader election (see gcon.cluster.leader_election) -- this process "
             "blocks at startup until it acquires leadership before serving "
             "anything, and exits if it ever loses the lease afterward (so a "
             "process supervisor can restart it cleanly into standby mode). "
             "Requires --db (or GCON_DATA_DIR) to point at a DB file every "
             "participating coordinator process can actually reach.",
    )
    parser.add_argument(
        "--coordinator-id", default=None,
        help="stable identity for --ha leader election (default: "
             "hostname:pid:random, regenerated every process start -- pass an "
             "explicit value if you want a restarted process to be "
             "recognizable in logs/lease history as \"the same\" coordinator)",
    )
    args = parser.parse_args()

    if args.data_dir:
        os.environ["GCON_DATA_DIR"] = args.data_dir

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("gcon.run_coordinator")

    control_plane = ControlPlane(path=args.db)
    config = TransportConfig.load(control_plane)

    leader_elector = None
    if args.ha:
        holder_id = args.coordinator_id or default_holder_id()

        def _on_lose_leadership():
            # See gcon.cluster.leader_election's module docstring for
            # why this exits rather than trying to keep running: a
            # demoted process would otherwise keep its gRPC transport
            # and web server up (nothing here tears those down), so a
            # worker or dashboard request could still land on it even
            # though scheduler_loop/submit_job now both correctly
            # refuse to act on it -- confusing 503s forever instead of
            # a clean restart into standby. os._exit (not sys.exit):
            # this callback runs on the elector's own background
            # thread, where sys.exit only unwinds that one thread.
            logger.critical(
                "'%s' lost leadership -- exiting so a process supervisor "
                "can restart this into standby mode", holder_id,
            )
            os._exit(1)

        leader_elector = LeaderElector(
            control_plane, holder_id=holder_id, on_lose_leadership=_on_lose_leadership,
        )
        logger.info(
            "--ha enabled as '%s': waiting to acquire coordinator leadership "
            "(lease TTL %.0fs)...", holder_id, leader_elector.ttl_seconds,
        )
        leader_elector.run_until_leader()
        logger.info("'%s' is now the active coordinator", holder_id)
        leader_elector.start()

    coordinator = GCONCoordinator(transport=None, control_plane=control_plane)
    coordinator.leader_elector = leader_elector

    def on_heartbeat(node_id, payload):
        coordinator.receive_heartbeat({
            "node_id": node_id,
            "status": payload["status"],
            "timestamp": datetime.now(UTC),
        })
        # payload already carries the agent's real, live cpu_percent/
        # memory_percent/running_jobs (see grpc_transport.py's
        # heartbeat envelope handling) -- this used to be discarded
        # here, silently, which meant every node's cpu/memory in the
        # registry sat at 0.0 forever except a one-shot update right
        # after that node finished a job. The scheduler's
        # cpu*0.5 + memory*0.3 + running_jobs*20 scoring was, in
        # effect, always scoring every idle node at ~0 -- load-aware
        # selection wasn't actually functioning. update_node_resources
        # deliberately never writes status (see its docstring), so
        # this can't race the atomic claim_best_idle_node() busy-flag
        # the way writing status here would.
        coordinator.receive_resource_report({
            "node_id": node_id,
            "cpu": payload["cpu_percent"],
            "memory": payload["memory_percent"],
            "running_jobs": payload["running_jobs"],
            "status": payload["status"],
            "timestamp": payload["timestamp"],
        })

    def on_node_registered(node_id, capabilities, org_id=None, address=None):
        proxy = RemoteNodeProxy(node_id, transport, org_id=org_id, address=address)
        coordinator.register_agent(proxy)
        logger.info(
            "'%s' registered with scheduler from %s, org=%s, capabilities=%s",
            node_id, address or "(unknown address)", org_id or "(none)", capabilities,
        )

    def on_node_disconnected(node_id):
        logger.info("node disconnected: %s", node_id)
        coordinator.on_node_disconnected(node_id)

    transport = GrpcTransport(
        control_plane=control_plane,
        config=config,
        on_heartbeat=on_heartbeat,
        on_node_registered=on_node_registered,
        on_node_disconnected=on_node_disconnected,
    )
    coordinator.communication = CommunicationManager(transport=transport)

    transport.start()
    logger.info(
        "gRPC transport (mTLS) listening on %s:%s, cert dir=%s",
        config.grpc_host, config.grpc_port, config.tls_cert_dir,
    )

    presentation = PresentationLayer(coordinator)
    web_server = WebServer(presentation)

    try:
        web_server.start()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("shutting down")
        coordinator.shutdown()
        transport.shutdown()
        control_plane.close()


if __name__ == "__main__":
    main()