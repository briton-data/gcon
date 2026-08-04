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

    coordinator = GCONCoordinator(transport=None, control_plane=control_plane)

    def on_heartbeat(node_id, payload):
        coordinator.receive_heartbeat({
            "node_id": node_id,
            "status": payload["status"],
            "timestamp": datetime.now(UTC),
        })

    def on_node_registered(node_id, capabilities):
        proxy = RemoteNodeProxy(node_id, transport)
        coordinator.register_agent(proxy)
        logger.info("'%s' registered with scheduler, capabilities=%s", node_id, capabilities)

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