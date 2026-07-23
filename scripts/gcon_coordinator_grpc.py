#!/usr/bin/env python3
"""
Entry point for running the coordinator's gRPC transport server on
its own (e.g. to smoke-test a cluster's TLS setup, or to run the
transport server as a sidecar process alongside a coordinator that
constructs `CommunicationManager(transport=...)` against the same
`ControlPlane` database).

Usage:
    python scripts/gcon_coordinator_grpc.py --db data/gcon_control_plane.db
"""

import argparse
import logging
import signal
import sys

sys.path.insert(0, "src")

from gcon.persistence.control_plane import ControlPlane
from gcon.transport.config import TransportConfig
from gcon.transport.grpc_transport import GrpcTransport


def main():
    parser = argparse.ArgumentParser(description="Run the GCON coordinator gRPC transport server")
    parser.add_argument("--db", default=None, help="control-plane sqlite path (default from config)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    control_plane = ControlPlane(path=args.db)
    config = TransportConfig.load(control_plane)
    transport = GrpcTransport(control_plane=control_plane, config=config)
    transport.start()

    def handle_signal(signum, frame):
        transport.shutdown()
        control_plane.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    transport.wait_for_termination()


if __name__ == "__main__":
    main()
