#!/usr/bin/env python3
"""
Provision a GCON cluster's mTLS certificates: a shared CA, one
coordinator server certificate, and one client certificate per agent
node_id. Intended for development/self-managed-CA deployments -- see
`gcon.transport.tls` module docstring for using an external CA
instead.

Usage:
    python scripts/generate_dev_certs.py \
        --cert-dir /etc/gcon/certs \
        --coordinator-hostname coordinator.internal \
        --node worker-01 --node worker-02 --node worker-03
"""

import argparse
import sys

sys.path.insert(0, "src")

from gcon.transport import tls


def main():
    parser = argparse.ArgumentParser(description="Provision GCON cluster mTLS certificates")
    parser.add_argument("--cert-dir", required=True)
    parser.add_argument("--coordinator-hostname", default="localhost")
    parser.add_argument("--node", action="append", default=[], dest="nodes")
    args = parser.parse_args()

    ca = tls.ensure_ca(args.cert_dir)
    print(f"CA:                {ca.cert_path}")

    coord = tls.issue_coordinator_cert(args.cert_dir, hostname=args.coordinator_hostname)
    print(f"Coordinator cert:  {coord.cert_path}")
    print(f"Coordinator key:   {coord.key_path}")

    for node_id in args.nodes:
        agent = tls.issue_agent_cert(args.cert_dir, node_id)
        print(f"Agent '{node_id}' cert: {agent.cert_path}")

    if not args.nodes:
        print("No --node given; only the CA and coordinator cert were issued.")


if __name__ == "__main__":
    main()
