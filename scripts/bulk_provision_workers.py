#!/usr/bin/env python3
"""
Provision cert material for many agents in one shot and emit a
ready-to-use .env file per node -- so bringing up worker #847 is
"download 1 file, run 1 command," not "clone the repo, run the cert
script, hand-copy three PEM files."

This does NOT talk to the coordinator and does NOT touch the CA
private key in the output -- it only wraps `gcon.transport.tls`
(the same functions `generate_dev_certs.py` uses) and formats the
result as env vars. Run this ONCE, centrally, on a machine you trust
(e.g. next to the coordinator) -- never on the ephemeral worker
itself. The CA's private key (ca.key.pem) never leaves --cert-dir;
only each worker's own cert/key + the CA's *public* cert go into
that worker's .env file.

Usage:
    # Mint 1000 fresh node ids worker-0001..worker-1000:
    python scripts/bulk_provision_workers.py \\
        --cert-dir keys/grpc \\
        --coordinator coordinator.example.com:50051 \\
        --count 1000 --prefix worker- --out-dir provisioned

    # Or provision specific, already-decided node ids:
    python scripts/bulk_provision_workers.py \\
        --cert-dir keys/grpc \\
        --coordinator coordinator.example.com:50051 \\
        --node-id kaggle-a100-1 --node-id kaggle-a100-2 \\
        --out-dir provisioned

Each run is idempotent (same as generate_dev_certs.py): re-running
with the same --cert-dir reuses each node's existing cert/key rather
than re-minting it, so you can top up (add more nodes later) without
invalidating already-deployed workers.

Output: provisioned/<node_id>.env, each containing everything
run_worker.py / docker/entrypoint.sh need:
    GCON_NODE_ID=...
    GCON_COORDINATOR_ADDRESS=...
    GCON_TLS_CERT_DIR=/etc/gcon/certs
    GCON_CA_CERT_B64=...
    GCON_AGENT_CERT_B64=...
    GCON_AGENT_KEY_B64=...

A worker (container, Kaggle session, bare VM -- doesn't matter, and
it's fine if the filesystem is wiped on every restart) loads that one
file and it has everything: no git clone, no re-running
generate_dev_certs.py, no manually hunting down which PEM file goes
where. See scripts/worker_bootstrap.sh for the non-Docker version of
docker/entrypoint.sh's materialize-from-env-vars step.
"""

import argparse
import base64
import os
import sys

sys.path.insert(0, "src")

from gcon.transport import tls


def _b64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")


def main():
    parser = argparse.ArgumentParser(
        description="Bulk-provision mTLS cert material + .env files for many agents"
    )
    parser.add_argument("--cert-dir", required=True, help="Where the CA + issued certs live/get written (keep this directory private -- it holds the CA private key)")
    parser.add_argument("--coordinator", required=True, help="Coordinator gRPC address, host:port, baked into each .env as GCON_COORDINATOR_ADDRESS")
    parser.add_argument("--coordinator-hostname", default="localhost", help="Hostname the coordinator's own server cert should be valid for (only matters the first time this cert-dir is used)")
    parser.add_argument("--count", type=int, default=0, help="Mint this many fresh sequential node ids (used with --prefix)")
    parser.add_argument("--prefix", default="worker-", help="Prefix for auto-minted node ids, e.g. worker- -> worker-0001")
    parser.add_argument("--start", type=int, default=1, help="First number for auto-minted node ids")
    parser.add_argument("--node-id", action="append", default=[], dest="node_ids", help="Explicit node id to provision (repeatable). Combine freely with --count.")
    parser.add_argument("--tls-cert-dir-on-worker", default="/etc/gcon/certs", help="GCON_TLS_CERT_DIR value written into each .env -- where the WORKER writes decoded certs, not this script's --cert-dir")
    parser.add_argument("--out-dir", required=True, help="Directory to write <node_id>.env files into")
    args = parser.parse_args()

    node_ids = list(args.node_ids)
    if args.count:
        width = max(4, len(str(args.start + args.count - 1)))
        node_ids += [f"{args.prefix}{i:0{width}d}" for i in range(args.start, args.start + args.count)]

    if not node_ids:
        parser.error("Provide --count and/or one or more --node-id")

    # Duplicates would silently reuse one cert for two workers -- refuse.
    dupes = {n for n in node_ids if node_ids.count(n) > 1}
    if dupes:
        parser.error(f"Duplicate node ids: {sorted(dupes)}")

    os.makedirs(args.out_dir, exist_ok=True)

    # Ensures the CA + coordinator cert exist; issue_agent_cert calls
    # below reuse them (and reuse any already-issued agent cert too).
    ca = tls.ensure_ca(args.cert_dir)
    tls.issue_coordinator_cert(args.cert_dir, hostname=args.coordinator_hostname)
    ca_b64 = _b64(ca.cert_path)

    written = []
    for node_id in node_ids:
        paths = tls.issue_agent_cert(args.cert_dir, node_id)
        env_path = os.path.join(args.out_dir, f"{node_id}.env")
        with open(env_path, "w") as f:
            f.write(f"GCON_NODE_ID={node_id}\n")
            f.write(f"GCON_COORDINATOR_ADDRESS={args.coordinator}\n")
            f.write(f"GCON_TLS_CERT_DIR={args.tls_cert_dir_on_worker}\n")
            f.write(f"GCON_CA_CERT_B64={ca_b64}\n")
            f.write(f"GCON_AGENT_CERT_B64={_b64(paths.cert_path)}\n")
            f.write(f"GCON_AGENT_KEY_B64={_b64(paths.key_path)}\n")
        os.chmod(env_path, 0o600)
        written.append(env_path)

    print(f"Provisioned {len(written)} node(s) into {args.out_dir}/")
    print(f"  first: {written[0]}")
    if len(written) > 1:
        print(f"  last:  {written[-1]}")
    print(
        "\nEach .env is self-contained and safe to hand to its worker "
        "(no CA private key inside). On the worker: source it, or pass "
        "it to `docker run --env-file`, or feed it to "
        "scripts/worker_bootstrap.sh for a non-Docker environment "
        "(Kaggle/Colab/bare VM)."
    )


if __name__ == "__main__":
    main()
