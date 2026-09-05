#!/usr/bin/env python3
"""
Mint a per-org enroll token -- the credential a customer's worker
CLI presents to the Enroll RPC to prove "allowed to enroll a worker
for THIS org," replacing the old single shared GCON_ENROLL_TOKEN
(which authorized enrollment but couldn't say which org a worker
belonged to). See EnrollTokenRepository and grpc_transport.py's
Enroll handler.

The token is printed ONCE. GCON stores only its SHA-256 hash --
there is no "show me that token again" later, only revoke + mint a
new one (see --revoke).

Usage:
    # Mint a new token for an org:
    python scripts/create_enroll_token.py --org-id acme-corp --label "acme prod workers"

    # List existing (non-revoked by default) tokens for an org:
    python scripts/create_enroll_token.py --org-id acme-corp --list

    # Revoke a token by its token_id (shown by --list):
    python scripts/create_enroll_token.py --revoke <token_id>

Run this against the same control-plane DB the coordinator itself
uses (GCON_CONTROL_PLANE_DB_PATH / GCON_DATA_DIR -- see gcon.config),
typically on/next to the coordinator, not on a worker.
"""

import argparse
import sys

sys.path.insert(0, "src")

from gcon.persistence.control_plane import ControlPlane


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--org-id", help="Org to mint/list tokens for")
    parser.add_argument("--label", help="Optional human-readable label for a new token (e.g. 'acme prod workers')")
    parser.add_argument("--list", action="store_true", help="List tokens for --org-id instead of minting one")
    parser.add_argument("--revoke", metavar="TOKEN_ID", help="Revoke a token by its token_id")
    parser.add_argument("--db-path", help="Explicit control-plane DB path (defaults to the coordinator's own resolution)")
    args = parser.parse_args()

    with ControlPlane(path=args.db_path) as cp:
        if args.revoke:
            cp.enroll_tokens.revoke(args.revoke)
            print(f"Revoked token_id={args.revoke}")
            return

        if not args.org_id:
            parser.error("--org-id is required (unless using --revoke)")

        if args.list:
            rows = cp.enroll_tokens.list_for_org(args.org_id)
            if not rows:
                print(f"No enroll tokens found for org_id={args.org_id!r}")
                return
            for r in rows:
                status = "revoked" if r["revoked_at"] else "active"
                print(f"{r['token_id']}  [{status}]  label={r['label']!r}  created_at={r['created_at']}")
            return

        token = cp.enroll_tokens.create_token(org_id=args.org_id, label=args.label)
        print(f"Enroll token for org_id={args.org_id!r} (save this now -- it will not be shown again):\n")
        print(f"  {token}\n")
        print("Worker enrollment command:")
        print(
            f"  python scripts/run_worker.py --node-id <node-id> --coordinator <host:port> "
            f"--cert-dir <dir> --enroll-token {token} --enroll-address <host:enroll-port>"
        )


if __name__ == "__main__":
    main()
