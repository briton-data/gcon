#!/bin/sh
# Enclave filesystem is a ramdisk -- nothing here survives a restart,
# so certs are materialized from secrets on every boot.
#
# Only the CA's PUBLIC cert + this node's own pre-issued cert/key are
# needed (see the tls.py fix: leaf certs are reused if present, not
# re-minted) -- the CA PRIVATE key never has to leave the machine
# that ran generate_dev_certs.py.
set -e

CERT_DIR="${GCON_TLS_CERT_DIR:-/etc/gcon/certs}"
mkdir -p "$CERT_DIR"

echo "$GCON_CA_CERT_B64"    | base64 -d > "$CERT_DIR/ca.cert.pem"
echo "$GCON_AGENT_CERT_B64" | base64 -d > "$CERT_DIR/agent-${GCON_NODE_ID}.cert.pem"
echo "$GCON_AGENT_KEY_B64"  | base64 -d > "$CERT_DIR/agent-${GCON_NODE_ID}.key.pem"

exec python scripts/run_worker.py
