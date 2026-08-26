#!/bin/sh
# Bring up ONE worker on a throwaway/ephemeral host (Kaggle, Colab, a
# spot VM, whatever) with no git clone and no manual cert wrangling.
# This is docker/entrypoint.sh's "materialize certs from base64 env
# vars" step, minus Docker -- for platforms where you can't run a
# container.
#
# Requires: python3, pip, and the env vars from ONE <node_id>.env
# produced by scripts/bulk_provision_workers.py already loaded into
# the environment (e.g. `set -a; . ./worker-0001.env; set +a` before
# calling this, or paste them as Kaggle/Colab secrets).
#
# Usage:
#   set -a; . ./worker-0001.env; set +a
#   sh scripts/worker_bootstrap.sh
#
# Safe to re-run: if the session dies and you restart the notebook,
# just re-source the same .env and re-run this -- nothing persisted
# on the dead session's disk was ever the source of truth, so there
# is nothing to lose or reclone.

set -e

: "${GCON_NODE_ID:?set GCON_NODE_ID (from your provisioned .env)}"
: "${GCON_COORDINATOR_ADDRESS:?set GCON_COORDINATOR_ADDRESS}"
: "${GCON_CA_CERT_B64:?set GCON_CA_CERT_B64}"
: "${GCON_AGENT_CERT_B64:?set GCON_AGENT_CERT_B64}"
: "${GCON_AGENT_KEY_B64:?set GCON_AGENT_KEY_B64}"

CERT_DIR="${GCON_TLS_CERT_DIR:-/etc/gcon/certs}"
GCON_REF="${GCON_GIT_REF:-main}"

mkdir -p "$CERT_DIR"
echo "$GCON_CA_CERT_B64"    | base64 -d > "$CERT_DIR/ca.cert.pem"
echo "$GCON_AGENT_CERT_B64" | base64 -d > "$CERT_DIR/agent-${GCON_NODE_ID}.cert.pem"
echo "$GCON_AGENT_KEY_B64"  | base64 -d > "$CERT_DIR/agent-${GCON_NODE_ID}.key.pem"
chmod 600 "$CERT_DIR"/agent-"${GCON_NODE_ID}".key.pem

# Install the `gcon` package straight from git -- no clone/cd needed,
# and it's idempotent (pip no-ops if already installed at this ref).
pip install --quiet "git+https://github.com/briton-data/gcon.git@${GCON_REF}"

# run_worker.py itself lives under scripts/, outside the installed
# package, so fetch just that one file instead of the whole repo.
curl -fsSL \
  "https://raw.githubusercontent.com/briton-data/gcon/${GCON_REF}/scripts/run_worker.py" \
  -o run_worker.py

exec python3 run_worker.py \
  --node-id "$GCON_NODE_ID" \
  --coordinator "$GCON_COORDINATOR_ADDRESS" \
  --cert-dir "$CERT_DIR"
