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

# The agent itself never serves HTTP -- it only holds an outbound
# gRPC connection to the coordinator. Some hosts (e.g. Render's free
# "Web Service" type) health-check by scanning for *any* open port
# and restart the container when they find none, which can kill a
# job mid-execution for reasons completely unrelated to the job or
# the gRPC connection. This trivial listener exists purely to satisfy
# that port scan; it does nothing else and the real agent code is
# unaffected either way.
python -c "
import http.server, os
port = int(os.environ.get('PORT', 8080))
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b'ok')
    def log_message(self, *a): pass
http.server.HTTPServer(('0.0.0.0', port), H).serve_forever()
" &

exec python scripts/run_worker.py
