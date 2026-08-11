# GCON Deployment & Operations Guide

Production-ready deployment instructions for GCON clusters.

---

## Table of Contents

1. [Pre-Deployment Checklist](#pre-deployment-checklist)
2. [Local Development](#local-development)
3. [Standalone Server Deployment](#standalone-server-deployment)
4. [Docker Deployment](#docker-deployment)
5. [Multi-Node Cluster](#multi-node-cluster)
6. [Security Hardening](#security-hardening)
7. [Monitoring & Observability](#monitoring--observability)
8. [Troubleshooting](#troubleshooting)
9. [Operations Runbooks](#operations-runbooks)

---

## Pre-Deployment Checklist

- [ ] Python 3.12+ installed on all machines
- [ ] Network connectivity between coordinator and agents (bidirectional)
- [ ] Persistent storage configured (database or object store)
- [ ] SSH/security groups allow coordinator ↔ agent communication (default port 8000)
- [ ] SSL/TLS certificates (for production)
- [ ] API key strategy defined (dev vs prod)
- [ ] Backup and disaster recovery plan
- [ ] Monitoring and alerting configured
- [ ] Capacity planning (expected job throughput, storage)
- [ ] Team trained on operational tasks

---

## Local Development

### Quick Start (5 min)

```bash
# 1. Clone and install
git clone https://github.com/briton-data/gcon.git
cd gcon
pip install -r requirements.txt

# 2. Generate dev mTLS certs (coordinator + agent + CA)
python scripts/generate_dev_certs.py --node worker-01

# 3. Start the coordinator (gRPC on :50051, dashboard/API on :8000)
python scripts/run_coordinator.py

# 4. Open the dashboard
# http://localhost:8000

# 5. In another terminal, start an agent — it connects to the
#    coordinator over mTLS gRPC, not an HTTP "register" call
python scripts/run_worker.py \
  --node-id worker-01 \
  --coordinator localhost:50051 \
  --cert-dir keys/grpc

# 6. Create an API key from the dashboard (Management > API Keys),
#    then submit a test job with the SDK — there is no "dev" bypass
#    key; every /api/v1 call needs a real key (see docs/API.md)
python -c "
from gcon_sdk import GconClient
client = GconClient(api_key='gcon_YOUR_API_KEY')
client.submit_job('test-job', 'echo hello')
print(client.get_job('test-job'))
"
```

---

## Standalone Server Deployment

### Architecture

```
Internet
    ↓ HTTPS
  [Load Balancer / Reverse Proxy (nginx/HAProxy)]
    ↓
  [GCON Coordinator + Web Server]
    ├─ In-memory job queue
    ├─ Event bus
    ├─ Workflow engine
    └─ REST API + Dashboard
    ↑ HTTP (internal)
    ├─ [Agent 1]
    ├─ [Agent 2]
    └─ [Agent N]
```

### Host Setup

**OS:** Ubuntu 20.04 LTS or later, AlmaLinux 9, or similar

**System Requirements:**
- CPU: 4+ cores recommended
- RAM: 8GB+ (for queued jobs, event log)
- Storage: 100GB+ (for receipts and artifacts)
- Network: 1Gbps+ connection to agents

**System Packages:**

```bash
sudo apt-get update
sudo apt-get install -y \
  python3.12 python3.12-venv python3-pip \
  git curl wget tmux supervisor nginx
```

### Installation

```bash
# 1. Create service user
sudo useradd -m -s /bin/bash gcon

# 2. Clone repository
cd /opt
sudo git clone https://github.com/briton-data/gcon.git
sudo chown -R gcon:gcon gcon

# 3. Create virtual environment
cd gcon
python3.12 -m venv venv
source venv/bin/activate

# 4. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 5. Generate crypto keys (for production)
mkdir -p keys
chmod 700 keys
# Move or generate coordinator's signing key
# cp /path/to/coordinator.key keys/
```

### Configuration

**Environment Variables** (`/opt/gcon/.env`):

These are the real settings read by `gcon.transport.config.TransportConfig`
(the gRPC/agent side) and documented in `scripts/run_coordinator.py`'s own
docstring (the dashboard/API side). There is no S3 or Postgres storage
backend — persistence is SQLite via `gcon.persistence.control_plane`
(`data/gcon_control_plane.db`), matching the "Single coordinator,
SQLite-backed, no HA/failover yet" status the README calls out. Env vars
override the `settings` table in that database, which overrides hardcoded
defaults — see `TransportConfig`'s docstring for the full precedence
rule.

```bash
# gRPC transport (coordinator <-> agents, mTLS) — see gcon.transport.config
GCON_GRPC_HOST=0.0.0.0
GCON_GRPC_PORT=50051
GCON_TLS_CERT_DIR=/opt/gcon/keys/grpc
GCON_HEARTBEAT_INTERVAL_SECONDS=5
GCON_HEARTBEAT_MISS_THRESHOLD=3
GCON_JOB_DISPATCH_TIMEOUT_SECONDS=3600
GCON_GRPC_MAX_WORKERS=256          # effectively "how many agents can be
                                    # connected at once" — scale with fleet size
GCON_GRACEFUL_SHUTDOWN_GRACE_SECONDS=30

# Dashboard / public API (see scripts/run_coordinator.py docstring)
GCON_DASHBOARD_HOST=127.0.0.1
GCON_DASHBOARD_PORT=8000
GCON_FORCE_HTTPS=0                 # 1 to terminate TLS here too, enable
                                    # HSTS, and mark the session cookie Secure
GCON_API_CORS_ORIGINS=             # comma-separated origins allowed to
                                    # call /api/v1 from a browser; unset =
                                    # no CORS (API-key/SDK clients unaffected)
```

There is no `GCON_STORAGE_BACKEND`, `GCON_API_KEY_REQUIRED`, or
`GCON_ENABLE_HTTPS` setting — API-key auth on `/api/v1` and RBAC on
`/management` are always on (see `docs/API.md`), and HTTPS is controlled
by `GCON_FORCE_HTTPS` above, not a separate cert/key pair passed via env.

### Systemd Service

**File:** `/etc/systemd/system/gcon-coordinator.service`

`scripts/run_coordinator.py` is the real production entry point — it
starts the mTLS gRPC transport *and* serves the dashboard + `/api/v1` in
the same process. (`python -m gcon.dashboard.dashboard_server` is a
separate, in-memory **local-dev** convenience script — it boots a fake
local cluster of `GCON_LOCAL_NODE_COUNT` in-process agents with no mTLS,
useful for manual testing, not for a real fleet. Don't use it here.)

```ini
[Unit]
Description=GCON Coordinator
After=network.target

[Service]
Type=simple
User=gcon
WorkingDirectory=/opt/gcon
Environment="PATH=/opt/gcon/venv/bin"
EnvironmentFile=/opt/gcon/.env
ExecStart=/opt/gcon/venv/bin/python scripts/run_coordinator.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**Start:**

```bash
sudo systemctl daemon-reload
sudo systemctl enable gcon-coordinator
sudo systemctl start gcon-coordinator

# Check status
sudo systemctl status gcon-coordinator
sudo journalctl -u gcon-coordinator -f
```

### Reverse Proxy (nginx)

**File:** `/etc/nginx/sites-available/gcon`

```nginx
upstream gcon_backend {
    server 127.0.0.1:8000;
}

server {
    listen 443 ssl http2;
    server_name gcon.example.com;

    ssl_certificate /etc/letsencrypt/live/gcon.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/gcon.example.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    client_max_body_size 1G;

    location / {
        proxy_pass http://gcon_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_buffering off;
        proxy_request_buffering off;
    }

    # Server-Sent Events (don't buffer)
    location /api/v1/stream {
        proxy_pass http://gcon_backend;
        proxy_buffering off;
        proxy_cache off;
        proxy_set_header Connection "";
        proxy_http_version 1.1;
    }
}

# Redirect HTTP to HTTPS
server {
    listen 80;
    server_name gcon.example.com;
    return 301 https://$server_name$request_uri;
}
```

**Enable:**

```bash
sudo ln -s /etc/nginx/sites-available/gcon /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

---

## Docker Deployment

There is exactly one `Dockerfile` in this repo (`docker/Dockerfile`), and
it packages the **agent only** — not the coordinator. There is currently
no `docker-compose.yml` in the repo and no coordinator container image;
if you want the coordinator containerized you'll need to write that
Dockerfile yourself (a plain `python scripts/run_coordinator.py` on top
of the same base image works). What follows is what actually ships.

### Dockerfile

**File:** `docker/Dockerfile`

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY scripts/run_worker.py scripts/run_worker.py
COPY docker/entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
```

### Entrypoint

**File:** `docker/entrypoint.sh`

The entrypoint doesn't `docker run` straight into `run_worker.py` — it
first **materializes the agent's mTLS cert material from base64-encoded
env vars**, because the image's filesystem is treated as ephemeral (the
comment in the script calls it out explicitly: "Enclave filesystem is a
ramdisk — nothing here survives a restart"). It expects:

- `GCON_CA_CERT_B64` — the CA's public cert, base64-encoded
- `GCON_AGENT_CERT_B64` / `GCON_AGENT_KEY_B64` — this node's own
  pre-issued cert/key pair, base64-encoded (generate these once with
  `scripts/generate_dev_certs.py`, or your real CA tooling — the CA's
  *private* key is never put in an env var or baked into the image)
- `GCON_NODE_ID` — used both to name the materialized cert files and as
  the agent's node id
- `GCON_TLS_CERT_DIR` — where to write them (default `/etc/gcon/certs`)

It also starts a trivial background HTTP listener on `$PORT` (default
`8080`) purely so host platforms that health-check by scanning for an
open port (e.g. some PaaS "web service" types) don't kill the container
— the agent itself never serves HTTP; it only holds an outbound gRPC
connection to the coordinator. Then it execs
`python scripts/run_worker.py`, which reads `GCON_NODE_ID`,
`GCON_COORDINATOR_ADDRESS`, and `GCON_TLS_CERT_DIR` (see
`scripts/run_worker.py`'s own `--help`).

**Build and run an agent container:**

```bash
docker build -f docker/Dockerfile -t gcon-agent .

docker run -d \
  -e GCON_NODE_ID=worker-01 \
  -e GCON_COORDINATOR_ADDRESS=coordinator.example.com:50051 \
  -e GCON_CA_CERT_B64="$(base64 -w0 keys/grpc/ca.cert.pem)" \
  -e GCON_AGENT_CERT_B64="$(base64 -w0 certs/agent-worker-01.cert.pem)" \
  -e GCON_AGENT_KEY_B64="$(base64 -w0 certs/agent-worker-01.key.pem)" \
  --name gcon-agent-worker-01 \
  gcon-agent
```

The coordinator itself is run as a plain process today (`python
scripts/run_coordinator.py`), not from a container image — see
[Multi-Node Cluster](#multi-node-cluster) and the systemd unit below.

---

## Multi-Node Cluster

### Architecture

```
┌─ Coordinator (Central)
│  ├─ REST API + Dashboard
│  ├─ Job scheduler
│  ├─ Event bus
│  └─ Storage (shared)
│
├─ Agents (Multiple hosts)
│  ├─ Agent 1 (GPU-1)
│  ├─ Agent 2 (GPU-2)
│  ├─ Agent 3 (GPU-3)
│  └─ Agent N (GPU-N)
│
└─ Storage Backend (Shared)
   ├─ Job metadata
   ├─ Receipts
   └─ Artifacts
```

### Agent Deployment

**On each worker node:**

Agents connect to the coordinator directly over mTLS gRPC via
`scripts/run_worker.py` — there's no separate "start-agent.py" wrapper or
HTTP `register()` call to write; the script itself is the entry point,
and every identifying value (node id, coordinator address, cert dir)
comes from CLI args or env vars (see the script's own docstring).

```bash
# 1. Install
git clone https://github.com/briton-data/gcon.git
cd gcon
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Provision this node's mTLS identity (once, from your CA / dev-cert
#    tooling) — e.g. keys/grpc/{ca.cert.pem,worker-1.cert.pem,worker-1.key.pem}
#    or use scripts/generate_dev_certs.py for local/dev clusters.

# 3. Create systemd service
sudo tee /etc/systemd/system/gcon-agent.service > /dev/null << 'EOF'
[Unit]
Description=GCON Agent
After=network.target

[Service]
Type=simple
User=gcon
WorkingDirectory=/opt/gcon
Environment="PATH=/opt/gcon/venv/bin"
Environment="GCON_NODE_ID=worker-1"
Environment="GCON_COORDINATOR_ADDRESS=coordinator.internal:50051"
Environment="GCON_TLS_CERT_DIR=/opt/gcon/keys/grpc"
ExecStart=/opt/gcon/venv/bin/python scripts/run_worker.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable gcon-agent
sudo systemctl start gcon-agent
```

### Shared Storage Setup

There is currently **no PostgreSQL or S3 storage backend** in GCON, and
no `GCON_STORAGE_BACKEND` / `GCON_ARTIFACT_BACKEND` setting to switch one
on — this whole section described infrastructure the codebase doesn't
have. Persistence is exclusively local SQLite, via
`gcon.persistence.control_plane.ControlPlane` and the coordinator's own
`gcon.persistence.db` module:

- `data/gcon.db` — coordinator's primary state
- `data/gcon_control_plane.db` — settings, API keys, users, audit log

Both use SQLite's WAL mode (you'll see matching `-shm`/`-wal` files next
to each `.db`). This is exactly what the README's status section already
flags: **single coordinator, SQLite-backed, no HA/failover yet.** For a
production deployment today, that means:

- `data/` and `keys/`/`certs/` must live on durable, backed-up storage
  (see [Backup and Restore](#backup-and-restore) below) — there is no
  built-in replication.
- Running two coordinators against the same `data/` directory is not a
  supported HA setup; SQLite isn't a multi-writer network database.
- If you need a different storage backend, that's a real gap to file an
  issue/PR against — don't configure env vars for a backend that isn't
  wired up, since GCON will silently ignore them and keep using SQLite.

---

## Security Hardening

### 1. Network Security

**Firewall Rules:**

```bash
# Allow only coordinator <-> agent traffic
sudo ufw allow from 10.0.0.0/8 to any port 8000 proto tcp  # Agents
sudo ufw allow from 203.0.113.0/24 to any port 443 proto tcp  # Public API

# Block direct access to coordinator from internet
sudo ufw default deny incoming
```

**TLS/SSL:**

```bash
# Get certificate (Let's Encrypt)
sudo certbot certonly --standalone -d gcon.example.com

# Configure nginx (see above)
sudo systemctl restart nginx
```

### 2. Cryptographic Keys

**Coordinator Key:**

```bash
# Generate 4096-bit RSA key
openssl genrsa -out keys/coordinator.key 4096
openssl rsa -in keys/coordinator.key -pubout -out keys/coordinator.pub

# Restrict permissions
chmod 600 keys/coordinator.key
chmod 644 keys/coordinator.pub
```

**Agent Keys:**

```bash
# Each agent should have its own key
# Generate during agent provisioning:
openssl genrsa -out keys/agent-{id}.key 4096
openssl rsa -in keys/agent-{id}.key -pubout -out keys/agent-{id}.pub

# Distribute public keys to coordinator securely
# (via config management, not HTTP)
```

**Key Storage (Production):**

- Store private keys in **Hardware Security Module (HSM)**
- Store in **Trusted Platform Module (TPM)** on agents
- Use **AWS KMS**, **Azure Key Vault**, or equivalent
- Never commit keys to version control

### 3. API Authentication

**API Keys:**

Every `/api/v1` request already requires a real API key — there is no
"require API keys" toggle to flip in production; it's unconditional (see
`docs/API.md`). Keys are created through the management layer (either
from the dashboard's **Management → API Keys** panel, or its
`POST /management/api-keys` route), not generated standalone with
`secrets.token_urlsafe` — that ties the key to an owner, its scopes, and
an expiry in `APIKeyManager`, which a hand-rolled token wouldn't have:

```bash
curl -X POST -b "session=<dashboard session cookie>" \
  -H "Content-Type: application/json" \
  -d '{"name": "CI pipeline", "owner_user_id": "user_abc123",
       "scopes": ["View monitoring", "Submit workflows"], "expires_in_days": 90}' \
  http://localhost:8000/management/api-keys
```

Send the returned secret as `Authorization: Bearer <key>` or
`X-API-Key: <key>` on `/api/v1` calls:

```bash
curl -H "Authorization: Bearer gcon_..." \
  https://gcon.example.com/api/v1/cluster
```

There is no JWT support — bearer tokens are opaque API-key secrets
looked up in the `api_keys` table, not signed/decoded JWTs.

### 4. Agent Verification

```python
# Coordinator should verify agent identity
# before accepting job results

from gcon.verification import ReceiptVerifier

receipt = agent_submission  # From agent
public_key = registry.get_agent_public_key(receipt['agent_id'])

verifier = ReceiptVerifier()
if not verifier.verify(receipt, public_key):
    # Reject receipt, alert security team
    raise Exception(f"Unverified receipt from {receipt['agent_id']}")
```

---

## Monitoring & Observability

### Logging

**Configure structured logging:**

```python
import logging
import json

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            'timestamp': self.formatTime(record),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage()
        }
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        return json.dumps(log_data)

handler = logging.FileHandler('/var/log/gcon/coordinator.log')
handler.setFormatter(JSONFormatter())
logger = logging.getLogger('gcon')
logger.addHandler(handler)
```

**Ship logs to central system:**

```bash
# Use ELK Stack, Splunk, Datadog, etc.
# Example with Filebeat:
sudo apt-get install filebeat

# /etc/filebeat/filebeat.yml
filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /var/log/gcon/*.log

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
```

### Metrics

**Export Prometheus metrics:**

```python
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import time

# Metrics
jobs_submitted = Counter('gcon_jobs_submitted_total', 'Total jobs submitted')
jobs_completed = Counter('gcon_jobs_completed_total', 'Total jobs completed')
jobs_failed = Counter('gcon_jobs_failed_total', 'Total jobs failed')
job_duration = Histogram('gcon_job_duration_seconds', 'Job execution time')
agents_online = Gauge('gcon_agents_online', 'Number of online agents')
queue_depth = Gauge('gcon_queue_depth', 'Current job queue depth')

# Start metrics server
start_http_server(8001)  # Prometheus scrapes http://localhost:8001
```

**Prometheus config** (`/etc/prometheus/prometheus.yml`):

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'gcon'
    static_configs:
      - targets: ['localhost:8001']
```

### Alerting

**Prometheus alert rules** (`/etc/prometheus/alerts.yml`):

```yaml
groups:
  - name: gcon
    rules:
      - alert: CoordinatorDown
        expr: up{job="gcon"} == 0
        for: 2m
        annotations:
          summary: "GCON Coordinator is down"

      - alert: HighJobFailureRate
        expr: rate(gcon_jobs_failed_total[5m]) > 0.1
        annotations:
          summary: "Job failure rate is high (>10%)"

      - alert: NoAgentsOnline
        expr: gcon_agents_online == 0
        for: 1m
        annotations:
          summary: "No agents online"

      - alert: QueueDepthHigh
        expr: gcon_queue_depth > 1000
        annotations:
          summary: "Job queue depth exceeds 1000"
```

### Health Checks

```bash
# Simple health check endpoint
curl http://localhost:8000/api/v1/cluster

# Add to load balancer / k8s liveness probe
```

---

## Troubleshooting

### Coordinator won't start

**Check logs:**

```bash
journalctl -u gcon-coordinator -n 50
tail -f /var/log/gcon/coordinator.log
```

**Common issues:**

1. **Port already in use**
   ```bash
   lsof -i :8000
   sudo kill -9 <PID>
   ```

2. **Missing dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Permission errors**
   ```bash
   sudo chown -R gcon:gcon /opt/gcon
   sudo chown -R gcon:gcon /var/log/gcon
   ```

### Agents not registering

**Check network:**

```bash
# From agent:
curl -v http://coordinator:8000/api/v1/cluster

# From coordinator:
sudo tcpdump -i any -n port 8000
```

**Check agent logs:**

```bash
journalctl -u gcon-agent -f
```

**Issues:**

- Firewall blocking port 50051 (the gRPC transport port agents actually
  connect on — not 8000, which is the dashboard/API port) or the
  coordinator's gRPC hostname not resolvable
- Agent stuck as `busy` and not picking up new work — there's no
  configurable per-agent "capacity"/concurrency setting in the current
  code (`GCONAgent` takes only a `node_id`); check `status` and
  `running_jobs` on `GET /api/v1/nodes/{node_id}` instead

### Jobs stuck in pending

**Check cluster status:**

```bash
curl http://localhost:8000/api/v1/cluster
curl http://localhost:8000/api/v1/nodes
```

**Issues:**

- No agents registered (`total_nodes == 0`)
- All agents are `offline` (check agent heartbeats)
- All agents are `busy` (at capacity, add more agents)

### Storage running out of space

**Check disk usage:**

```bash
du -sh /var/lib/gcon/storage
df -h /var/lib/gcon/

# List largest artifacts
find /var/lib/gcon/storage -type f -exec du -h {} \; | sort -rh | head
```

**Solutions:**

- Archive old receipts/artifacts to S3
- Increase disk allocation
- Switch to cloud storage backend

---

## Operations Runbooks

### Graceful Coordinator Shutdown

```bash
# 1. Drain jobs (stop accepting new submissions)
# TODO: Add --drain flag to coordinator

# 2. Wait for running jobs to complete
watch -n 1 'curl http://localhost:8000/api/v1/cluster | grep running_jobs'

# 3. Stop coordinator
sudo systemctl stop gcon-coordinator

# 4. Backup state (if using disk storage)
tar czf /backups/gcon-state-$(date +%s).tar.gz /var/lib/gcon/storage
```

### Add a New Agent

```bash
# 1. Provision machine
# 2. Install and configure (see above)
# 3. Start agent
sudo systemctl start gcon-agent

# 4. Verify registration
curl http://localhost:8000/api/v1/nodes | grep "docker-agent-1"

# 5. Confirm it's accepting jobs
# (submit a test job, check assignment)
```

### Deregister an Agent

There is no `DELETE /api/v1/nodes/{id}` on the public API — draining and
deregistration are dashboard/session-authenticated routes (RBAC
`"Manage cluster"` permission), not part of the API-key-authenticated
`/api/v1` surface. Call them from an authenticated dashboard session (or
script one with a logged-in session cookie), not with a bare `curl` and
an API key:

```bash
# Graceful deregistration (drain jobs first)

# 1. Drain the node — coordinator stops assigning it new jobs, running
#    jobs finish normally (POST /cluster/nodes/{node_id}/drain)
curl -X POST -b "session=<your dashboard session cookie>" \
  http://localhost:8000/cluster/nodes/gpu-1/drain

# 2. Wait for running jobs on gpu-1 to complete

# 3. Deregister it (POST /admin/nodes/{node_id}/deregister)
curl -X POST -b "session=<your dashboard session cookie>" \
  http://localhost:8000/admin/nodes/gpu-1/deregister

# 4. Stop agent
sudo systemctl stop gcon-agent

# 4. Perform maintenance
# (update hardware, upgrade software, etc.)

# 5. Re-register agent
sudo systemctl start gcon-agent
```

### Backup & Restore

```bash
# Back up the real state: the SQLite databases (data/), the mTLS keys
# (keys/, certs/) — there is no separate "storage" or S3 path; this is
# it, per gcon.persistence.control_plane and the repo's data/ directory
tar czf /backups/gcon-full-$(date +%Y%m%d).tar.gz \
  /opt/gcon/data \
  /opt/gcon/keys \
  /opt/gcon/certs

# Restore
sudo systemctl stop gcon-coordinator
tar xzf /backups/gcon-full-20260720.tar.gz -C /opt/gcon
sudo systemctl start gcon-coordinator
```

### Monitor Coordinator Health

`GET /api/v1/cluster` has no `status` field — use `GET /api/v1/health`,
whose `state` field is exactly what `HealthService.compute()` produces
(`"healthy"`, or one of the other states — check `checks`/`reasons` in
the same response for why, if not):

```bash
#!/bin/bash
# Monitor coordinator in production — requires an API key with
# 'View monitoring' scope (see docs/API.md)

while true; do
  response=$(curl -s -H "Authorization: Bearer $GCON_API_KEY" \
    http://localhost:8000/api/v1/health)
  state=$(echo "$response" | jq -r '.state')
  timestamp=$(date)

  echo "[$timestamp] Coordinator: $state"

  if [ "$state" != "healthy" ]; then
    echo "ALERT: Coordinator unhealthy! $(echo "$response" | jq -c '.reasons')"
    # Send to alert system
  fi
  
  sleep 30
done
```

