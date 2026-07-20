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

# 2. Start coordinator + dashboard
python -m gcon.dashboard.dashboard_server

# 3. Open browser
# http://localhost:8000

# 4. In another terminal, register agents
python -c "
from gcon.execution.agent import GCONAgent
from gcon_sdk import GconClient

client = GconClient(api_key='dev')
for i in range(4):
    agent = GCONAgent(f'local-agent-{i}', capacity=4)
    agent.register('http://localhost:8000')
"

# 5. Submit a test job
python -c "
from gcon_sdk import GconClient
client = GconClient(api_key='dev')
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

```bash
# Coordinator settings
GCON_COORDINATOR_HOST=0.0.0.0
GCON_COORDINATOR_PORT=8000
GCON_WORKER_THREADS=4

# Storage
GCON_STORAGE_BACKEND=local  # local, s3, postgresql
GCON_STORAGE_PATH=/var/lib/gcon/storage

# Security
GCON_API_KEY_REQUIRED=false  # Set to true in production
GCON_ENABLE_HTTPS=false      # Set to true + configure certificates
GCON_TLS_CERT=/etc/gcon/certs/gcon.crt
GCON_TLS_KEY=/etc/gcon/certs/gcon.key

# Logging
GCON_LOG_LEVEL=INFO
GCON_LOG_FILE=/var/log/gcon/coordinator.log

# Job execution
GCON_JOB_TIMEOUT_SECONDS=300
GCON_JOB_MAX_RETRIES=3
GCON_AGENT_HEARTBEAT_TIMEOUT_SECONDS=15
```

### Systemd Service

**File:** `/etc/systemd/system/gcon-coordinator.service`

```ini
[Unit]
Description=GCON Coordinator
After=network.target
Wants=gcon-coordinator.service

[Service]
Type=simple
User=gcon
WorkingDirectory=/opt/gcon
Environment="PATH=/opt/gcon/venv/bin"
EnvironmentFile=/opt/gcon/.env
ExecStart=/opt/gcon/venv/bin/python -m gcon.dashboard.dashboard_server
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

### Dockerfile

**File:** `Dockerfile`

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git curl \
    && rm -rf /var/lib/apt/lists/*

# Copy repository
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create directories
RUN mkdir -p /app/keys /var/lib/gcon/storage /var/log/gcon

# Expose port
EXPOSE 8000

# Run coordinator
CMD ["python", "-m", "gcon.dashboard.dashboard_server"]
```

### Docker Compose

**File:** `docker-compose.yml`

```yaml
version: '3.8'

services:
  coordinator:
    build: .
    ports:
      - "8000:8000"
    environment:
      GCON_COORDINATOR_HOST: 0.0.0.0
      GCON_COORDINATOR_PORT: 8000
      GCON_STORAGE_PATH: /var/lib/gcon/storage
      GCON_LOG_LEVEL: INFO
    volumes:
      - ./keys:/app/keys:ro
      - gcon_storage:/var/lib/gcon/storage
      - gcon_logs:/var/log/gcon
    restart: unless-stopped

  # Optional: Add agents as separate containers
  agent-1:
    build: .
    depends_on:
      - coordinator
    environment:
      GCON_AGENT_ID: docker-agent-1
      GCON_AGENT_CAPACITY: 4
      GCON_COORDINATOR_URL: http://coordinator:8000
    command: >
      python -c "
      from gcon.execution.agent import GCONAgent
      agent = GCONAgent('docker-agent-1', capacity=4)
      agent.register('http://coordinator:8000')
      agent.run()
      "
    restart: unless-stopped

volumes:
  gcon_storage:
  gcon_logs:
```

**Run:**

```bash
docker-compose up -d

# View logs
docker-compose logs -f coordinator

# Stop
docker-compose down
```

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

```bash
# 1. Install
git clone https://github.com/briton-data/gcon.git
cd gcon
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Create agent startup script (start-agent.py)
cat > start-agent.py << 'EOF'
import os
from gcon.execution.agent import GCONAgent

agent_id = os.environ.get('AGENT_ID', 'default-agent')
coordinator_url = os.environ.get('COORDINATOR_URL', 'http://localhost:8000')
capacity = int(os.environ.get('AGENT_CAPACITY', '4'))

agent = GCONAgent(agent_id, capacity=capacity)
agent.register(coordinator_url)
agent.run()
EOF

# 3. Create systemd service
sudo tee /etc/systemd/system/gcon-agent.service > /dev/null << 'EOF'
[Unit]
Description=GCON Agent
After=network.target
Wants=gcon-agent.service

[Service]
Type=simple
User=gcon
WorkingDirectory=/opt/gcon
Environment="PATH=/opt/gcon/venv/bin"
Environment="AGENT_ID=worker-1"
Environment="COORDINATOR_URL=http://coordinator.internal:8000"
Environment="AGENT_CAPACITY=4"
ExecStart=/opt/gcon/venv/bin/python start-agent.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable gcon-agent
sudo systemctl start gcon-agent
```

### Shared Storage Setup

**PostgreSQL (for metadata):**

```bash
# 1. Install PostgreSQL
sudo apt-get install -y postgresql postgresql-contrib

# 2. Create database
sudo -u postgres psql << EOF
CREATE DATABASE gcon;
CREATE USER gcon WITH PASSWORD 'secure_password';
ALTER ROLE gcon SET client_encoding TO 'utf8';
ALTER ROLE gcon SET default_transaction_isolation TO 'read committed';
ALTER ROLE gcon SET default_transaction_deferrable TO on;
ALTER ROLE gcon SET default_transaction_level TO 'read committed';
GRANT ALL PRIVILEGES ON DATABASE gcon TO gcon;
EOF

# 3. Configure GCON to use PostgreSQL
export GCON_STORAGE_BACKEND=postgresql
export GCON_DATABASE_URL=postgresql://gcon:secure_password@localhost/gcon
```

**S3 (for artifacts):**

```bash
# 1. Create S3 bucket
aws s3 mb s3://gcon-artifacts --region us-east-1

# 2. Configure GCON
export GCON_ARTIFACT_BACKEND=s3
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export GCON_S3_BUCKET=gcon-artifacts
export GCON_S3_REGION=us-east-1
```

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

```bash
# In production, require API key for all requests
export GCON_API_KEY_REQUIRED=true

# Generate keys for users/applications
python -c "
import secrets
key = 'gcon_' + secrets.token_urlsafe(32)
print(f'New API Key: {key}')
# Store in secure key management system
"
```

**JWT Tokens (Future):**

```bash
# Once implemented, use JWT bearer tokens
# instead of static API keys
curl -H "Authorization: Bearer eyJ0eXA..." \
  https://gcon.example.com/api/v1/cluster
```

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

- Firewall blocking port 8000
- Hostname resolution failing (`coordinator:8000` not resolvable)
- Agent capacity = 0 (check AGENT_CAPACITY env var)

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

```bash
# Graceful deregistration (drain jobs first)

# 1. Mark agent as deregistering
curl -X DELETE http://localhost:8000/api/v1/nodes/gpu-1

# 2. Wait for running jobs to complete
# The coordinator stops assigning new jobs to gpu-1

# 3. Stop agent
sudo systemctl stop gcon-agent

# 4. Perform maintenance
# (update hardware, upgrade software, etc.)

# 5. Re-register agent
sudo systemctl start gcon-agent
```

### Backup & Restore

```bash
# Backup all state and artifacts
tar czf /backups/gcon-full-$(date +%Y%m%d).tar.gz \
  /var/lib/gcon/storage \
  /opt/gcon/keys

# Restore
tar xzf /backups/gcon-full-20260720.tar.gz -C /

# Restore from S3
aws s3 cp s3://gcon-backups/gcon-full-20260720.tar.gz - | tar xz -C /
```

### Monitor Coordinator Health

```bash
#!/bin/bash
# Monitor coordinator in production

while true; do
  response=$(curl -s http://localhost:8000/api/v1/cluster)
  status=$(echo $response | jq -r '.status')
  timestamp=$(date)
  
  echo "[$timestamp] Coordinator: $status"
  
  if [ "$status" != "healthy" ]; then
    echo "ALERT: Coordinator unhealthy!"
    # Send to alert system
  fi
  
  sleep 30
done
```

