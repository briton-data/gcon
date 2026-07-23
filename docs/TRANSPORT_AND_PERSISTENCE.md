# GCON Distributed Foundation: Transport Layer + Persistence Layer

This document covers exactly the two subsystems built for this task.
The scheduler, workflow engine, dashboard, autoscaling, and execution
engine are unmodified.

## What changed, at a glance

- **`src/gcon/cluster/communication.py`** was rewritten so
  `CommunicationManager` depends only on `gcon.transport.interfaces.Transport`,
  via constructor injection, defaulting to `LocalTransport` (behavior-preserving —
  every existing test and call site is unaffected).
- **`src/gcon/transport/`** — new package: the `Transport` interface,
  `LocalTransport`, and `GrpcTransport` (real gRPC/HTTP2/mTLS), plus the
  `AgentDaemon` that runs on worker machines.
- **`src/gcon/persistence/`** — new package: the durable control-plane
  database (SQLite today, Postgres-compatible by construction),
  migrations, and one repository per required table.

Nothing else was touched.

## Architecture

```
Coordinator
 ├── Transport Layer      (gcon.transport)
 │    ├── interfaces.Transport            <- CommunicationManager depends on this only
 │    ├── local_transport.LocalTransport  <- default; in-process, current behavior
 │    ├── grpc_transport.GrpcTransport    <- gRPC/HTTP2/mTLS server, real network
 │    ├── agent_daemon.AgentDaemon        <- runs on worker machines (gRPC client)
 │    ├── tls.py                          <- mTLS certificate authority + credentials
 │    ├── config.py                       <- env -> db settings -> defaults precedence
 │    ├── idempotency.py                  <- message-id / sequence helpers
 │    └── proto/gcon_transport.proto      <- wire contract (Register, Control, StreamLogs, UploadReceipt)
 │
 ├── Persistence Layer     (gcon.persistence)
 │    ├── db.ControlPlaneDatabase         <- connection, WAL, migrations, dialect abstraction
 │    ├── migrations/registry.py          <- versioned schema (9 tables)
 │    ├── repositories/*.py               <- one repository per table, idempotent writes
 │    └── control_plane.ControlPlane      <- DI facade wiring db + all repositories
 │
 └── Presentation Layer     (unchanged — dashboard, etc.)
```

`CommunicationManager`, `GrpcTransport`, and `AgentDaemon` never touch
`sqlite3` directly; they go through `ControlPlane`'s repositories.
`GrpcTransport`/`AgentDaemon` never import each other's internals —
only the shared `.proto` contract.

## Why gRPC over HTTP/2 with TLS

See the module docstring in `gcon/transport/tls.py` and
`gcon/transport/proto/gcon_transport.proto` for the full reasoning;
in short: one multiplexed HTTP/2 connection carries the unary calls
(Register, UploadReceipt) and the long-lived bidirectional Control
stream (heartbeats, job assignment, cancellation) without separate
polling machinery, protobuf gives a versioned typed contract, and
mutual TLS is gRPC's native, first-class credential mechanism. Agents
are the gRPC *clients* (they dial the coordinator, not the reverse),
because worker machines are commonly behind NAT/firewalls with no
inbound connectivity — job assignment flows down a stream the agent
itself opened.

## Mutual authentication

Each agent's client certificate Common Name is its node identity,
verified during the TLS handshake itself. `AgentControlServicer.Register`
additionally checks that the in-band claimed `node_id` matches the
TLS-verified certificate CN (`grpc_transport.py::_peer_common_name`) —
an agent cannot register as an identity it doesn't hold a certificate
for. The server requires client certificates
(`grpc.ssl_server_credentials(..., require_client_auth=True)`); a
connection without one is rejected during the handshake before any
RPC executes. See `tests/transport/test_grpc_transport.py::
test_register_rejected_when_claimed_node_id_does_not_match_certificate`
and `::test_connection_without_client_certificate_is_rejected`.

Provisioning: `scripts/generate_dev_certs.py` issues a self-managed CA
and per-node certificates on top of `cryptography` (already a project
dependency). Operators with an existing CA can point `tls_cert_dir` at
certificates issued by that CA instead.

## Reconnection

`AgentDaemon.run_forever()` retries the entire connect-register-serve
cycle on any failure, with exponential backoff bounded by
`reconnect_initial_backoff_seconds` / `reconnect_max_backoff_seconds`
/ `reconnect_backoff_multiplier` (all configurable). A reconnect is a
fresh `Register` call producing a fresh session token; the coordinator
side's `NodeRepository.upsert` is idempotent, so a reconnecting node
resumes cleanly with no duplicate rows. See
`tests/transport/test_grpc_transport.py::test_agent_reconnects_after_coordinator_restart`,
which kills and restarts the coordinator's gRPC server out from under
an already-running `AgentDaemon` and confirms it finds its way back
without being restarted itself.

## Idempotent message processing

Every write path that could plausibly be retried (by a coordinator
retry, a reconnect, or a duplicate delivery) is deduplicated at the
database level via a `UNIQUE` constraint, not by trusting the sender
not to resend:

| Operation | Idempotency key | Enforced by |
|---|---|---|
| Job dispatch | `job_attempts.request_message_id` (UNIQUE) | `JobAttemptRepository.record_attempt` |
| Heartbeat | `(heartbeats.node_id, sequence)` (UNIQUE) | `HeartbeatRepository.record` |
| Log line | `(execution_logs.attempt_id, stream, sequence)` (UNIQUE) | `ExecutionLogRepository.append` |
| Receipt upload | `receipts.receipt_hash` (UNIQUE) | `ReceiptRepository.upload` |
| Node registration | `nodes.node_id` (PK), upsert | `NodeRepository.upsert` |

Sequence numbers are minted by the sender (agent) and must be resumed,
not reset, across a reconnect — see `gcon.transport.idempotency.SequenceCounter`.

## Configuration precedence

Environment variable > database `settings` row > hardcoded default.
No operational value (port, host, timeouts, backoff, cert directory)
is hardcoded anywhere else — every one is read through
`gcon.transport.config.TransportConfig` / `ConfigResolver`. See its
module docstring for the full key list and env var names.

## Crash safety and thread safety

- **Persistence:** WAL journal mode, `synchronous=FULL`, foreign keys
  on, a `threading.RLock`-guarded connection, and all multi-statement
  writes wrapped in `ControlPlaneDatabase.transaction()` (commit or
  full rollback, never partial). See `gcon/persistence/db.py`.
- **Transport:** each connected node's live state
  (`grpc_transport.NodeSession`) uses its own lock for the pending-result
  table; the servicer's session dict is guarded by a single `RLock`.
  `LocalTransport` is similarly lock-guarded. See
  `tests/persistence/test_control_plane.py::test_concurrent_heartbeats_thread_safe`
  and `tests/transport/test_local_transport.py::test_local_transport_thread_safety`.

## Graceful shutdown

`AgentDaemon.stop()` stops accepting new jobs, lets the executor drain
in-flight jobs, sends a `ShutdownNotice` envelope, and closes its
channel. `GrpcTransport.shutdown()` closes all live sessions, stops
the gRPC server with a grace period, and waits (bounded) for
disconnect bookkeeping to finish before returning — see
`tests/transport/test_grpc_transport.py::test_graceful_daemon_shutdown_notifies_coordinator`.

## SQLite -> PostgreSQL migration path

See the module docstring in `gcon/persistence/db.py`. Summary: every
migration is portable SQL; the one genuine divergence (auto-incrementing
surrogate keys on the three append-only tables) is isolated behind a
`{{PK}}` template token expanded per-`Dialect`; all other primary keys
are application-generated UUID text, identical on both engines;
timestamps are ISO-8601 TEXT; no SQLite-only SQL functions are used
anywhere. Swapping the driver (`sqlite3` -> `psycopg`) and selecting
`PostgresDialect` is the entire migration — no schema rewrite.

## Tests

- `tests/persistence/test_control_plane.py` (18 tests) — migrations,
  schema, foreign keys, WAL/crash-safety pragmas, and idempotent
  dedup for every repository, including a concurrent-writer thread-safety test.
- `tests/transport/test_local_transport.py` (11 tests) — `CommunicationManager`
  dependency injection and default behavior preservation.
- `tests/transport/test_config.py` (6 tests) — env/db/default precedence.
- `tests/transport/test_tls.py` (5 tests) — CA/cert issuance, identity, credentials.
- `tests/transport/test_grpc_transport.py` (14 tests) — **real** gRPC server + real
  mTLS channels + real `AgentDaemon` processes over real localhost TCP ports:
  registration, mutual-auth rejection (both the impersonation case and the
  no-client-cert case), heartbeats, job dispatch, cancellation (kills a real
  `sleep 30` subprocess), log streaming, receipt upload, coordinator-restart
  reconnection, and graceful shutdown.

Run everything:

```
pip install -r requirements.txt
PYTHONPATH=src pytest tests/persistence tests/transport -v
```

## Running a real cluster

```bash
# 1. Provision certs once
python scripts/generate_dev_certs.py --cert-dir /etc/gcon/certs \
    --coordinator-hostname coordinator.internal \
    --node worker-01 --node worker-02

# 2. On the coordinator machine
python scripts/gcon_coordinator_grpc.py --db /var/lib/gcon/control_plane.db

# 3. On each worker machine
python scripts/gcon_agent_daemon.py --node-id worker-01 \
    --coordinator coordinator.internal:50051 \
    --cert-dir /etc/gcon/certs --capability gpu=A100
```

A coordinator process that also runs the existing scheduler/dispatcher
wires the two together with:

```python
from gcon.persistence.control_plane import ControlPlane
from gcon.transport.config import TransportConfig
from gcon.transport.grpc_transport import GrpcTransport
from gcon.cluster.communication import CommunicationManager

control_plane = ControlPlane()  # or path=...
transport = GrpcTransport(control_plane=control_plane, config=TransportConfig.load(control_plane))
transport.start()

comm = CommunicationManager(transport=transport)  # same interface as before
```
