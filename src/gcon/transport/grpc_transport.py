"""
GrpcTransport — the real, network `Transport` implementation.

Runs a gRPC server (`AgentControlServicer`) that agent daemons
(`gcon.transport.agent_daemon.AgentDaemon`) connect to. Implements the
`Transport` interface so `CommunicationManager` can be constructed
with `CommunicationManager(transport=GrpcTransport(...))` and behave
identically, from the coordinator's point of view, to
`LocalTransport` -- `send_job` still blocks and returns
`{"status": "success", "result": {...}}`, `cancel_job` still returns
a bool, etc. The network round trip, mutual-TLS authentication,
heartbeats, reconnect tolerance, and idempotent message processing
are all internal to this class.

Concurrency model
-------------------
Each connected agent has one `NodeSession`: an outbound queue of
`CoordinatorEnvelope` messages (job assignments, cancellations,
pings) and a table of `threading.Event`s that `send_job` blocks on,
keyed by the idempotency `request_message_id` of the dispatch. The
`Control` RPC handler runs two loops concurrently for the lifetime of
one agent's stream: a reader loop (a background thread) that consumes
inbound `AgentEnvelope`s (heartbeats, job results, shutdown notices)
and a writer loop (the RPC handler itself, as a generator) that
drains the outbound queue and yields envelopes to the client. This is
the standard gRPC bidi-streaming pattern; it's what lets the
coordinator *push* a job assignment down a connection the agent
itself opened, without polling.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from concurrent import futures
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Callable

import grpc

from gcon.persistence.control_plane import ControlPlane
from gcon.transport import tls
from gcon.transport.config import TransportConfig
from gcon.transport.errors import (
    JobDispatchTimeoutError,
    NodeUnauthenticatedError,
    NodeUnavailableError,
)
from gcon.transport.idempotency import new_message_id
from gcon.transport.interfaces import Transport
from gcon.transport.proto import gcon_transport_pb2 as pb
from gcon.transport.proto import gcon_transport_pb2_grpc as pb_grpc

logger = logging.getLogger(__name__)


def _peer_common_name(context: grpc.ServicerContext) -> Optional[str]:
    """Extract the client certificate's Common Name from an mTLS peer
    connection -- this is the agent's authenticated identity, as
    verified by the TLS handshake itself (not something the agent
    merely claims in a message field)."""
    auth_context = context.auth_context() or {}
    names = auth_context.get("x509_common_name")
    if not names:
        return None
    value = names[0]
    return value.decode("utf-8") if isinstance(value, bytes) else value


class _PendingResult:
    def __init__(self):
        self.event = threading.Event()
        self.envelope: Optional[pb.JobResult] = None


class NodeSession:
    """Live connection state for one registered node. Not durable --
    this is exactly the "process, not currently durable across a
    restart" state `gcon.storage.database` already called out;
    durable facts about the node itself live in `ControlPlane.nodes`.
    """

    def __init__(self, node_id: str, session_token: str):
        self.node_id = node_id
        self.session_token = session_token
        self.outbound: "queue.Queue[Optional[pb.CoordinatorEnvelope]]" = queue.Queue()
        self.connected = threading.Event()
        self.pending_results: Dict[str, _PendingResult] = {}
        self._lock = threading.RLock()

    def register_waiter(self, request_message_id: str) -> _PendingResult:
        pending = _PendingResult()
        with self._lock:
            self.pending_results[request_message_id] = pending
        return pending

    def resolve(self, request_message_id: str, envelope: pb.JobResult) -> bool:
        with self._lock:
            pending = self.pending_results.pop(request_message_id, None)
        if pending is None:
            return False
        pending.envelope = envelope
        pending.event.set()
        return True

    def send(self, envelope: pb.CoordinatorEnvelope) -> None:
        self.outbound.put(envelope)

    def close(self) -> None:
        self.connected.clear()
        self.outbound.put(None)
        with self._lock:
            for pending in self.pending_results.values():
                pending.event.set()
            self.pending_results.clear()


class AgentControlServicer(pb_grpc.AgentControlServicer):
    def __init__(
        self,
        control_plane: ControlPlane,
        config: TransportConfig,
        on_heartbeat: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        on_node_disconnected: Optional[Callable[[str], None]] = None,
        on_node_registered=None,
    ):
        self.control_plane = control_plane
        self.config = config
        self.on_heartbeat = on_heartbeat
        self.on_node_disconnected = on_node_disconnected
        self.on_node_registered = on_node_registered
        self._sessions: Dict[str, NodeSession] = {}
        self._lock = threading.RLock()

    # -------------------------------------------------------- registration
    def Register(self, request, context):
        peer_cn = _peer_common_name(context)
        if peer_cn is None:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "no client certificate presented")
        if peer_cn != request.node_id:
            # Mutual authentication: the TLS-verified certificate identity
            # must match the node_id the agent claims in-band. A node
            # cannot register as an identity it doesn't hold a certificate
            # for.
            context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f"certificate identity '{peer_cn}' does not match claimed node_id "
                f"'{request.node_id}'",
            )

        session_token = uuid.uuid4().hex
        with self._lock:
            self._sessions[request.node_id] = NodeSession(request.node_id, session_token)

        self.control_plane.nodes.upsert(
            node_id=request.node_id,
            hostname=request.hostname or request.node_id,
            status="idle",
            transport_endpoint=context.peer(),
            agent_version=request.agent_version or None,
            auth_fingerprint=peer_cn,
            metadata={"capabilities": dict(request.capabilities)},
        )
        if request.capabilities:
            self.control_plane.node_capabilities.set_capabilities(
                request.node_id, dict(request.capabilities)
            )
        self.control_plane.cluster_events.record(
            "NODE_REGISTERED", node_id=request.node_id,
            payload={"hostname": request.hostname},
        )
        
        if self.on_node_registered:
            self.on_node_registered(request.node_id, dict(request.capabilities))
        
        logger.info("Node '%s' registered from %s", request.node_id, context.peer())
        return pb.RegisterResponse(
            accepted=True,
            session_token=session_token,
            heartbeat_interval_seconds=int(self.config.heartbeat_interval_seconds),
        )

    # -------------------------------------------------------- control stream
    def Control(self, request_iterator, context):
        state = {"node_id": None, "session": None}
        stop = threading.Event()

        def reader():
            try:
                for envelope in request_iterator:
                    if state["session"] is None:
                        session = self._sessions.get(envelope.node_id)
                        if session is None or session.session_token != envelope.session_token:
                            logger.warning(
                                "Rejecting Control stream for unknown/invalid session: %s",
                                envelope.node_id,
                            )
                            stop.set()
                            return
                        state["node_id"] = envelope.node_id
                        state["session"] = session
                        session.connected.set()
                        self.control_plane.nodes.set_status(envelope.node_id, "idle")

                    self._handle_agent_envelope(state["session"], envelope)
            except grpc.RpcError:
                # Expected when the agent's channel closes (graceful
                # shutdown or a genuine network drop) -- the writer
                # loop's `finally` below handles disconnect bookkeeping
                # either way, so this isn't logged as an error.
                logger.info("Control stream for node %s ended (peer disconnected)", state["node_id"])
            except Exception:
                logger.exception("Control stream reader failed for node %s", state["node_id"])
            finally:
                stop.set()

        reader_thread = threading.Thread(target=reader, daemon=True)
        reader_thread.start()

        try:
            while not stop.is_set():
                session = state["session"]
                if session is None:
                    time.sleep(0.02)
                    continue
                try:
                    envelope = session.outbound.get(timeout=0.1)
                except queue.Empty:
                    continue
                if envelope is None:
                    break
                yield envelope
        finally:
            node_id = state["node_id"]
            session = state["session"]
            if node_id is not None:
                with self._lock:
                    if self._sessions.get(node_id) is session:
                        del self._sessions[node_id]
                if session is not None:
                    session.close()
                # Best-effort: during a coordinator-wide shutdown the
                # control-plane database may already be closing
                # concurrently with the last few agents disconnecting.
                # Disconnect bookkeeping should never crash the gRPC
                # server's request-handling thread pool over that race.
                try:
                    self.control_plane.nodes.set_status(node_id, "offline")
                    self.control_plane.cluster_events.record(
                        "NODE_DISCONNECTED", node_id=node_id
                    )
                except Exception:
                    logger.warning(
                        "Could not persist disconnect bookkeeping for node '%s' "
                        "(control plane likely shutting down)", node_id,
                    )
                if self.on_node_disconnected:
                    self.on_node_disconnected(node_id)
                logger.info("Node '%s' disconnected", node_id)

    def _handle_agent_envelope(self, session: NodeSession, envelope) -> None:
        kind = envelope.WhichOneof("payload")
        if kind == "heartbeat":
            hb = envelope.heartbeat
            recorded = self.control_plane.heartbeats.record(
                node_id=envelope.node_id,
                sequence=hb.sequence,
                status=hb.status,
                cpu_percent=hb.cpu_percent,
                memory_percent=hb.memory_percent,
                running_jobs=hb.running_jobs,
            )
            if recorded:
                self.control_plane.nodes.set_status(envelope.node_id, hb.status)
                if self.on_heartbeat:
                    self.on_heartbeat(
                        envelope.node_id,
                        {
                            "status": hb.status,
                            "cpu_percent": hb.cpu_percent,
                            "memory_percent": hb.memory_percent,
                            "running_jobs": hb.running_jobs,
                            "timestamp": hb.timestamp,
                        },
                    )
        elif kind == "job_result":
            jr = envelope.job_result
            resolved = session.resolve(jr.request_message_id, jr)
            attempt = self.control_plane.job_attempts.get_by_request_id(
                jr.request_message_id
            )
            if attempt is not None:
                self.control_plane.job_attempts.set_status(
                    attempt["attempt_id"],
                    status=jr.status,
                    error=jr.error or None,
                    completed=True,
                )
            if not resolved:
                logger.warning(
                    "Received JobResult for unknown/expired request_message_id=%s "
                    "(job_id=%s) -- dispatcher likely already timed out.",
                    jr.request_message_id, jr.job_id,
                )
        elif kind == "shutdown_notice":
            self.control_plane.cluster_events.record(
                "NODE_SHUTDOWN_NOTICE",
                node_id=envelope.node_id,
                payload={"reason": envelope.shutdown_notice.reason},
            )

    # -------------------------------------------------------------- logs
    def StreamLogs(self, request_iterator, context):
        last_sequence = 0
        for chunk in request_iterator:
            session = self._sessions.get(chunk.node_id)
            if session is None or session.session_token != chunk.session_token:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid session")
            self.control_plane.execution_logs.append(
                job_id=chunk.job_id,
                attempt_id=chunk.attempt_id or None,
                node_id=chunk.node_id,
                stream=chunk.stream or "stdout",
                sequence=chunk.sequence,
                content=chunk.content,
            )
            last_sequence = chunk.sequence
        return pb.LogAck(last_sequence_received=last_sequence)

    # ----------------------------------------------------------- receipts
    def UploadReceipt(self, request, context):
        session = self._sessions.get(request.node_id)
        if session is None or session.session_token != request.session_token:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid session")

        import json as _json

        existing = self.control_plane.receipts.get_by_hash(request.receipt_hash)
        stored = self.control_plane.receipts.upload(
            job_id=request.job_id,
            payload=_json.loads(request.payload_json),
            receipt_hash=request.receipt_hash,
            attempt_id=request.attempt_id or None,
            node_id=request.node_id,
            signature=request.signature or None,
        )
        return pb.ReceiptAck(accepted=stored is not None, duplicate=existing is not None)


class GrpcTransport(Transport):
    """
    `Transport` implementation used by `CommunicationManager` when
    running against real, remote agents. Owns the gRPC server
    lifecycle; `register_node`/`send_job`/`cancel_job` operate on
    whichever agent is currently connected under that node_id (an
    agent that reconnects after a network blip re-registers and
    resumes receiving dispatches transparently).
    """

    def __init__(
        self,
        control_plane: Optional[ControlPlane] = None,
        config: Optional[TransportConfig] = None,
        on_heartbeat: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        on_node_disconnected: Optional[Callable[[str], None]] = None,
    ):
        self.control_plane = control_plane or ControlPlane()
        self.config = config or TransportConfig.load(self.control_plane)
        self.servicer = AgentControlServicer(
            self.control_plane, self.config,
            on_heartbeat=on_heartbeat, on_node_disconnected=on_node_disconnected,
            on_node_registered=on_node_registered,
        )
        self._server: Optional[grpc.Server] = None

    # ---------------------------------------------------------- lifecycle
    def start(self) -> None:
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=32),
            options=[
                ("grpc.max_send_message_length", self.config.grpc_max_message_bytes),
                ("grpc.max_receive_message_length", self.config.grpc_max_message_bytes),
            ],
        )
        pb_grpc.add_AgentControlServicer_to_server(self.servicer, self._server)
        credentials = tls.load_server_credentials(
            self.config.tls_cert_dir, hostname=self.config.grpc_host
            if self.config.grpc_host not in ("0.0.0.0", "")
            else "localhost",
        )
        bind_addr = f"{self.config.grpc_host}:{self.config.grpc_port}"
        actual_port = self._server.add_secure_port(bind_addr, credentials)
        self._server.start()
        logger.info("GrpcTransport listening on %s (TLS, mTLS required)", bind_addr)
        return actual_port

    def wait_for_termination(self, timeout: Optional[float] = None) -> None:
        if self._server is not None:
            self._server.wait_for_termination(timeout=timeout)

    # ------------------------------------------------------ Transport API
    def register_node(self, node: Any) -> None:
        # Real registration happens via the Register RPC (see
        # AgentControlServicer.Register), driven by the remote agent
        # dialing in -- there is nothing for the coordinator side to
        # do here beyond accepting a node_id string for interface
        # symmetry with LocalTransport. Kept as a no-op rather than
        # raising, so code written against the Transport interface
        # doesn't need to branch on which implementation is active.
        pass

    def unregister_node(self, node_id: str) -> None:
        with self.servicer._lock:
            session = self.servicer._sessions.pop(node_id, None)
        if session is not None:
            session.close()
        self.control_plane.nodes.remove(node_id)

    def get_node(self, node_id: str) -> Any:
        session = self.servicer._sessions.get(node_id)
        if session is None or not session.connected.is_set():
            raise NodeUnavailableError(f"Node '{node_id}' is not connected.")
        return session

    def list_node_ids(self) -> List[str]:
        return [
            node_id
            for node_id, session in list(self.servicer._sessions.items())
            if session.connected.is_set()
        ]

    def send_job(
        self, node_id: str, job_id: str, command: str, timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        session = self.get_node(node_id)

        request_message_id = new_message_id()
        self.control_plane.jobs.ensure_exists(job_id, command, timeout_seconds=int(timeout) if timeout else None)
        attempt = self.control_plane.job_attempts.record_attempt(job_id, node_id, request_message_id)

        pending = session.register_waiter(request_message_id)
        session.send(
            pb.CoordinatorEnvelope(
                job_assign=pb.JobAssign(
                    job_id=job_id,
                    command=command,
                    timeout_seconds=int(timeout) if timeout else 0,
                    request_message_id=request_message_id,
                    attempt_id=attempt["attempt_id"],
                )
            )
        )

        wait_timeout = timeout or self.config.job_dispatch_timeout_seconds
        # Give the network/agent a little slack beyond the job's own
        # execution timeout, since the agent enforces the same
        # deadline locally and needs time to report back.
        got_result = pending.event.wait(timeout=wait_timeout + 15)

        if not got_result or pending.envelope is None:
            raise JobDispatchTimeoutError(
                f"No result received for job '{job_id}' on node '{node_id}' "
                f"within {wait_timeout + 15}s"
            )

        jr = pending.envelope
        import json as _json

        result = {
            "job_id": jr.job_id,
            "status": jr.status,
            "return_code": jr.return_code,
            "runtime_seconds": jr.runtime_seconds,
            "stdout": jr.stdout,
            "stderr": jr.stderr,
            "timestamp": jr.timestamp,
        }
        if jr.error:
            result["error"] = jr.error
        if jr.metrics_json:
            result["metrics"] = _json.loads(jr.metrics_json)

        self.control_plane.jobs.set_status(job_id, jr.status, result=result, completed=True)

        return {"status": "success", "result": result}

    def cancel_job(self, node_id: str, job_id: str) -> bool:
        try:
            session = self.get_node(node_id)
        except NodeUnavailableError:
            return False
        session.send(pb.CoordinatorEnvelope(job_cancel=pb.JobCancel(job_id=job_id)))
        return True

    def shutdown(self, grace_period: Optional[float] = None) -> None:
        grace = grace_period if grace_period is not None else self.config.graceful_shutdown_grace_seconds
        with self.servicer._lock:
            sessions = list(self.servicer._sessions.values())
        for session in sessions:
            session.close()
        if self._server is not None:
            self._server.stop(grace).wait()
        # Extra bounded wait for each Control stream's own disconnect
        # bookkeeping (run on the server's request-handling threads,
        # not something server.stop().wait() strictly synchronizes on)
        # to finish, so a caller that closes the ControlPlane database
        # immediately after shutdown() returns doesn't race it.
        deadline = time.monotonic() + min(grace, 5)
        while self.servicer._sessions and time.monotonic() < deadline:
            time.sleep(0.05)
        logger.info("GrpcTransport shut down gracefully (grace=%ss)", grace)
