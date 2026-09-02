"""
AgentDaemon — the persistent process that runs on every worker
machine and communicates with the Coordinator's `GrpcTransport` over
gRPC/HTTP2/mTLS.

This wraps `gcon.execution.agent.GCONAgent` (the execution engine --
untouched, imported and used exactly as-is) with everything the
"persistent daemon" requirement asks for: automatic registration,
mutual authentication (its own client certificate), heartbeats,
automatic reconnect with exponential backoff, receiving job
submissions and cancellations, streaming logs, uploading signed
receipts (via the existing `gcon.execution.receipt.ReceiptGenerator`,
also untouched), and graceful shutdown.

Connection model: the daemon is the gRPC *client*. It dials the
coordinator and keeps the `Control` bidirectional stream open for its
entire lifetime, reconnecting (with backoff) whenever the connection
drops. See `grpc_transport.py`'s module docstring for why agents
dial out rather than the coordinator dialing in.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import socket
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional

import grpc

from gcon.execution.agent import GCONAgent
from gcon.execution.receipt import ReceiptGenerator
from gcon.transport import tls
from gcon.transport.config import TransportConfig
from gcon.transport.idempotency import SequenceCounter
from gcon.transport.proto import gcon_transport_pb2 as pb
from gcon.transport.proto import gcon_transport_pb2_grpc as pb_grpc

logger = logging.getLogger(__name__)


class AgentDaemon:
    def __init__(
        self,
        node_id: str,
        coordinator_address: str,
        cert_dir: str,
        agent: Optional[GCONAgent] = None,
        hostname: Optional[str] = None,
        capabilities: Optional[Dict[str, str]] = None,
        config: Optional[TransportConfig] = None,
        sni_override: Optional[str] = None,
        enroll_token: Optional[str] = None,
        enroll_address: Optional[str] = None,
    ):
        self.node_id = node_id
        self.coordinator_address = coordinator_address
        self.cert_dir = cert_dir
        self.agent = agent or GCONAgent(node_id=node_id)
        self.hostname = hostname or socket.gethostname()
        # Forces TLS SNI / hostname verification to a fixed name
        # (e.g. "bore.pub") regardless of what host we actually dial
        # -- needed when the coordinator sits behind a proxy whose
        # real hostname (e.g. Railway's "caboose.proxy.rlwy.net")
        # isn't in the server cert's SAN, but a name that IS in the
        # SAN (bore.pub/localhost) still reaches it at the TCP layer.
        # None (default) preserves the existing behavior of
        # verifying against whatever host we actually resolved.
        self.sni_override = sni_override
        # Shared bootstrap secret for first-boot self-enrollment (see
        # _ensure_enrolled). Only ever used once per worker -- after
        # a cert exists on disk it's never read again, so it's safe
        # to bake the same value into every worker's provisioning
        # image without that being a per-node secret.
        self.enroll_token = enroll_token
        # External host:port for the coordinator's plaintext enroll
        # port (see GrpcTransport.start()'s second server) -- NOT the
        # same as coordinator_address's port once behind something
        # like Railway's TCP proxy, where each exposed internal port
        # gets its own distinct external port/proxy. Falls back to
        # coordinator_address only for the simple case (no proxy in
        # front, internal port + 1 reachable directly).
        self.enroll_address = enroll_address or coordinator_address
        self.capabilities = {k: str(v) for k, v in (capabilities or {}).items()}
        self.config = config or TransportConfig.load(control_plane=None)

        self._stop = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"job-{node_id}")
        self._hb_sequence = SequenceCounter()
        self._active_jobs: Dict[str, object] = {}
        self._run_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------- control
    def start(self) -> None:
        """Start the daemon's connection loop in a background thread.
        Returns immediately; use `run_forever()` to block instead."""
        self._run_thread = threading.Thread(target=self.run_forever, daemon=True)
        self._run_thread.start()

    def run_forever(self) -> None:
        backoff = self.config.reconnect_initial_backoff_seconds
        while not self._stop.is_set():
            try:
                logger.info("Connecting to coordinator at %s ...", self.coordinator_address)
                self._connect_and_serve()
                backoff = self.config.reconnect_initial_backoff_seconds
            except Exception:
                logger.exception(
                    "Lost connection to coordinator; reconnecting in %.1fs", backoff
                )
                self._stop.wait(backoff)
                backoff = min(
                    backoff * self.config.reconnect_backoff_multiplier,
                    self.config.reconnect_max_backoff_seconds,
                )

    def stop(self, reason: str = "operator requested shutdown") -> None:
        """Graceful shutdown: stop accepting new jobs, let in-flight
        jobs finish (bounded by `graceful_shutdown_grace_seconds`),
        notify the coordinator, then tear down the connection."""
        logger.info("Graceful shutdown requested: %s", reason)
        self._shutdown_reason = reason
        self._stop.set()
        self._executor.shutdown(wait=True, cancel_futures=False)
        if self._run_thread is not None:
            self._run_thread.join(timeout=self.config.graceful_shutdown_grace_seconds)

    # ------------------------------------------------------------ session
    def _resolve_ipv4_target(self) -> tuple[str, str]:
        """
        Resolve `self.coordinator_address` to an IPv4 literal.

        gRPC's default (c-ares) resolver doesn't consult the local
        routing table, so in environments with a broken/absent IPv6
        route but a real AAAA record for the target (Colab, several
        container/sandbox setups, some corporate networks), it can
        get stuck retrying an unreachable IPv6 address indefinitely
        instead of falling through to a working IPv4 one.

        Returns (ipv4_target, original_host) so the caller can keep
        using `original_host` for TLS SNI / hostname verification --
        forcing IPv4 at the socket layer must not change what
        certificate identity we validate against.
        """
        host, _, port = self.coordinator_address.rpartition(":")
        if not host:
            # no ":" in the address -- nothing to split, leave as-is
            return self.coordinator_address, self.coordinator_address
        infos = socket.getaddrinfo(host, int(port), socket.AF_INET, socket.SOCK_STREAM)
        if not infos:
            raise OSError(f"No IPv4 address found for coordinator host '{host}'")
        ipv4 = infos[0][4][0]
        return f"{ipv4}:{port}", host

    def _ensure_enrolled(self) -> None:
        """
        First-boot self-enrollment: if this node has no cert on disk
        yet, generate a keypair + CSR locally, send it to the
        coordinator's unauthenticated Enroll RPC along with the
        shared bootstrap token, and write back the signed cert + CA
        cert it returns. No-ops immediately if a cert already exists
        (every boot after the first) -- so this is safe to call
        unconditionally on every startup, not just "first ever" ones.

        Deliberately dials with grpc.insecure_channel for this one
        call: this node has no CA cert yet to verify the coordinator
        against, so there's nothing to pin TLS to on the very first
        contact. Security here comes from `enroll_token` (must match
        the coordinator's GCON_ENROLL_TOKEN) instead of transport
        verification -- standard trust-on-first-use, same trust model
        as e.g. SSH host keys on first connect. Every call after this
        one uses full mTLS as normal, verified against the CA cert
        this call just fetched and saved.
        """
        cert_path = os.path.join(self.cert_dir, f"agent-{self.node_id}.cert.pem")
        ca_cert_path = os.path.join(self.cert_dir, tls.CA_CERT_FILE)
        if os.path.exists(cert_path) and os.path.exists(ca_cert_path):
            return  # already enrolled from a previous boot -- nothing to do

        if not self.enroll_token:
            raise RuntimeError(
                f"No certificate found for node '{self.node_id}' in {self.cert_dir} "
                "and no enroll_token was provided -- pass --enroll-token / set "
                "GCON_ENROLL_TOKEN to self-enroll, or pre-provision a cert."
            )

        os.makedirs(self.cert_dir, exist_ok=True)
        key_pem, csr_pem = tls.generate_agent_csr(self.node_id)

        logger.info("No cert on disk for '%s' -- self-enrolling with coordinator at %s ...",
                    self.node_id, self.enroll_address)
        channel = grpc.insecure_channel(self.enroll_address)
        try:
            stub = pb_grpc.AgentControlStub(channel)
            response = stub.Enroll(
                pb.EnrollRequest(
                    node_id=self.node_id, enroll_token=self.enroll_token, csr_pem=csr_pem,
                ),
                timeout=15,
            )
        finally:
            channel.close()

        if not response.accepted:
            raise RuntimeError(f"Enrollment rejected by coordinator: {response.reason}")

        key_path = os.path.join(self.cert_dir, f"agent-{self.node_id}.key.pem")
        with open(key_path, "wb") as f:
            f.write(key_pem)
        os.chmod(key_path, 0o600)
        with open(cert_path, "wb") as f:
            f.write(response.cert_pem)
        with open(ca_cert_path, "wb") as f:
            f.write(response.ca_cert_pem)
        logger.info("Enrolled '%s'; cert + CA saved to %s", self.node_id, self.cert_dir)

    def _connect_and_serve(self) -> None:
        self._ensure_enrolled()
        credentials = tls.load_agent_channel_credentials(self.cert_dir, self.node_id)
        try:
            target, original_host = self._resolve_ipv4_target()
        except OSError:
            logger.warning(
                "Could not resolve an IPv4 address for %s; falling back to "
                "letting gRPC's own resolver pick (may retry IPv6).",
                self.coordinator_address,
            )
            target, original_host = self.coordinator_address, None
        options = [
            ("grpc.keepalive_time_ms", 20000),
            ("grpc.keepalive_timeout_ms", 10000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.max_send_message_length", self.config.grpc_max_message_bytes),
            ("grpc.max_receive_message_length", self.config.grpc_max_message_bytes),
        ]
        # sni_override wins when set (e.g. "bore.pub" for a coordinator
        # behind a proxy whose real hostname isn't in the cert's SAN);
        # otherwise fall back to the previous behavior of validating
        # against whatever host we actually resolved, so dialing an
        # IP literal doesn't silently change what cert identity we
        # check against.
        sni_name = self.sni_override or original_host
        if sni_name is not None:
            options.append(("grpc.ssl_target_name_override", sni_name))
        channel = grpc.secure_channel(target, credentials, options=options)
        try:
            stub = pb_grpc.AgentControlStub(channel)

            response = stub.Register(
                pb.RegisterRequest(
                    node_id=self.node_id,
                    hostname=self.hostname,
                    agent_version="1.0.0",
                    capabilities=self.capabilities,
                ),
                timeout=15,
            )
            if not response.accepted:
                raise RuntimeError(f"Registration rejected by coordinator: {response.reason}")

            session_token = response.session_token
            heartbeat_interval = response.heartbeat_interval_seconds or int(
                self.config.heartbeat_interval_seconds
            )
            logger.info("Registered as node '%s'; heartbeat every %ss", self.node_id, heartbeat_interval)

            outbound: "queue.Queue[Optional[pb.AgentEnvelope]]" = queue.Queue()
            stream_stop = threading.Event()

            def request_generator():
                while not stream_stop.is_set() and not self._stop.is_set():
                    try:
                        env = outbound.get(timeout=0.5)
                    except queue.Empty:
                        continue
                    if env is None:
                        return
                    yield env

            hb_thread = threading.Thread(
                target=self._heartbeat_loop,
                args=(outbound, session_token, heartbeat_interval, stream_stop),
                daemon=True,
            )
            hb_thread.start()

            try:
                for coordinator_envelope in stub.Control(request_generator()):
                    self._handle_coordinator_envelope(
                        coordinator_envelope, outbound, session_token, stub
                    )
                    if self._stop.is_set():
                        outbound.put(
                            pb.AgentEnvelope(
                                node_id=self.node_id,
                                session_token=session_token,
                                shutdown_notice=pb.ShutdownNotice(
                                    reason=getattr(self, "_shutdown_reason", "shutdown")
                                ),
                            )
                        )
                        break
            finally:
                stream_stop.set()
                outbound.put(None)
                hb_thread.join(timeout=2)
        finally:
            channel.close()

    def _heartbeat_loop(self, outbound, session_token, interval, stop_event) -> None:
        while not stop_event.is_set() and not self._stop.is_set():
            status = "busy" if self._active_jobs else "idle"
            snapshot = self.agent.resource_snapshot() if hasattr(self.agent, "resource_snapshot") else {}
            outbound.put(
                pb.AgentEnvelope(
                    node_id=self.node_id,
                    session_token=session_token,
                    heartbeat=pb.Heartbeat(
                        sequence=self._hb_sequence.next(),
                        status=status,
                        cpu_percent=float(snapshot.get("cpu_percent", 0.0)),
                        memory_percent=float(snapshot.get("memory_percent", 0.0)),
                        running_jobs=len(self._active_jobs),
                        timestamp=_now_iso(),
                    ),
                )
            )
            stop_event.wait(interval)

    def _handle_coordinator_envelope(self, envelope, outbound, session_token, stub) -> None:
        kind = envelope.WhichOneof("payload")
        if kind == "job_assign":
            job = envelope.job_assign
            future = self._executor.submit(
                self._run_job, job, outbound, session_token, stub
            )
            self._active_jobs[job.job_id] = future
        elif kind == "job_cancel":
            job_id = envelope.job_cancel.job_id
            if job_id in self._active_jobs and hasattr(self.agent, "cancel"):
                self.agent.cancel()
        elif kind == "ping":
            pass  # keepalive; no response payload required

    def _run_job(self, job_assign, outbound, session_token, stub) -> None:
        timeout = job_assign.timeout_seconds or None

        # metadata_json currently only ever carries staged-job info
        # ({"kind": "staged", "stages": {...}}) -- see JobAssign in
        # gcon_transport.proto. "resourced" jobs' `requires` never
        # reaches here at all: it's matched by the scheduler before a
        # node is even chosen, so the agent has no need to know about
        # it.
        stage_report_path = None
        if job_assign.metadata_json:
            try:
                job_metadata = json.loads(job_assign.metadata_json)
            except ValueError:
                job_metadata = {}
            if job_metadata.get("kind") == "staged":
                stage_report_path = os.path.join(
                    tempfile.gettempdir(),
                    f"gcon-stages-{job_assign.job_id}.jsonl",
                )

        try:
            result = self.agent.execute_job(
                job_assign.job_id,
                job_assign.command,
                timeout=timeout,
                stage_report_path=stage_report_path,
            )
        except Exception as exc:  # the execution engine is untouched and may itself
            # raise rather than return an error dict for unexpected failures;
            # the transport layer must still report *something* back so the
            # coordinator's dispatch doesn't hang until JobDispatchTimeoutError.
            logger.exception("Job '%s' raised during execution", job_assign.job_id)
            result = {
                "job_id": job_assign.job_id,
                "status": "error",
                "error": str(exc),
                "stdout": "",
                "stderr": "",
                "timestamp": _now_iso(),
            }
        finally:
            self._active_jobs.pop(job_assign.job_id, None)

        self._report_result(job_assign, result, outbound, session_token)
        self._stream_logs(job_assign, result, session_token, stub)
        # _upload_receipt() intentionally not called here anymore.
        # It independently built and uploaded a second, Ed25519-signed
        # receipt (ReceiptGenerator.generate) for every job -- separate
        # from, and incompatible with, the HMAC receipt the coordinator
        # already creates and signs itself the moment _report_result()
        # above lands (GCONCoordinator._run_job -> ExecutionVerifier.
        # create_receipt). The coordinator's verifier could never check
        # this one (different scheme, signature nested one level up
        # from where validate_proof() looks), and because it was the
        # only receipt ever persisted to the control-plane DB, it
        # silently overwrote the correct in-memory receipt on every
        # coordinator restart -- the cause of every receipt showing
        # "Unverified" / "Proof missing signature" in the dashboard.
        # _upload_receipt()/ReceiptGenerator are left in place (still
        # covered by tests) in case a real per-agent asymmetric-signing
        # design replaces the coordinator-side scheme later.

    def _report_result(self, job_assign, result, outbound, session_token) -> None:
        import json as _json

        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        # Fold in stage checkpoints (staged jobs only -- see _run_job).
        # No new proto field needed: metrics_json is already a generic
        # JSON bag, so this rides along with gpu_name/cpu_percent/etc.
        # without touching the wire schema.
        if result.get("stages"):
            metrics = dict(metrics)
            metrics["stages"] = result["stages"]
        outbound.put(
            pb.AgentEnvelope(
                node_id=self.node_id,
                session_token=session_token,
                job_result=pb.JobResult(
                    job_id=job_assign.job_id,
                    request_message_id=job_assign.request_message_id,
                    status=str(result.get("status", "unknown")),
                    return_code=int(result.get("return_code", 0) or 0),
                    runtime_seconds=float(result.get("runtime_seconds", 0.0) or 0.0),
                    stdout=str(result.get("stdout", "") or ""),
                    stderr=str(result.get("stderr", "") or ""),
                    error=str(result.get("error", "") or ""),
                    metrics_json=_json.dumps(metrics),
                    timestamp=str(result.get("timestamp", _now_iso())),
                ),
            )
        )

    def _stream_logs(self, job_assign, result, session_token, stub) -> None:
        """
        Streams the job's captured stdout/stderr to the coordinator's
        `StreamLogs` RPC. `GCONAgent.execute_job` (execution engine,
        untouched) returns output synchronously once the job has
        finished rather than yielding it incrementally, so this
        replays it as a sequence of line-chunks over the same
        client-streaming RPC a truly-incremental agent would use --
        the transport contract (idempotent, sequenced, resumable log
        chunks) is exercised either way.
        """

        def chunks():
            sequence = 0
            for stream_name, text in (("stdout", result.get("stdout") or ""),
                                       ("stderr", result.get("stderr") or "")):
                for line in text.splitlines() or [""]:
                    if line == "" and text == "":
                        continue
                    sequence += 1
                    yield pb.LogChunk(
                        node_id=self.node_id,
                        session_token=session_token,
                        job_id=job_assign.job_id,
                        attempt_id=job_assign.attempt_id,
                        stream=stream_name,
                        sequence=sequence,
                        content=line,
                    )

        chunk_list = list(chunks())
        if not chunk_list:
            return
        try:
            stub.StreamLogs(iter(chunk_list), timeout=30)
        except grpc.RpcError:
            logger.exception("Failed to stream logs for job '%s'", job_assign.job_id)

    def _upload_receipt(self, job_assign, result, session_token, stub) -> None:
        import json as _json

        try:
            receipt = ReceiptGenerator.generate(result)
        except Exception:
            logger.exception("Failed to generate receipt for job '%s'", job_assign.job_id)
            return
        try:
            stub.UploadReceipt(
                pb.ReceiptUpload(
                    node_id=self.node_id,
                    session_token=session_token,
                    job_id=job_assign.job_id,
                    attempt_id=job_assign.attempt_id,
                    receipt_hash=receipt["receipt_hash"],
                    signature=receipt.get("signature", ""),
                    payload_json=_json.dumps(receipt),
                ),
                timeout=15,
            )
        except grpc.RpcError:
            logger.exception("Failed to upload receipt for job '%s'", job_assign.job_id)


def _now_iso() -> str:
    from datetime import datetime, UTC

    return datetime.now(UTC).isoformat()
