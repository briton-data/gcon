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

import logging
import queue
import socket
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
    ):
        self.node_id = node_id
        self.coordinator_address = coordinator_address
        self.cert_dir = cert_dir
        self.agent = agent or GCONAgent(node_id=node_id)
        self.hostname = hostname or socket.gethostname()
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
    def _connect_and_serve(self) -> None:
        credentials = tls.load_agent_channel_credentials(self.cert_dir, self.node_id)
        channel = grpc.secure_channel(
            self.coordinator_address,
            credentials,
            options=[
                ("grpc.keepalive_time_ms", 20000),
                ("grpc.keepalive_timeout_ms", 10000),
                ("grpc.keepalive_permit_without_calls", 1),
                ("grpc.max_send_message_length", self.config.grpc_max_message_bytes),
                ("grpc.max_receive_message_length", self.config.grpc_max_message_bytes),
            ],
        )
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
        try:
            result = self.agent.execute_job(job_assign.job_id, job_assign.command, timeout=timeout)
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
        self._upload_receipt(job_assign, result, session_token, stub)

    def _report_result(self, job_assign, result, outbound, session_token) -> None:
        import json as _json

        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
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
