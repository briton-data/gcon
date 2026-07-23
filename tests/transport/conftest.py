"""
Shared fixtures for transport-layer tests. Every test here exercises
the *real* gRPC server and a *real* TLS-secured client channel over
localhost -- there is no mock networking or simulated transport
anywhere in this suite, per the engineering rules for this task.
"""

import socket
import time

import pytest

from gcon.persistence.control_plane import ControlPlane
from gcon.transport.config import TransportConfig
from gcon.transport.grpc_transport import GrpcTransport


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


@pytest.fixture
def cert_dir(tmp_path):
    d = tmp_path / "certs"
    d.mkdir()
    return str(d)


@pytest.fixture
def control_plane(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "control_plane.db"))
    yield plane
    plane.close()


@pytest.fixture
def running_transport(control_plane, cert_dir):
    port = free_tcp_port()
    control_plane.settings.set("grpc_port", str(port))
    control_plane.settings.set("tls_cert_dir", cert_dir)
    control_plane.settings.set("heartbeat_interval_seconds", "1")
    config = TransportConfig.load(control_plane)
    transport = GrpcTransport(control_plane=control_plane, config=config)
    transport.start()
    yield transport, f"localhost:{port}"
    transport.shutdown(grace_period=3)


def wait_until(predicate, timeout=5.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()
