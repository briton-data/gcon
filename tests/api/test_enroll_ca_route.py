"""GET /enroll/ca -- public, so a website can put the CA certificate into a
worker's join command and the customer never handles a CA file."""
import os

import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane
from gcon.transport import tls


@pytest.fixture
def env(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "cp.db"))
    cert_dir = str(tmp_path / "certs")
    os.makedirs(cert_dir)
    plane.settings.set("tls_cert_dir", cert_dir)
    coordinator = GCONCoordinator(control_plane=plane)
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    yield TestClient(create_api_v1_app(management, PresentationLayer(coordinator))), cert_dir
    coordinator.shutdown()


def test_returns_the_ca_and_its_fingerprint_without_authentication(env):
    client, cert_dir = env
    tls.ensure_ca(cert_dir)
    r = client.get("/enroll/ca")
    assert r.status_code == 200
    body = r.json()
    assert body["sha256_fingerprint"] == tls.cert_fingerprint(os.path.join(cert_dir, tls.CA_CERT_FILE))
    assert body["ca_cert_pem"].startswith("-----BEGIN CERTIFICATE-----")
    assert "PRIVATE KEY" not in r.text
    # what the website embeds must install cleanly on a worker
    tls.install_ca(os.path.join(cert_dir, "worker"), body["ca_cert_b64"], body["sha256_fingerprint"])


def test_503_when_the_coordinator_has_no_ca_yet(env):
    client, _ = env
    assert client.get("/enroll/ca").status_code == 503
