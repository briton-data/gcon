"""
Attaching a persistent volume (Railway sets RAILWAY_VOLUME_MOUNT_PATH) is enough to keep the databases, the worker
certificate authority and the receipt signing key across redeploys. Not on Railway, nothing changes.
"""
import os

import pytest
from fastapi.testclient import TestClient

from gcon import config
from gcon.persistence.control_plane import ControlPlane

ENV = ("RAILWAY_VOLUME_MOUNT_PATH", "RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID", "GCON_DATA_DIR", "GCON_TLS_CERT_DIR",
       "GCON_HMAC_KEY_PATH", "GCON_DB_PATH", "GCON_CONTROL_PLANE_DB_PATH", "GCON_CONTROL_PLANE_DATABASE_URL")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def mounted(monkeypatch):
    """A temp dir stands in for a real mounted volume."""
    monkeypatch.setattr(os.path, "ismount", lambda p: True)


class TestDefaultsAreUnchangedOffRailway:
    def test_the_old_locations(self):
        assert config.resolve_data_dir() == "data"
        assert config.resolve_tls_cert_dir() == "keys/grpc"
        assert config.resolve_hmac_key_path() == "./keys/hmac_secret.key"
        assert config.persistent_volume_dir() is None

    def test_explicit_settings_still_win(self, monkeypatch):
        monkeypatch.setenv("GCON_DATA_DIR", "/a"); monkeypatch.setenv("GCON_TLS_CERT_DIR", "/b")
        monkeypatch.setenv("GCON_HMAC_KEY_PATH", "/c/key")
        assert (config.resolve_data_dir(), config.resolve_tls_cert_dir(), config.resolve_hmac_key_path()) == ("/a", "/b", "/c/key")


class TestVolumeBecomesTheHome:
    def test_everything_follows_the_volume(self, monkeypatch, tmp_path):
        monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path))
        assert config.resolve_data_dir() == str(tmp_path)
        assert config.resolve_tls_cert_dir() == str(tmp_path / "keys" / "grpc")
        assert config.resolve_hmac_key_path() == str(tmp_path / "keys" / "hmac_secret.key")

    def test_an_explicit_setting_beats_the_volume(self, monkeypatch, tmp_path):
        monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path)); monkeypatch.setenv("GCON_TLS_CERT_DIR", "/elsewhere")
        assert config.resolve_tls_cert_dir() == "/elsewhere"

    def test_the_transport_config_uses_it_too(self, monkeypatch, tmp_path):
        from gcon.transport.config import TransportConfig
        monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path))
        assert TransportConfig.load(None).tls_cert_dir == str(tmp_path / "keys" / "grpc")

    def test_state_survives_a_simulated_redeploy(self, monkeypatch, tmp_path):
        """Build the stack twice against the same volume (a redeploy: new process, same disk)."""
        from gcon.execution.verifier import ExecutionVerifier
        from gcon.management.management_layer import ManagementLayer
        from gcon.cluster.coordinator import GCONCoordinator
        from gcon.transport import tls
        monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path))
        monkeypatch.chdir(tmp_path.parent)                       # a different working dir each "deploy"

        def deploy():
            co = GCONCoordinator(control_plane=ControlPlane())
            mgmt = ManagementLayer(coordinator=co)
            tls.ensure_ca(config.resolve_tls_cert_dir())
            return co, mgmt

        co1, m1 = deploy()
        org_id = m1.signup_customer("Acme", "Ann", "ann@acme.test", "correct-horse-1")["organization"]["org_id"]
        key1 = ExecutionVerifier().sign_data({"x": 1})
        ca1 = open(os.path.join(config.resolve_tls_cert_dir(), "ca.crt"), "rb").read() if os.path.exists(
            os.path.join(config.resolve_tls_cert_dir(), "ca.crt")) else sorted(os.listdir(config.resolve_tls_cert_dir()))
        co1.shutdown()

        co2, m2 = deploy()                                       # "redeploy"
        try:
            assert m2.org_registry.get_organization(org_id).name == "Acme"          # identity database survived
            assert m2.customer_registry.get_user_by_email("ann@acme.test") is not None
            assert ExecutionVerifier().sign_data({"x": 1}) == key1                    # same signing key
            ca2 = open(os.path.join(config.resolve_tls_cert_dir(), "ca.crt"), "rb").read() if os.path.exists(
                os.path.join(config.resolve_tls_cert_dir(), "ca.crt")) else sorted(os.listdir(config.resolve_tls_cert_dir()))
            assert ca2 == ca1                                                         # same certificate authority
        finally:
            co2.shutdown()
        # and it all really is on the volume
        assert any(n.endswith(".db") for n in os.listdir(tmp_path))
        assert os.path.exists(tmp_path / "keys" / "hmac_secret.key") and os.path.isdir(tmp_path / "keys" / "grpc")


class TestDescribePersistence:
    def test_off_railway_it_is_fine(self):
        p = config.describe_persistence()
        assert p["durable"] and not p["on_railway"]

    def test_on_railway_without_a_volume_it_says_data_is_lost(self, monkeypatch):
        monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
        p = config.describe_persistence()
        assert not p["durable"] and "no volume is attached" in p["reason"] and "lost on every redeploy" in p["reason"]

    def test_on_railway_with_a_volume_it_is_durable(self, monkeypatch, tmp_path, mounted):
        monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production"); monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path))
        assert config.describe_persistence()["durable"]

    def test_one_path_pointed_off_the_volume_is_called_out(self, monkeypatch, tmp_path, mounted):
        monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production"); monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path))
        monkeypatch.setenv("GCON_TLS_CERT_DIR", "/tmp-not-on-the-volume")
        p = config.describe_persistence()
        assert not p["durable"] and "tls_cert_dir" in p["reason"] and "data_dir" not in p["reason"]

    def test_a_database_path_set_off_the_volume_is_called_out(self, monkeypatch, tmp_path, mounted):
        monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production"); monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path))
        monkeypatch.setenv("GCON_DB_PATH", "/not/on/the/volume/gcon.db")
        p = config.describe_persistence()
        assert not p["durable"] and "identity_db" in p["reason"] and "control_plane_db" not in p["reason"]

    def test_a_control_plane_on_postgres_has_no_local_file_to_lose(self, monkeypatch, tmp_path, mounted):
        monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production"); monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path))
        monkeypatch.setenv("GCON_CONTROL_PLANE_DATABASE_URL", "postgresql://u:p@h/db")
        p = config.describe_persistence()
        assert p["durable"] and "control_plane_db" not in p

    def test_a_volume_path_typed_in_by_hand_is_not_trusted(self, monkeypatch):
        monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production"); monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", "GCON")
        assert config.persistent_volume_dir() is None and config.resolve_data_dir() == "data"       # nothing follows it
        p = config.describe_persistence()
        assert not p["durable"] and "'GCON'" in p["reason"] and "not an absolute path" in p["reason"] and "delete it" in p["reason"]

    def test_an_absolute_path_with_nothing_mounted_is_not_durable(self, monkeypatch, tmp_path):
        monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production"); monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path))
        p = config.describe_persistence()                                   # a plain folder is not a mount point
        assert not p["durable"] and "nothing is mounted there" in p["reason"]

    def test_the_dashboard_route_reports_it(self, monkeypatch, tmp_path):
        from gcon.cluster.coordinator import GCONCoordinator
        from gcon.dashboard.presentation import PresentationLayer
        from gcon.dashboard.web_server import WebServer
        monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db")); monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
        monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
        co = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
        try:
            with TestClient(WebServer(PresentationLayer(co)).app) as client:
                from tests.integration.smoke_management_gaps import OWNER_EMAIL
                assert client.get("/management/persistence").status_code in (401, 403)
                client.post("/auth/login", json={"email": OWNER_EMAIL, "password": "owner-pw-123"})
                body = client.get("/management/persistence").json()
                assert body["on_railway"] and not body["durable"]
        finally:
            co.shutdown()
