import os

import pytest

from gcon.persistence.control_plane import ControlPlane
from gcon.transport.config import ConfigResolver, TransportConfig


@pytest.fixture
def cp(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "cp.db"))
    yield plane
    plane.close()


def test_default_used_when_nothing_else_set(cp):
    resolver = ConfigResolver(cp)
    assert resolver.get("grpc_port") == "50051"


def test_db_setting_overrides_default(cp):
    cp.settings.set("grpc_port", "60000")
    resolver = ConfigResolver(cp)
    assert resolver.get("grpc_port") == "60000"


def test_env_var_overrides_db_setting(cp, monkeypatch):
    cp.settings.set("grpc_port", "60000")
    monkeypatch.setenv("GCON_GRPC_PORT", "61234")
    resolver = ConfigResolver(cp)
    assert resolver.get("grpc_port") == "61234"


def test_env_var_overrides_default_with_no_db(monkeypatch):
    monkeypatch.setenv("GCON_GRPC_PORT", "61234")
    resolver = ConfigResolver(control_plane=None)
    assert resolver.get("grpc_port") == "61234"


def test_unknown_key_raises(cp):
    resolver = ConfigResolver(cp)
    with pytest.raises(KeyError):
        resolver.get("not_a_real_setting")


def test_transport_config_load_snapshot(cp, monkeypatch):
    cp.settings.set("heartbeat_interval_seconds", "9")
    monkeypatch.setenv("GCON_GRPC_PORT", "55555")
    config = TransportConfig.load(cp)
    assert config.grpc_port == 55555
    assert config.heartbeat_interval_seconds == 9.0
    # default, untouched by either env or db
    assert config.reconnect_initial_backoff_seconds == 1.0
