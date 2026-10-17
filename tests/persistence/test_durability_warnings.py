"""A redeploy must not silently wipe state: the coordinator warns at startup when it would."""
import logging

import pytest

from gcon.persistence import durability


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("GCON_DATA_DIR", "GCON_DB_PATH", "GCON_CONTROL_PLANE_DB_PATH",
                 "GCON_CONTROL_PLANE_DATABASE_URL", "GCON_HMAC_KEY_PATH",
                 "GCON_SECRETS_KEY", "GCON_SECRETS_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


def _mounts(*mount_points):
    return lambda p: p in mount_points


def test_path_under_a_mount_point_is_durable():
    assert durability.on_mounted_volume("/data/gcon.db", _mounts("/data"))
    assert durability.on_mounted_volume("/data/sub/dir/gcon.db", _mounts("/data"))


def test_path_with_no_mount_point_above_it_is_not_durable():
    assert not durability.on_mounted_volume("/app/data/gcon.db", _mounts("/data"))
    assert not durability.on_mounted_volume("/app/data/gcon.db", _mounts())


def test_nothing_is_warned_outside_a_container():
    locations = durability.state_locations(None, "keys/grpc")
    assert durability.durability_warnings(locations, in_container=False) == []


def test_everything_is_warned_in_a_container_without_a_volume(monkeypatch):
    monkeypatch.setenv("GCON_DATA_DIR", "/app/data")
    locations = durability.state_locations(None, "/app/keys/grpc")
    warnings = durability.durability_warnings(locations, in_container=True, is_mount=_mounts())
    assert len(warnings) == len(locations) == 5
    assert any("identity database" in w and "API keys" in w for w in warnings)


def test_a_volume_under_data_dir_silences_only_what_it_covers(monkeypatch):
    monkeypatch.setenv("GCON_DATA_DIR", "/data")
    locations = durability.state_locations(None, "/app/keys/grpc")
    warnings = durability.durability_warnings(locations, in_container=True, is_mount=_mounts("/data"))
    flagged = " ".join(warnings)
    assert "identity database" not in flagged
    assert "control-plane database" not in flagged
    # the keys and certs default to ./keys and are NOT under /data
    assert "receipt-signing key" in flagged
    assert "webhook-secret key" in flagged
    assert "coordinator CA" in flagged


def test_postgres_control_plane_is_not_checked_as_a_local_file(monkeypatch):
    monkeypatch.setenv("GCON_CONTROL_PLANE_DATABASE_URL", "postgresql://u:p@h/db")
    labels = [label for label, _, _ in durability.state_locations(None, None)]
    assert "control-plane database" not in labels
    assert "identity database" in labels  # the identity DB stays local SQLite


def test_secrets_key_in_env_is_not_checked_as_a_file(monkeypatch):
    monkeypatch.setenv("GCON_SECRETS_KEY", "x" * 44)
    labels = [label for label, _, _ in durability.state_locations(None, None)]
    assert "webhook-secret key" not in labels


def test_explicit_control_plane_path_is_the_one_checked(monkeypatch):
    monkeypatch.setenv("GCON_DATA_DIR", "/data")
    locations = dict((label, path) for label, path, _ in durability.state_locations("/elsewhere/cp.db", None))
    assert locations["control-plane database"] == "/elsewhere/cp.db"


def test_report_logs_locations_and_warns(monkeypatch, caplog):
    monkeypatch.setenv("GCON_CONTROL_PLANE_DATABASE_URL", "postgresql://u:p@h/db")
    monkeypatch.setattr(durability, "running_in_container", lambda: True)
    monkeypatch.setattr(durability, "on_mounted_volume", lambda path, is_mount=None: False)
    logger = logging.getLogger("gcon.test_durability")
    with caplog.at_level(logging.INFO, logger="gcon.test_durability"):
        durability.log_durable_storage_report(logger, None, "/app/keys/grpc")
    text = caplog.text
    assert "identity database" in text
    assert "does NOT move with it" in text
    assert "DATA WILL NOT SURVIVE A REDEPLOY" in text
