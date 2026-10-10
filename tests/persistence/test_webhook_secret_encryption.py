"""Webhook signing secrets must not sit in the database in plain text."""
import pytest

from gcon.persistence import ControlPlane, secret_box


@pytest.fixture(autouse=True)
def key_file(tmp_path, monkeypatch):
    monkeypatch.delenv("GCON_SECRETS_KEY", raising=False)
    monkeypatch.setenv("GCON_SECRETS_KEY_PATH", str(tmp_path / "secrets.key"))


def _raw(cp, table, column="secret"):
    return [r[column] for r in cp.db.query(f"SELECT {column} FROM {table}")]


def test_subscription_secret_is_encrypted_in_the_row_and_readable_through_the_repository(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    sub = cp.webhooks.create_subscription("org1", "https://example.com/h", ["JOB_COMPLETED"], secret="s3cret-value-123")
    assert sub["secret"] == "s3cret-value-123"
    raw = _raw(cp, "webhook_subscriptions")
    assert raw and all(secret_box.is_sealed(v) and "s3cret-value-123" not in v for v in raw)
    assert cp.webhooks.list_for_org("org1")[0]["secret"] == "s3cret-value-123"


def test_delivery_copy_of_the_secret_is_encrypted_too_and_still_signs(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    d = cp.webhooks.enqueue_delivery("JOB_COMPLETED", {"a": 1}, "https://example.com/h", "deliv-secret-999", org_id="org1")
    assert d["secret"] == "deliv-secret-999"
    assert all(secret_box.is_sealed(v) for v in _raw(cp, "webhook_deliveries"))
    assert cp.webhooks.due_deliveries("9999-01-01")[0]["secret"] == "deliv-secret-999"


def test_plaintext_secrets_from_before_are_encrypted_on_the_next_start(tmp_path):
    path = str(tmp_path / "cp.db")
    cp = ControlPlane(path=path)
    cp.db.execute(
        "INSERT INTO webhook_subscriptions (subscription_id, org_id, url, secret, event_types_json, active, created_at) "
        "VALUES ('legacy', 'org1', 'https://example.com/h', 'old-plain-secret', '[]', 1, 'now')")
    assert cp.webhooks.get_subscription("legacy")["secret"] == "old-plain-secret"      # still readable
    cp2 = ControlPlane(path=path)                                                    # next start
    assert all(secret_box.is_sealed(v) for v in _raw(cp2, "webhook_subscriptions"))
    assert cp2.webhooks.get_subscription("legacy")["secret"] == "old-plain-secret"


def test_the_key_comes_from_the_environment_when_set(tmp_path, monkeypatch):
    from cryptography.fernet import Fernet
    monkeypatch.setenv("GCON_SECRETS_KEY", Fernet.generate_key().decode())
    assert secret_box.open_(secret_box.seal("x")) == "x"
    assert not (tmp_path / "secrets.key").exists()


def test_a_different_key_cannot_read_the_secret(tmp_path, monkeypatch):
    from cryptography.fernet import Fernet
    sealed = secret_box.seal("x")
    monkeypatch.setenv("GCON_SECRETS_KEY", Fernet.generate_key().decode())
    with pytest.raises(secret_box.SecretDecryptionError):
        secret_box.open_(sealed)


def test_the_generated_key_file_is_owner_only(tmp_path):
    secret_box.seal("x")
    import os, stat
    assert stat.S_IMODE(os.stat(tmp_path / "secrets.key").st_mode) == 0o600
