"""create_user(password=), change_password and admin set_password changed the
password only in memory: after a restart a new user could not log in and a
changed password reverted to the old one."""
import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.management.management_layer import ManagementLayer


@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "m.db")


def _layer(db):
    return ManagementLayer(coordinator=GCONCoordinator(), db_path=db)


def _restart(db):
    return _layer(db)       # a fresh process reading the same database


def test_a_password_given_at_creation_survives_a_restart(db):
    uid = _layer(db).create_user("Ann", "ann@gcon.example", role="Viewer", password="first-pass-1")["user_id"]
    assert _restart(db).user_registry.get_user(uid).check_password("first-pass-1")


def test_a_changed_password_survives_a_restart(db):
    layer = _layer(db)
    uid = layer.create_user("Ann", "ann@gcon.example", role="Viewer", password="first-pass-1")["user_id"]
    layer.change_password(uid, "first-pass-1", "second-pass-2")
    user = _restart(db).user_registry.get_user(uid)
    assert user.check_password("second-pass-2") and not user.check_password("first-pass-1")


def test_an_admin_set_password_survives_a_restart(db):
    layer = _layer(db)
    uid = layer.create_user("Ann", "ann@gcon.example", role="Viewer")["user_id"]
    layer.set_password(uid, "admin-set-3")
    assert _restart(db).user_registry.get_user(uid).check_password("admin-set-3")


class TestResetTokenIsNotLogged:
    def _request(self, db, caplog, monkeypatch, expose):
        import logging
        if expose:
            monkeypatch.setenv("GCON_EXPOSE_RESET_TOKEN", "1")
        else:
            monkeypatch.delenv("GCON_EXPOSE_RESET_TOKEN", raising=False)
        layer = _layer(db)
        layer.create_user("Ann", "ann@gcon.example", role="Viewer", password="first-pass-1")
        caplog.clear()
        with caplog.at_level(logging.DEBUG):
            layer.request_password_reset("ann@gcon.example")
        return layer

    def test_the_token_never_reaches_the_log_by_default(self, db, caplog, monkeypatch):
        layer = self._request(db, caplog, monkeypatch, expose=False)
        assert "Password reset requested" in caplog.text
        assert "/reset-password?token=" not in caplog.text and "Reset token" not in caplog.text

    def test_it_is_logged_only_with_the_development_switch(self, db, caplog, monkeypatch):
        self._request(db, caplog, monkeypatch, expose=True)
        assert "/reset-password?token=" in caplog.text
