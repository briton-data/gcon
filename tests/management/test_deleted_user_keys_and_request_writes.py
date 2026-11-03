"""
  K1  Deleting a staff user ends their API keys and sessions. Before, the keys stayed Active and,
      because the owner no longer resolved, kept authenticating as an owner-less key.
  W1  An authenticated API request no longer costs a durable (fsync'd) write per request: usage
      counters stay exact in memory and reach disk at most once per interval per key / user.
"""
import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.persistence import ControlPlane


@pytest.fixture
def mgmt(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    yield server.management
    coordinator.shutdown()


def _staff_user_with_key(m, email="dev@gcon.example"):
    user = m.create_user("Dev", email, "Developer", password="dev-pw-12345")
    key = m.create_api_key("ci", user["user_id"], scopes=None)
    return user["user_id"], key


class TestDeletedUsersKeysStopWorking:
    def test_keys_of_a_deleted_staff_user_no_longer_authenticate(self, mgmt):
        user_id, key = _staff_user_with_key(mgmt)
        assert mgmt.authenticate_api_key(key["secret"])[0].key_id == key["key_id"]

        mgmt.delete_user(user_id)

        with pytest.raises(ValueError):
            mgmt.authenticate_api_key(key["secret"])
        assert mgmt.api_key_manager.get_key(key["key_id"]).status == "Revoked"

    def test_only_that_users_keys_are_revoked(self, mgmt):
        gone_id, _ = _staff_user_with_key(mgmt, "gone@gcon.example")
        _, kept = _staff_user_with_key(mgmt, "kept@gcon.example")
        mgmt.delete_user(gone_id)
        assert mgmt.authenticate_api_key(kept["secret"])[0].key_id == kept["key_id"]

    def test_a_deleted_users_sessions_end(self, mgmt):
        user = mgmt.create_user("Dev", "dev@gcon.example", "Developer", password="dev-pw-12345")
        token, _ = mgmt.login("dev@gcon.example", "dev-pw-12345")
        assert mgmt.session_manager.get_user_id(token) == user["user_id"]
        mgmt.delete_user(user["user_id"])
        assert mgmt.session_manager.get_user_id(token) is None

    def test_a_key_whose_owner_vanished_by_other_means_is_refused(self, mgmt):
        """Defence in depth: even if some other path removes the owner, the key must not
        authenticate as an owner-less key."""
        user_id, key = _staff_user_with_key(mgmt)
        del mgmt.user_registry.users[user_id]
        with pytest.raises(ValueError):
            mgmt.authenticate_api_key(key["secret"])


class TestPerRequestWrites:
    def _count_commits(self, mgmt, monkeypatch):
        calls = {"n": 0}
        db = mgmt.api_key_manager.db
        real = db.transaction

        def counting():
            calls["n"] += 1
            return real()

        monkeypatch.setattr(db, "transaction", counting)
        return calls

    def test_a_burst_of_requests_does_not_write_each_time(self, mgmt, monkeypatch):
        user_id, key = _staff_user_with_key(mgmt)
        mgmt.authenticate_api_key(key["secret"])           # first use is written
        calls = self._count_commits(mgmt, monkeypatch)
        for _ in range(60):
            mgmt.authenticate_api_key(key["secret"])
        assert calls["n"] < 10, f"{calls['n']} durable writes for 60 requests"

    def test_counts_stay_exact_in_memory(self, mgmt):
        user_id, key = _staff_user_with_key(mgmt)
        for _ in range(25):
            mgmt.authenticate_api_key(key["secret"])
        assert mgmt.api_key_manager.get_key(key["key_id"]).usage_count == 25
        assert mgmt.user_registry.get_user(user_id).stats["api_requests"] == 25

    def test_the_interval_can_be_set_to_zero_to_write_every_time(self, mgmt, monkeypatch):
        monkeypatch.setenv("GCON_USAGE_WRITE_INTERVAL_SECONDS", "0")
        mgmt.api_key_manager._usage_throttle.interval = 0
        mgmt._usage_throttle.interval = 0
        user_id, key = _staff_user_with_key(mgmt)
        mgmt.authenticate_api_key(key["secret"])
        calls = self._count_commits(mgmt, monkeypatch)
        for _ in range(5):
            mgmt.authenticate_api_key(key["secret"])
        assert calls["n"] >= 5

    def test_what_was_written_survives_a_restart(self, mgmt, tmp_path):
        user_id, key = _staff_user_with_key(mgmt)
        mgmt.authenticate_api_key(key["secret"])             # first use reaches disk
        from gcon.management.api_keys import APIKeyManager
        from gcon.storage.database import Database
        reopened = APIKeyManager(Database(str(tmp_path / "gcon.db")))
        stored = reopened.get_key(key["key_id"])
        assert stored.usage_count >= 1 and stored.last_used_at is not None
