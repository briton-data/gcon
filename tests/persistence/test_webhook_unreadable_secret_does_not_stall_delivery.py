"""
A webhook delivery whose stored signing secret can no longer be decrypted (the secrets key was lost or changed, for
example a redeploy without a volume) must fail on its own. Before, reading it raised from inside the query that
loads every due delivery, so the dispatcher failed on every tick and NO customer's webhooks were delivered.
"""
import pytest
from cryptography.fernet import Fernet

from gcon.events.event_types import EventType
from gcon.persistence import ControlPlane
from gcon.transport import webhooks as webhooks_mod
from gcon.transport.webhooks import WebhookDispatcher


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, event):
        self.events.append(event)


@pytest.fixture(autouse=True)
def key(tmp_path, monkeypatch):
    monkeypatch.delenv("GCON_SECRETS_KEY", raising=False)
    monkeypatch.setenv("GCON_SECRETS_KEY_PATH", str(tmp_path / "secrets.key"))


@pytest.fixture
def delivered(monkeypatch):
    sent = []

    class _Resp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake_urlopen(req, timeout=None):
        sent.append(req.full_url)
        return _Resp()

    monkeypatch.setattr(webhooks_mod, "safe_urlopen", fake_urlopen)
    return sent


def _two_deliveries(tmp_path, monkeypatch):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    cp.webhooks.enqueue_delivery("JOB_COMPLETED", {"n": 1}, "https://old.example.com/h", "old-secret", org_id="org-old")
    # The secrets key is lost: a new one takes its place.
    monkeypatch.setenv("GCON_SECRETS_KEY", Fernet.generate_key().decode())
    cp.webhooks.enqueue_delivery("JOB_COMPLETED", {"n": 2}, "https://new.example.com/h", "new-secret", org_id="org-new")
    return cp


def test_one_unreadable_delivery_does_not_stop_the_others(tmp_path, monkeypatch, delivered):
    cp = _two_deliveries(tmp_path, monkeypatch)
    dispatcher = WebhookDispatcher(cp, event_bus=_Bus())
    dispatcher.drain_due()
    assert delivered == ["https://new.example.com/h"]


def test_the_unreadable_one_is_marked_failed_with_the_reason_and_announced(tmp_path, monkeypatch, delivered):
    cp = _two_deliveries(tmp_path, monkeypatch)
    bus = _Bus()
    WebhookDispatcher(cp, event_bus=bus).drain_due()
    rows = cp.db.query("SELECT url, status, last_error FROM webhook_deliveries ORDER BY created_at")
    old = next(r for r in rows if r["url"].startswith("https://old"))
    assert old["status"] == "failed" and "secret" in old["last_error"].lower()
    assert [e.event_type for e in bus.events] == [EventType.WEBHOOK_DELIVERY_FAILED]
    assert "old-secret" not in repr(bus.events[0].payload)


def test_it_is_not_retried_forever(tmp_path, monkeypatch, delivered):
    cp = _two_deliveries(tmp_path, monkeypatch)
    dispatcher = WebhookDispatcher(cp, event_bus=_Bus())
    dispatcher.drain_due()
    delivered.clear()
    assert dispatcher.drain_due() == 0
    assert delivered == []


def test_readable_deliveries_are_unaffected(tmp_path, delivered):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    cp.webhooks.enqueue_delivery("JOB_COMPLETED", {"n": 1}, "https://ok.example.com/h", "fine", org_id="o")
    assert WebhookDispatcher(cp, event_bus=_Bus()).drain_due() == 1
    assert delivered == ["https://ok.example.com/h"]
