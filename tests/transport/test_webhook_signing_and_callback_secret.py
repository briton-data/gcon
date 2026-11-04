"""
Webhook signatures and the secret behind a job's callback_url.

  T1  Every delivery attempt carries a timestamp that is part of what is signed, so a captured delivery cannot be
      replayed later: a receiver rejects one whose timestamp is stale. The original body-only signature is still
      sent (it can be switched off) so existing receivers keep working while they move to the new one.
  C1  A callback_url used to be signed with a random per-delivery secret that only GCON knew, so the receiver could
      not verify anything. An organization now has a callback signing secret it can read once and rotate, and its
      callback_url deliveries are signed with it.
"""
import hashlib
import hmac
import json
import threading
import time

import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence import ControlPlane, secret_box
from gcon.transport import webhooks as webhooks_mod
from gcon.transport.webhooks import WebhookDispatcher, dispatch_job_event


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    monkeypatch.delenv("GCON_SECRETS_KEY", raising=False)
    monkeypatch.setenv("GCON_SECRETS_KEY_PATH", str(tmp_path / "secrets.key"))
    monkeypatch.delenv("GCON_WEBHOOK_LEGACY_SIGNATURE", raising=False)
    monkeypatch.delenv("GCON_REQUIRE_CALLBACK_SECRET", raising=False)


class _Sent:
    """Captures what the dispatcher POSTs instead of touching the network."""

    def __init__(self, monkeypatch, statuses=(200,)):
        self.requests = []
        self._statuses = list(statuses)
        sent = self

        class _Resp:
            def __init__(self, status): self.status = status
            def __enter__(self): return self
            def __exit__(self, *a): return False

        def fake(req, timeout=None):
            sent.requests.append({
                "url": req.full_url, "body": req.data,
                "headers": {k.lower(): v for k, v in req.header_items()},
                "at": time.time(),
            })
            status = sent._statuses.pop(0) if sent._statuses else 200
            return _Resp(status)

        monkeypatch.setattr(webhooks_mod, "safe_urlopen", fake)


@pytest.fixture
def cp(tmp_path):
    return ControlPlane(path=str(tmp_path / "cp.db"))


def _job(callback_url="https://hooks.example.com/h", org="acme"):
    return {"job_id": "job_1", "org_id": org, "status": "completed", "completed_at": "2026-11-03T10:00:00+00:00",
            "result": {"status": "completed"}, "callback_url": callback_url}


def _v2(secret, ts, body):
    return "sha256=" + hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()


# ------------------------------------------------------------------ T1
class TestTimestampedSignature:
    def _deliver(self, cp, monkeypatch, secret="s3cret", statuses=(200,)):
        sent = _Sent(monkeypatch, statuses)
        cp.webhooks.enqueue_delivery("JOB_COMPLETED", {"job_id": "j"}, "https://hooks.example.com/h", secret, org_id="acme")
        d = WebhookDispatcher(cp, event_bus=None)
        d.drain_due()
        return sent, d

    def test_each_delivery_has_a_current_timestamp_that_is_signed(self, cp, monkeypatch):
        before = int(time.time())
        sent, _ = self._deliver(cp, monkeypatch)
        h, body = sent.requests[0]["headers"], sent.requests[0]["body"]
        ts = int(h["x-gcon-timestamp"])
        assert before - 2 <= ts <= int(time.time()) + 2
        assert h["x-gcon-signature-v2"] == _v2("s3cret", h["x-gcon-timestamp"], body)

    def test_the_original_signature_is_still_sent_unchanged(self, cp, monkeypatch):
        sent, _ = self._deliver(cp, monkeypatch)
        h, body = sent.requests[0]["headers"], sent.requests[0]["body"]
        assert h["x-gcon-signature"] == "sha256=" + hmac.new(b"s3cret", body, hashlib.sha256).hexdigest()

    @pytest.mark.parametrize("off", ["0", "false", "no"])
    def test_the_original_signature_can_be_switched_off(self, cp, monkeypatch, off):
        monkeypatch.setenv("GCON_WEBHOOK_LEGACY_SIGNATURE", off)
        sent, _ = self._deliver(cp, monkeypatch)
        h = sent.requests[0]["headers"]
        assert "x-gcon-signature" not in h and "x-gcon-signature-v2" in h

    def test_a_retry_is_signed_with_a_fresh_timestamp(self, cp, monkeypatch):
        sent, d = self._deliver(cp, monkeypatch, statuses=(500, 200))
        cp.db.execute("UPDATE webhook_deliveries SET next_attempt_at = ?", ("2000-01-01T00:00:00+00:00",))
        time.sleep(1.1)
        d.drain_due()
        assert len(sent.requests) == 2
        t1, t2 = (int(r["headers"]["x-gcon-timestamp"]) for r in sent.requests)
        assert t2 > t1
        assert sent.requests[1]["headers"]["x-gcon-signature-v2"] == _v2("s3cret", str(t2), sent.requests[1]["body"])

    def test_delivery_id_and_event_headers_are_unchanged(self, cp, monkeypatch):
        sent, _ = self._deliver(cp, monkeypatch)
        h = sent.requests[0]["headers"]
        assert h["x-gcon-event"] == "JOB_COMPLETED" and h["x-gcon-delivery"]


class TestVerifySignature:
    SECRET, BODY = "s3cret", b'{"job_id":"j"}'

    def _good(self, ts):
        return _v2(self.SECRET, ts, self.BODY)

    def test_a_good_signature_is_accepted(self):
        from gcon.transport.webhooks import verify_signature
        ts = str(int(time.time()))
        assert verify_signature(self.SECRET, self.BODY, ts, self._good(ts))

    def test_a_tampered_body_is_refused(self):
        from gcon.transport.webhooks import verify_signature
        ts = str(int(time.time()))
        assert not verify_signature(self.SECRET, self.BODY + b" ", ts, self._good(ts))

    def test_the_wrong_secret_is_refused(self):
        from gcon.transport.webhooks import verify_signature
        ts = str(int(time.time()))
        assert not verify_signature("other", self.BODY, ts, self._good(ts))

    def test_a_replayed_delivery_is_refused_once_it_is_stale(self):
        from gcon.transport.webhooks import verify_signature
        old = str(int(time.time()) - 3600)
        assert not verify_signature(self.SECRET, self.BODY, old, self._good(old))

    def test_a_timestamp_from_the_future_is_refused(self):
        from gcon.transport.webhooks import verify_signature
        future = str(int(time.time()) + 3600)
        assert not verify_signature(self.SECRET, self.BODY, future, self._good(future))

    def test_the_timestamp_cannot_be_changed_without_breaking_the_signature(self):
        from gcon.transport.webhooks import verify_signature
        old = str(int(time.time()) - 3600)
        fresh = str(int(time.time()))
        assert not verify_signature(self.SECRET, self.BODY, fresh, self._good(old))

    @pytest.mark.parametrize("ts,sig", [("", ""), ("abc", "sha256=00"), (None, None), ("12", "nope")])
    def test_garbage_is_refused_not_raised(self, ts, sig):
        from gcon.transport.webhooks import verify_signature
        assert verify_signature(self.SECRET, self.BODY, ts, sig) is False

    def test_the_tolerance_can_be_set(self):
        from gcon.transport.webhooks import verify_signature
        ts = str(int(time.time()) - 100)
        assert verify_signature(self.SECRET, self.BODY, ts, self._good(ts), tolerance=300)
        assert not verify_signature(self.SECRET, self.BODY, ts, self._good(ts), tolerance=30)


# ------------------------------------------------------------------ C1
class TestOrganizationCallbackSecret:
    def test_rotating_returns_a_secret_and_it_is_stored_sealed(self, cp):
        from gcon.transport import callback_secrets
        out = callback_secrets.rotate(cp, "acme")
        assert len(out["secret"]) >= 32 and out["created_at"]
        raw = cp.db.query("SELECT value FROM settings WHERE key LIKE 'callback_secret:%'")[0]["value"]
        assert out["secret"] not in raw
        assert callback_secrets.get(cp, "acme") == out["secret"]

    def test_status_never_contains_the_secret(self, cp):
        from gcon.transport import callback_secrets
        assert callback_secrets.status(cp, "acme") == {"configured": False, "created_at": None}
        out = callback_secrets.rotate(cp, "acme")
        status = callback_secrets.status(cp, "acme")
        assert status["configured"] is True and out["secret"] not in json.dumps(status)

    def test_rotating_again_replaces_it(self, cp):
        from gcon.transport import callback_secrets
        first = callback_secrets.rotate(cp, "acme")["secret"]
        second = callback_secrets.rotate(cp, "acme")["secret"]
        assert first != second and callback_secrets.get(cp, "acme") == second

    def test_organizations_have_their_own(self, cp):
        from gcon.transport import callback_secrets
        a = callback_secrets.rotate(cp, "acme")["secret"]
        g = callback_secrets.rotate(cp, "globex")["secret"]
        assert a != g and callback_secrets.get(cp, "acme") == a

    def test_a_secret_that_cannot_be_decrypted_reads_as_not_configured_and_never_raises(self, cp, monkeypatch):
        from cryptography.fernet import Fernet
        from gcon.transport import callback_secrets
        callback_secrets.rotate(cp, "acme")
        monkeypatch.setenv("GCON_SECRETS_KEY", Fernet.generate_key().decode())
        assert callback_secrets.get(cp, "acme") is None


class TestCallbackDeliveriesAreSignedWithIt:
    def test_a_callback_url_delivery_verifies_with_the_organizations_secret(self, cp, monkeypatch):
        from gcon.transport import callback_secrets
        from gcon.transport.webhooks import verify_signature
        secret = callback_secrets.rotate(cp, "acme")["secret"]
        sent = _Sent(monkeypatch)
        dispatch_job_event(cp, _job(), "JOB_COMPLETED")
        WebhookDispatcher(cp, event_bus=None).drain_due()
        h, body = sent.requests[0]["headers"], sent.requests[0]["body"]
        assert verify_signature(secret, body, h["x-gcon-timestamp"], h["x-gcon-signature-v2"])

    def test_another_organizations_secret_does_not_verify_it(self, cp, monkeypatch):
        from gcon.transport import callback_secrets
        from gcon.transport.webhooks import verify_signature
        callback_secrets.rotate(cp, "acme")
        other = callback_secrets.rotate(cp, "globex")["secret"]
        sent = _Sent(monkeypatch)
        dispatch_job_event(cp, _job(org="acme"), "JOB_COMPLETED")
        WebhookDispatcher(cp, event_bus=None).drain_due()
        h, body = sent.requests[0]["headers"], sent.requests[0]["body"]
        assert not verify_signature(other, body, h["x-gcon-timestamp"], h["x-gcon-signature-v2"])

    def test_a_rotation_applies_to_later_deliveries_only(self, cp, monkeypatch):
        from gcon.transport import callback_secrets
        old = callback_secrets.rotate(cp, "acme")["secret"]
        dispatch_job_event(cp, _job(), "JOB_COMPLETED")
        new = callback_secrets.rotate(cp, "acme")["secret"]
        job2 = dict(_job(), job_id="job_2")
        dispatch_job_event(cp, job2, "JOB_COMPLETED")
        secrets_in_queue = {d["job_id"]: d["secret"] for d in cp.webhooks.due_deliveries("9999-01-01T00:00:00+00:00")}
        assert secrets_in_queue == {"job_1": old, "job_2": new}

    def test_without_an_organization_secret_delivery_still_happens_as_before(self, cp, monkeypatch):
        sent = _Sent(monkeypatch)
        dispatch_job_event(cp, _job(), "JOB_COMPLETED")
        WebhookDispatcher(cp, event_bus=None).drain_due()
        assert len(sent.requests) == 1 and "x-gcon-signature-v2" in sent.requests[0]["headers"]

    def test_standing_subscriptions_keep_their_own_secret(self, cp, monkeypatch):
        from gcon.transport import callback_secrets
        callback_secrets.rotate(cp, "acme")
        sub = cp.webhooks.create_subscription(org_id="acme", url="https://sub.example.com/h", event_types=["JOB_COMPLETED"])
        dispatch_job_event(cp, _job(callback_url=None), "JOB_COMPLETED")
        due = cp.webhooks.due_deliveries("9999-01-01T00:00:00+00:00")
        assert [d["secret"] for d in due] == [sub["secret"]]


# ------------------------------------------------------------------ C1 through the API
@pytest.fixture
def api(tmp_path):
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "m.db"))
    coordinator.org_worker_gate_resolver = None
    coordinator.pause_scheduler()
    keys, keys_org = {}, {}
    for name in ("acme", "globex"):
        org = management.create_organization(f"{name} Inc")
        keys_org[name] = org["org_id"]
        user = management.create_user(f"{name} user", f"u@{name}.example", role="Owner", organization_id=org["org_id"])
        keys[name] = management.create_api_key(f"{name}-k", user["user_id"], scopes=["Submit workflows", "View monitoring"])["secret"]
    staff = management.create_user("Staff", "s@gcon.example", role="Owner", organization_id=None)
    keys["staff"] = management.create_api_key("staff-k", staff["user_id"], scopes=["Submit workflows"])["secret"]
    # a key that can only look: it must not be able to read or rotate the signing secret
    viewer = management.create_user("Viewer", "viewer@acme.example", role="Owner", organization_id=keys_org["acme"])
    keys["viewer"] = management.create_api_key("viewer-k", viewer["user_id"], scopes=["View monitoring"])["secret"]
    yield TestClient(create_api_v1_app(management, PresentationLayer(coordinator))), coordinator, keys
    coordinator.shutdown()


def _h(key):
    return {"X-API-Key": key}


class TestCallbackSecretEndpoints:
    def test_creating_one_returns_it_once_and_reading_never_does(self, api):
        client, _, keys = api
        before = client.get("/callback-secret", headers=_h(keys["acme"])).json()
        assert before == {"configured": False, "created_at": None}
        made = client.post("/callback-secret", headers=_h(keys["acme"]))
        assert made.status_code == 200 and len(made.json()["secret"]) >= 32
        after = client.get("/callback-secret", headers=_h(keys["acme"])).json()
        assert after["configured"] is True and made.json()["secret"] not in json.dumps(after)

    def test_posting_again_rotates_it(self, api):
        client, _, keys = api
        a = client.post("/callback-secret", headers=_h(keys["acme"])).json()["secret"]
        b = client.post("/callback-secret", headers=_h(keys["acme"])).json()["secret"]
        assert a != b

    def test_organizations_are_separate(self, api):
        client, _, keys = api
        client.post("/callback-secret", headers=_h(keys["acme"]))
        assert client.get("/callback-secret", headers=_h(keys["globex"])).json()["configured"] is False

    def test_a_key_with_no_organization_is_refused(self, api):
        client, _, keys = api
        # The API's rule for customer routes: a key that belongs to no organization gets 403.
        assert client.post("/callback-secret", headers=_h(keys["staff"])).status_code == 403
        assert client.get("/callback-secret", headers=_h(keys["staff"])).status_code == 403

    def test_no_key_is_refused(self, api):
        client, _, _ = api
        assert client.post("/callback-secret").status_code == 401

    def test_a_key_without_the_submit_scope_is_refused(self, api):
        client, _, keys = api
        # The API's rule: a key without the scope fails authentication (401).
        assert client.post("/callback-secret", headers=_h(keys["viewer"])).status_code == 401
        assert client.get("/callback-secret", headers=_h(keys["viewer"])).status_code == 401


class TestRequireTheSecretSwitch:
    BODY = {"command": 'python3 -c "print(1)"', "callback_url": "https://hooks.example.com/h"}

    def test_off_by_default_callback_urls_work_without_a_secret(self, api):
        client, _, keys = api
        assert client.post("/jobs", json=self.BODY, headers=_h(keys["acme"])).status_code == 200

    def test_when_required_a_callback_url_without_a_secret_is_refused_with_the_way_out(self, api, monkeypatch):
        client, coordinator, keys = api
        monkeypatch.setenv("GCON_REQUIRE_CALLBACK_SECRET", "1")
        r = client.post("/jobs", json=self.BODY, headers=_h(keys["acme"]))
        assert r.status_code == 400 and "/callback-secret" in r.json()["detail"]
        assert not coordinator.jobs

    def test_when_required_it_works_once_a_secret_exists(self, api, monkeypatch):
        client, _, keys = api
        monkeypatch.setenv("GCON_REQUIRE_CALLBACK_SECRET", "1")
        client.post("/callback-secret", headers=_h(keys["acme"]))
        assert client.post("/jobs", json=self.BODY, headers=_h(keys["acme"])).status_code == 200

    def test_when_required_jobs_without_a_callback_url_are_unaffected(self, api, monkeypatch):
        client, _, keys = api
        monkeypatch.setenv("GCON_REQUIRE_CALLBACK_SECRET", "1")
        assert client.post("/jobs", json={"command": 'python3 -c "print(1)"'}, headers=_h(keys["acme"])).status_code == 200
