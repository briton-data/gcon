"""
Password reset by email -- through a REAL SMTP conversation.

A tiny SMTP server runs on a local port; GCON is pointed at it with the same
GCON_SMTP_* variables a deployment would use. Every message is really sent over
a socket, captured, parsed, and its link followed all the way to a changed
password. What is NOT covered here: a real provider, and the STARTTLS / SSL
transports (they use Python's smtplib as-is; check them against your provider
with `python -m gcon.management.mailer you@example.com`).
"""
import email as emaillib
import re
import socketserver
import threading
import time
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management import mailer as mailer_module
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

WEB = "https://app.example.test"


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        w = lambda line: self.wfile.write((line + "\r\n").encode())
        w("220 fake-smtp ready")
        mail_from, rcpts = None, []
        while True:
            raw = self.rfile.readline()
            if not raw:
                return
            line = raw.decode(errors="replace").strip()
            cmd = line.upper()
            if cmd.startswith("EHLO") or cmd.startswith("HELO"):
                w("250 fake-smtp")
            elif cmd.startswith("MAIL FROM"):
                mail_from = line[10:].strip()
                w("250 OK")
            elif cmd.startswith("RCPT TO"):
                rcpts.append(line[8:].strip().strip("<>"))
                w("250 OK")
            elif cmd == "DATA":
                w("354 end with <CRLF>.<CRLF>")
                data = b""
                while True:
                    chunk = self.rfile.readline()
                    if chunk in (b".\r\n", b""):
                        break
                    data += chunk
                self.server.inbox.append({"from": mail_from, "to": list(rcpts), "message": emaillib.message_from_bytes(data)})
                mail_from, rcpts = None, []
                w("250 OK queued")
            elif cmd == "QUIT":
                w("221 bye")
                return
            else:
                w("250 OK")


class FakeSmtp(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.inbox = []
        threading.Thread(target=self.serve_forever, daemon=True).start()

    @property
    def port(self):
        return self.server_address[1]


@pytest.fixture
def smtp():
    server = FakeSmtp()
    yield server
    server.shutdown()
    server.server_close()


@pytest.fixture
def env(tmp_path, monkeypatch):
    for k in ("GCON_EXPOSE_RESET_TOKEN", "GCON_SMTP_HOST", "GCON_SMTP_PORT", "GCON_SMTP_FROM", "GCON_SMTP_USER",
              "GCON_SMTP_PASSWORD", "GCON_SMTP_SECURITY", "GCON_WEB_BASE_URL"):
        monkeypatch.delenv(k, raising=False)
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, monkeypatch
    coordinator.shutdown()


def _configure(monkeypatch, smtp, **over):
    values = {"GCON_SMTP_HOST": "127.0.0.1", "GCON_SMTP_PORT": str(smtp.port), "GCON_SMTP_SECURITY": "none",
              "GCON_SMTP_FROM": "GCON <no-reply@example.test>", "GCON_WEB_BASE_URL": WEB}
    values.update(over)
    for k, v in values.items():
        if v is None:
            monkeypatch.delenv(k, raising=False)
        else:
            monkeypatch.setenv(k, v)


def _signup(client, email="ann@acme.example", pw="correct-horse-1", name="Ann Acme"):
    r = client.post("/auth/signup", json={"org_name": "Acme", "name": name, "email": email, "password": pw})
    assert r.status_code == 200, r.text


def _wait_mail(smtp, n=1, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if len(smtp.inbox) >= n:
            return smtp.inbox
        time.sleep(0.05)
    return smtp.inbox


def _body(msg):
    return msg.get_payload(decode=True).decode()


def _link_token(body):
    link = re.search(r"https?://\S+", body).group(0)
    return link, parse_qs(urlparse(link).query)["token"][0]


class TestNotConfigured:
    def test_says_unavailable_sends_nothing_and_issues_no_token(self, env, smtp):
        client, _ = env
        _signup(client)
        r = client.post("/auth/forgot-password", json={"email": "ann@acme.example"})
        assert r.json() == {"token": None, "delivery": "unavailable"}
        assert _wait_mail(smtp, 1, timeout=0.6) == []
        assert client.post("/auth/reset-password", json={"token": "x", "new_password": "long-enough-1"}).status_code == 400

    @pytest.mark.parametrize("missing", ["GCON_SMTP_HOST", "GCON_SMTP_FROM", "GCON_WEB_BASE_URL"])
    def test_any_missing_required_setting_means_unavailable(self, env, smtp, missing):
        client, mp = env
        _signup(client)
        _configure(mp, smtp, **{missing: None})
        assert client.post("/auth/forgot-password", json={"email": "ann@acme.example"}).json()["delivery"] == "unavailable"
        assert _wait_mail(smtp, 1, timeout=0.6) == []

    def test_a_web_address_that_is_not_http_is_not_accepted(self, env, smtp):
        client, mp = env
        _signup(client)
        _configure(mp, smtp, GCON_WEB_BASE_URL="javascript:alert(1)")
        assert client.post("/auth/forgot-password", json={"email": "ann@acme.example"}).json()["delivery"] == "unavailable"


class TestEmailDelivery:
    def test_the_full_journey_from_request_to_new_password(self, env, smtp):
        client, mp = env
        _signup(client)
        _configure(mp, smtp)

        r = client.post("/auth/forgot-password", json={"email": "ann@acme.example"})
        assert r.status_code == 200
        assert r.json() == {"token": None, "delivery": "email"}      # the caller is never handed the token

        inbox = _wait_mail(smtp)
        assert len(inbox) == 1
        m = inbox[0]
        assert m["to"] == ["ann@acme.example"] and "no-reply@example.test" in m["from"]
        assert m["message"]["Subject"] == "Reset your GCON password"
        body = _body(m["message"])
        link, token = _link_token(body)
        assert link.startswith(f"{WEB}/reset-password.html?token=")
        assert "Hi Ann Acme" in body and "30 minutes" in body and "ignore this email" in body
        assert "correct-horse-1" not in body

        # follow the link: the token works exactly once, and changes the password
        done = client.post("/auth/reset-password", json={"token": token, "new_password": "a-brand-new-pass-1"})
        assert done.status_code == 200
        assert client.post("/auth/login", json={"email": "ann@acme.example", "password": "correct-horse-1"}).status_code == 401
        assert client.post("/auth/login", json={"email": "ann@acme.example", "password": "a-brand-new-pass-1"}).status_code == 200
        again = client.post("/auth/reset-password", json={"token": token, "new_password": "yet-another-pass-1"})
        assert again.status_code == 400

    def test_unknown_email_gets_the_identical_answer_and_no_mail(self, env, smtp):
        client, mp = env
        _signup(client)
        _configure(mp, smtp)
        known = client.post("/auth/forgot-password", json={"email": "ann@acme.example"})
        unknown = client.post("/auth/forgot-password", json={"email": "nobody@nowhere.example"})
        assert known.status_code == unknown.status_code == 200
        assert known.json() == unknown.json() == {"token": None, "delivery": "email"}
        assert len(_wait_mail(smtp, 2, timeout=1.0)) == 1          # only the real account got mail

    def test_email_is_matched_case_insensitively_and_sent_to_the_stored_address(self, env, smtp):
        client, mp = env
        _signup(client)
        _configure(mp, smtp)
        client.post("/auth/forgot-password", json={"email": "  ANN@Acme.Example "})
        inbox = _wait_mail(smtp)
        assert len(inbox) == 1 and inbox[0]["to"] == ["ann@acme.example"]

    def test_settings_are_read_live_not_frozen_at_startup(self, env, smtp):
        client, mp = env
        _signup(client)
        assert client.post("/auth/forgot-password", json={"email": "ann@acme.example"}).json()["delivery"] == "unavailable"
        _configure(mp, smtp)                                       # configured after the app is running
        assert client.post("/auth/forgot-password", json={"email": "ann@acme.example"}).json()["delivery"] == "email"

    def test_a_dead_mail_server_does_not_break_the_response_or_leak_anything(self, env, smtp):
        client, mp = env
        _signup(client)
        _configure(mp, smtp, GCON_SMTP_PORT="1")                   # nothing listens there
        r = client.post("/auth/forgot-password", json={"email": "ann@acme.example"})
        assert r.status_code == 200 and r.json() == {"token": None, "delivery": "email"}
        assert "ECONN" not in r.text and "refused" not in r.text.lower()


class TestAbuseProtection:
    def test_repeated_requests_for_one_address_are_limited_and_stop_sending(self, env, smtp):
        client, mp = env
        _signup(client)
        _configure(mp, smtp)
        codes = [client.post("/auth/forgot-password", json={"email": "ann@acme.example"}).status_code for _ in range(8)]
        assert codes[:5] == [200] * 5 and 429 in codes
        assert client.post("/auth/forgot-password", json={"email": "ann@acme.example"}).json()["detail"].startswith("Too many reset requests")
        time.sleep(0.6)
        assert len(smtp.inbox) <= 5                                # the flood did not reach the inbox

    def test_the_limit_applies_whether_or_not_the_account_exists(self, env, smtp):
        client, mp = env
        _configure(mp, smtp)
        codes = [client.post("/auth/forgot-password", json={"email": "ghost@nowhere.example"}).status_code for _ in range(7)]
        assert 429 in codes                                        # same behaviour for unknown addresses: no enumeration

    def test_another_address_is_not_affected(self, env, smtp):
        client, mp = env
        _signup(client)
        _configure(mp, smtp)
        for _ in range(7):
            client.post("/auth/forgot-password", json={"email": "ann@acme.example"})
        assert client.post("/auth/forgot-password", json={"email": "other@acme.example"}).status_code == 200


class TestDevelopmentModeStillWorks:
    def test_the_dev_flag_takes_precedence_and_sends_no_mail(self, env, smtp):
        client, mp = env
        _signup(client)
        _configure(mp, smtp)
        mp.setenv("GCON_EXPOSE_RESET_TOKEN", "1")
        r = client.post("/auth/forgot-password", json={"email": "ann@acme.example"}).json()
        assert r["delivery"] == "dev_token" and r["token"]
        assert _wait_mail(smtp, 1, timeout=0.6) == []


class TestMailerModule:
    def test_reset_link_encodes_the_token(self, monkeypatch):
        monkeypatch.setenv("GCON_WEB_BASE_URL", "https://app.example.test/")
        assert mailer_module.reset_link("a b&c") == "https://app.example.test/reset-password.html?token=a%20b%26c"

    def test_the_self_test_command_reports_missing_configuration(self, monkeypatch, capsys):
        for k in ("GCON_SMTP_HOST", "GCON_SMTP_FROM", "GCON_WEB_BASE_URL"):
            monkeypatch.delenv(k, raising=False)
        assert mailer_module._selftest("you@example.com") == 2
        assert "NOT configured" in capsys.readouterr().out

    def test_the_self_test_command_really_sends(self, monkeypatch, smtp, capsys):
        _configure(monkeypatch, smtp)
        assert mailer_module._selftest("you@example.com") == 0
        assert _wait_mail(smtp)[0]["to"] == ["you@example.com"]

    def test_the_self_test_command_shows_the_real_failure(self, monkeypatch, smtp, capsys):
        _configure(monkeypatch, smtp, GCON_SMTP_PORT="1")
        assert mailer_module._selftest("you@example.com") == 1
        assert "FAILED" in capsys.readouterr().out
