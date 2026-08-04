"""
Covers "tighten the CSP": script-src must no longer include
'unsafe-inline' now that templates/login.html's only inline <script>
block moved to /static/js/login.js, and the page that used to need it
must still fully work (script loads, page renders).
"""

import pytest
from fastapi.testclient import TestClient

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "csp-test-pw-123")
    coordinator = GCONCoordinator()
    presentation = PresentationLayer(coordinator)
    server = WebServer(presentation)
    with TestClient(server.app) as c:
        yield c
    coordinator.shutdown()


def test_script_src_has_no_unsafe_inline(client):
    resp = client.get("/login")
    csp = resp.headers["content-security-policy"]
    directives = {d.strip().split()[0]: d for d in csp.split(";")}
    assert "'unsafe-inline'" not in directives["script-src"]


def test_login_page_has_no_inline_script_block(client):
    resp = client.get("/login")
    assert resp.status_code == 200
    assert "<script>" not in resp.text
    assert '<script src="/static/js/login.js"></script>' in resp.text


def test_login_js_is_served_as_a_static_file(client):
    resp = client.get("/static/js/login.js")
    assert resp.status_code == 200
    assert "doLogin" in resp.text


def test_font_sources_actually_allowed_by_csp(client):
    """
    Regression guard for a pre-existing bug this task's CSP edit also
    fixed: login.html loads fonts.googleapis.com/fonts.gstatic.com,
    which the old CSP didn't allow-list.
    """
    resp = client.get("/login")
    csp = resp.headers["content-security-policy"]
    assert "fonts.googleapis.com" in csp
    assert "fonts.gstatic.com" in csp
    assert "fonts.googleapis.com" in resp.text  # still actually used