"""
Customer-supplied callback URLs are fetched by the coordinator. They must not
be able to reach GCON's own loopback, private network or the cloud metadata
service, and a redirect must not be able to bounce a public URL into one.
"""
import http.server
import socket
import threading
import urllib.error
import urllib.request

import pytest

pytestmark = pytest.mark.real_ssrf_guard

from gcon.cluster.coordinator import GCONCoordinator
from gcon.transport import url_safety
from gcon.transport.url_safety import UnsafeURLError, safe_urlopen, validate_outbound_url


@pytest.mark.parametrize("url", [
    "http://example.com/hook",                    # not https
    "ftp://example.com/x", "file:///etc/passwd", "gopher://example.com/",
    "https://127.0.0.1/x", "https://localhost/x", "https://[::1]/x",
    "https://10.0.0.5/x", "https://192.168.1.1/x", "https://172.16.0.1/x",
    "https://169.254.169.254/latest/meta-data/",  # cloud metadata
    "https://0.0.0.0/x", "https://[::ffff:127.0.0.1]/x", "https://[fd00::1]/x",
    "https://user:pw@example.com/x", "https:///nohost", "", "   ",
    "https://example.com/" + "a" * 2100, "https://example.com/\nX: y",
])
def test_unsafe_urls_are_refused(url):
    with pytest.raises(UnsafeURLError):
        validate_outbound_url(url)


def test_a_name_that_resolves_to_a_private_address_is_refused(monkeypatch):
    monkeypatch.setattr(url_safety.socket, "getaddrinfo",
                        lambda *a, **k: [(2, 1, 6, "", ("10.1.2.3", 443))])
    with pytest.raises(UnsafeURLError, match="non-public"):
        validate_outbound_url("https://innocent.example.com/hook")


def test_every_resolved_address_must_be_public(monkeypatch):
    monkeypatch.setattr(url_safety.socket, "getaddrinfo",
                        lambda *a, **k: [(2, 1, 6, "", ("93.184.216.34", 443)), (2, 1, 6, "", ("127.0.0.1", 443))])
    with pytest.raises(UnsafeURLError):
        validate_outbound_url("https://mixed.example.com/hook")


def test_an_unresolvable_host_is_refused(monkeypatch):
    def boom(*a, **k):
        raise socket.gaierror("nope")
    monkeypatch.setattr(url_safety.socket, "getaddrinfo", boom)
    with pytest.raises(UnsafeURLError, match="resolved"):
        validate_outbound_url("https://does-not-exist.example/hook")


def test_a_public_https_url_is_accepted(monkeypatch):
    monkeypatch.setattr(url_safety.socket, "getaddrinfo",
                        lambda *a, **k: [(2, 1, 6, "", ("93.184.216.34", 443))])
    assert validate_outbound_url("https://hooks.example.com/gcon") == "https://hooks.example.com/gcon"


def test_the_dev_switch_allows_loopback_http(monkeypatch):
    monkeypatch.setenv("GCON_WEBHOOK_ALLOW_PRIVATE_TARGETS", "1")
    assert validate_outbound_url("http://127.0.0.1:9999/hook")


def test_delivery_revalidates_so_a_url_that_turned_private_is_not_fetched():
    # Even a URL stored earlier is refused at the moment of fetching.
    with pytest.raises(UnsafeURLError):
        safe_urlopen(urllib.request.Request("https://127.0.0.1:1/x"), timeout=1)


def test_redirects_are_never_followed(monkeypatch):
    hits = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            hits.append(self.path)
            if self.path == "/start":
                self.send_response(302)
                self.send_header("Location", "/internal")
                self.end_headers()
            else:
                self.send_response(200)
                self.end_headers()
        def log_message(self, *a): pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("GCON_WEBHOOK_ALLOW_PRIVATE_TARGETS", "1")   # to reach the local test server
    try:
        req = urllib.request.Request(f"http://127.0.0.1:{server.server_port}/start", data=b"{}", method="POST")
        with pytest.raises(urllib.error.HTTPError) as err:
            safe_urlopen(req, timeout=3)
        assert err.value.code == 302
        assert hits == ["/start"]                                   # /internal was never requested
    finally:
        server.shutdown()


def test_the_coordinator_refuses_an_unsafe_callback_url_at_submission():
    coord = GCONCoordinator()
    try:
        with pytest.raises(ValueError, match="private|https"):
            coord.submit_job("j1", "echo hi", callback_url="https://169.254.169.254/latest/meta-data/")
        assert "j1" not in coord.jobs
    finally:
        coord.shutdown()
