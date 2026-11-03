"""The staff login: a missing or non-text field is a 422 (not a 500), the session cookie is Secure whenever the browser used
HTTPS (including behind a TLS-terminating proxy such as Railway), and an unknown email costs the same time as a real one."""
import time

from starlette.testclient import TestClient

from tests.api.test_staff_plan_routes import OWNER_EMAIL, OWNER_PASSWORD, stack  # noqa: F401  (stack is a fixture)


def _cookie(response):
    return response.headers.get("set-cookie", "").lower()


def test_a_missing_or_non_text_password_is_a_422_not_a_500(stack):
    server, _ = stack
    client = TestClient(server.app)
    assert client.post("/auth/login", json={"email": OWNER_EMAIL}).status_code == 422
    assert client.post("/auth/login", json={"email": OWNER_EMAIL, "password": 123}).status_code == 422


def test_the_session_cookie_is_secure_behind_a_tls_proxy_and_only_then(stack, monkeypatch):
    server, _ = stack
    monkeypatch.delenv("GCON_FORCE_HTTPS", raising=False)
    body = {"email": OWNER_EMAIL, "password": OWNER_PASSWORD}
    plain = TestClient(server.app).post("/auth/login", json=body)
    proxied = TestClient(server.app).post("/auth/login", json=body, headers={"X-Forwarded-Proto": "https"})
    assert plain.status_code == proxied.status_code == 200
    assert "secure" not in _cookie(plain)
    assert "secure" in _cookie(proxied)


def test_an_unknown_email_takes_as_long_as_a_real_one(stack):
    server, _ = stack
    server.management.signup_customer("Acme", "Ann", "ann@acme.example", "correct-horse-1")

    def median_ms(call):
        samples = []
        for _ in range(3):
            start = time.perf_counter()
            call()
            samples.append(time.perf_counter() - start)
        return sorted(samples)[1] * 1000

    staff = lambda email: (lambda: _swallow(lambda: server.management.login(email, "wrong")))
    customer = lambda email: (lambda: server.management.customer_registry.authenticate(email, "wrong"))
    # A real address costs a full password check (tens of milliseconds); the unknown one used to cost under one.
    assert median_ms(staff("nobody@example.com")) > 0.5 * median_ms(staff(OWNER_EMAIL))
    assert median_ms(customer("nobody@example.com")) > 0.5 * median_ms(customer("ann@acme.example"))


def _swallow(call):
    try:
        call()
    except ValueError:
        pass
