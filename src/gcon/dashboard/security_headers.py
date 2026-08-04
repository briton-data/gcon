"""
Baseline HTTP security headers, shared by the dashboard app
(web_server.py) and the public API app (api/api_v1.py).

This is the browser/HTTP-facing hardening layer -- separate from
transport mTLS (transport/tls.py, coordinator<->agent only) and from
authentication/authorization (management/auth.py, rbac.py). It
doesn't authenticate anyone; it just tells browsers how to treat
responses safely once they arrive.

`force_https` controls HSTS: only turn it on once the process is
actually reachable over TLS (either uvicorn given a cert directly, or
a TLS-terminating reverse proxy in front of it) -- sending
Strict-Transport-Security over plain HTTP is a footgun that can lock
users out of a dev instance that never gets HTTPS.
"""

import os

from starlette.middleware.base import BaseHTTPMiddleware


def env_flag(name, default=False):
    """Parse a boolean-ish environment variable ('1'/'true'/'yes'/'on').

    Shared across the dashboard/API security wiring (this module and
    gcon.dashboard.web_server) so GCON_FORCE_HTTPS is interpreted
    identically everywhere it's read.
    """
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


# Backwards-compatible alias for any existing internal callers.
_env_flag = env_flag


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, force_https=None, extra_csp=None):
        super().__init__(app)
        self.force_https = (
            _env_flag("GCON_FORCE_HTTPS") if force_https is None else force_https
        )
        self.csp = extra_csp or (
            "default-src 'self'; img-src 'self' data:; "
            # style-src still needs 'unsafe-inline': several dashboard
            # panels (node_summary.html, receipt_summary.html,
            # storage_summary.html, ...) render server-computed inline
            # `style="width: NN%; ..."` bars whose value is a live
            # percentage from `dashboard.*_summary`, not static markup.
            # Removing this would need converting those to CSS custom
            # properties set via JS/data attributes instead of Jinja;
            # left as a follow-up (script-src below is the one that
            # actually mattered here -- no template needs inline
            # <script> anymore, see templates/login.html -> static/js/login.js).
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com; "
            "script-src 'self' https://cdn.jsdelivr.net; "
            "font-src 'self' https://cdn.jsdelivr.net https://fonts.gstatic.com; "
            "connect-src 'self' ws: wss:; frame-ancestors 'none'"
        )

    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Permissions-Policy"] = (
            "geolocation=(), microphone=(), camera=()"
        )
        response.headers.setdefault("Content-Security-Policy", self.csp)
        if self.force_https:
            response.headers["Strict-Transport-Security"] = (
                "max-age=63072000; includeSubDomains"
            )
        return response