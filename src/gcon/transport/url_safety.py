"""
Customer-supplied URLs (a job's callback_url, a webhook subscription's url) are
fetched BY THE COORDINATOR, from inside GCON's network. Left unchecked that lets
a customer make GCON call its own loopback, its private network or the cloud
metadata service (169.254.169.254, which hands out the host's cloud
credentials). So a URL is only ever fetched if:

  * the scheme is https (http only when GCON_WEBHOOK_ALLOW_PRIVATE_TARGETS is
    set, which is for local development and tests and also lifts the address
    check below),
  * it has a host and no embedded credentials,
  * EVERY address the host resolves to is a public one (not loopback, private,
    link-local, reserved, multicast or unspecified), and
  * redirects are never followed (a public URL must not bounce to a private one).

The address check is repeated at delivery time, not just at submission, because
DNS can change between the two. A name that is swapped between that check and the
connection itself is not defended against here; for that, also block egress to
private ranges at the network level.
"""
import ipaddress
import os
import socket
import urllib.error
import urllib.parse
import urllib.request

MAX_URL_LENGTH = 2048


class UnsafeURLError(ValueError):
    """The URL must not be fetched by the coordinator. The message says why."""


def _allow_private_targets() -> bool:
    return os.environ.get("GCON_WEBHOOK_ALLOW_PRIVATE_TARGETS", "").strip().lower() in ("1", "true", "yes")


def _is_public(address: str) -> bool:
    ip = ipaddress.ip_address(address.split("%", 1)[0])
    if getattr(ip, "ipv4_mapped", None) is not None:
        ip = ip.ipv4_mapped
    return not (
        ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
        or ip.is_reserved or ip.is_unspecified
    )


def validate_outbound_url(url, *, resolve: bool = True) -> str:
    """Return the URL if it is safe to fetch, otherwise raise UnsafeURLError."""
    if not isinstance(url, str) or not url.strip():
        raise UnsafeURLError("URL is empty.")
    url = url.strip()
    if len(url) > MAX_URL_LENGTH or not url.isprintable():
        raise UnsafeURLError("URL is too long or contains control characters.")

    parts = urllib.parse.urlsplit(url)
    allow_private = _allow_private_targets()
    allowed_schemes = ("https", "http") if allow_private else ("https",)
    if parts.scheme not in allowed_schemes:
        raise UnsafeURLError("Only https:// URLs are allowed.")
    if not parts.hostname:
        raise UnsafeURLError("URL has no host.")
    if parts.username or parts.password:
        raise UnsafeURLError("URLs with embedded credentials are not allowed.")
    if allow_private:
        return url

    host = parts.hostname
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        addresses = [str(literal)]
    elif not resolve:
        return url
    else:
        try:
            port = parts.port or 443
            infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except (socket.gaierror, ValueError):
            raise UnsafeURLError(f"Host '{host}' could not be resolved.")
        addresses = sorted({info[4][0] for info in infos})
    for address in addresses:
        if not _is_public(address):
            raise UnsafeURLError("URL points at a private, loopback or otherwise non-public address.")
    return url


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirects are not followed", headers, fp)


_OPENER = urllib.request.build_opener(_NoRedirects)


def safe_urlopen(request, timeout):
    """urlopen for a customer-supplied URL: validated again now, no redirects."""
    validate_outbound_url(request.full_url)
    return _OPENER.open(request, timeout=timeout)
