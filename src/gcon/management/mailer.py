"""
Email delivery for GCON (currently used for password-reset links).

Plain SMTP through the standard library, so it works with any provider that
offers SMTP: Gmail (app password), SendGrid, Resend, Mailgun, Amazon SES, a
company mail server, and so on. Nothing here is provider-specific.

Configuration is read from the environment on EVERY call (never frozen at
import), so it can be changed without a code change:

    GCON_SMTP_HOST       required   e.g. smtp.sendgrid.net
    GCON_SMTP_FROM       required   e.g. "GCON <no-reply@yourdomain.com>"
    GCON_WEB_BASE_URL    required   where the customer site lives, e.g.
                                    https://gcon66.netlify.app  (used to build the reset link)
    GCON_SMTP_PORT       optional   default 587 (starttls), 465 (ssl), 25 (none)
    GCON_SMTP_USER       optional   } login, if the server needs one
    GCON_SMTP_PASSWORD   optional   }
    GCON_SMTP_SECURITY   optional   starttls (default) | ssl | none

If host / from / web base URL are not all set, email is simply "not configured"
and the password-reset endpoint says so instead of pretending to send anything.

Check your settings without going through the website:

    python -m gcon.management.mailer you@example.com
"""
import logging
import os
import smtplib
import ssl
import sys
from email.message import EmailMessage
from urllib.parse import quote

log = logging.getLogger(__name__)

SEND_TIMEOUT_SECONDS = 15
_DEFAULT_PORTS = {"starttls": 587, "ssl": 465, "none": 25}


def _env(name):
    return (os.environ.get(name) or "").strip()


def web_base_url():
    """The customer site's address, no trailing slash; "" if unset or not http(s)."""
    url = _env("GCON_WEB_BASE_URL").rstrip("/")
    return url if url.startswith(("http://", "https://")) else ""


def security_mode():
    mode = (_env("GCON_SMTP_SECURITY") or "starttls").lower()
    return mode if mode in _DEFAULT_PORTS else "starttls"


class SmtpMailer:
    """Sends mail through the SMTP server named in the environment."""

    def configured(self):
        """True only when a message could really be sent AND its link would point somewhere."""
        return bool(_env("GCON_SMTP_HOST") and _env("GCON_SMTP_FROM") and web_base_url())

    def send(self, to, subject, body):
        """Send one plain-text message. Raises on any failure (callers decide what to do)."""
        host, sender = _env("GCON_SMTP_HOST"), _env("GCON_SMTP_FROM")
        if not host or not sender:
            raise RuntimeError("Email is not configured (GCON_SMTP_HOST / GCON_SMTP_FROM).")
        mode = security_mode()
        try:
            port = int(_env("GCON_SMTP_PORT") or _DEFAULT_PORTS[mode])
        except ValueError:
            raise RuntimeError("GCON_SMTP_PORT must be a number.")

        msg = EmailMessage()
        msg["From"], msg["To"], msg["Subject"] = sender, to, subject
        msg.set_content(body)

        context = ssl.create_default_context()
        server = (smtplib.SMTP_SSL(host, port, timeout=SEND_TIMEOUT_SECONDS, context=context)
                  if mode == "ssl" else smtplib.SMTP(host, port, timeout=SEND_TIMEOUT_SECONDS))
        with server:
            server.ehlo()
            if mode == "starttls":
                server.starttls(context=context)
                server.ehlo()
            if _env("GCON_SMTP_USER"):
                server.login(_env("GCON_SMTP_USER"), os.environ.get("GCON_SMTP_PASSWORD", ""))
            server.send_message(msg)


def reset_link(token):
    return f"{web_base_url()}/reset-password.html?token={quote(token, safe='')}"


def password_reset_message(name, token, minutes):
    """(subject, body) for a password-reset email."""
    who = f"Hi {name}," if name else "Hello,"
    body = (
        f"{who}\n\n"
        "Someone asked to reset the password for your GCON account. "
        f"To choose a new password, open this link within {minutes} minutes:\n\n"
        f"{reset_link(token)}\n\n"
        "If you didn't ask for this, you can ignore this email: your password will not change.\n"
        "For your security, don't forward this email to anyone.\n\n"
        "GCON\n"
    )
    return "Reset your GCON password", body


def _selftest(to):
    m = SmtpMailer()
    if not m.configured():
        print("Email is NOT configured. Set GCON_SMTP_HOST, GCON_SMTP_FROM and GCON_WEB_BASE_URL "
              "(and GCON_SMTP_USER / GCON_SMTP_PASSWORD if your server needs a login).")
        return 2
    print(f"Sending a test message to {to} via {_env('GCON_SMTP_HOST')} "
          f"(port {_env('GCON_SMTP_PORT') or _DEFAULT_PORTS[security_mode()]}, {security_mode()}) ...")
    try:
        m.send(to, "GCON email test", "If you can read this, GCON can send email. No action needed.\n")
    except Exception as e:                       # show the real reason: this is a setup tool
        print(f"FAILED: {type(e).__name__}: {e}")
        return 1
    print("Sent. Check the inbox (and spam folder).")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2 or "@" not in sys.argv[1]:
        print("usage: python -m gcon.management.mailer you@example.com")
        sys.exit(2)
    sys.exit(_selftest(sys.argv[1]))
