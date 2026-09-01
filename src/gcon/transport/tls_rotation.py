"""
mTLS certificate rotation — closes the "mTLS certs and HMAC keys are
static once issued" gap for the cert half; see
gcon.execution.hmac_keyring for the HMAC half.

Three independent operations, because they have genuinely different
blast radii and you don't want reaching for the big one to be the
path of least resistance:

  * `reissue_leaf_cert` — re-mint one node's (or the coordinator's)
    certificate, signed by the *current* CA. For routine periodic
    rotation of a single identity, or after a suspected individual
    key compromise. Cheap, narrow.
  * `rotate_ca` — replace the CA itself. Every future leaf cert is
    signed by the new CA; every certificate already issued under the
    old CA keeps working during a grace period (the old CA cert is
    kept in a trust bundle, not deleted) so a rolling re-issue of
    every node's cert doesn't require a synchronized flag-day. Use
    when the CA private key itself is suspected compromised, or on a
    routine long-interval schedule (e.g. yearly).
  * `revoke_node_cert` — immediately stop trusting one specific
    node's certificate, independent of CA/leaf rotation. This is
    application-level revocation (a fingerprint denylist checked in
    grpc_transport.py's Register handler), because gRPC's Python
    SSL credentials have no CRL/OCSP support -- the TLS handshake
    itself has no way to reject an individual revoked leaf cert, only
    "signed by a trusted CA or not".

Hot rotation without dropping connections
--------------------------------------------
A `grpc.Server`'s credentials are normally fixed at
`add_secure_port()` time -- there is no supported way to swap them on
a live `grpc.ssl_server_credentials()` object. `load_dynamic_server_
credentials` uses `grpc.dynamic_ssl_server_credentials` instead: gRPC
calls the supplied fetcher fresh for *each new incoming connection*,
so a coordinator process picks up a rotated leaf cert or an updated
trust bundle for every subsequent handshake without a restart --
already-established connections are unaffected either way (TLS
doesn't renegotiate mid-connection), which is fine: they'll pick up
the change the next time they reconnect (agents already have a
reconnect loop for exactly this kind of transient disruption).

The one thing that is NOT hot-reloadable this way: an *agent's own*
outbound channel credentials (`tls.load_agent_channel_credentials`).
grpc-python has no client-side equivalent of `dynamic_ssl_server_
credentials` -- an agent must reconnect (tearing down and rebuilding
its channel) to pick up a new client cert or trust bundle. Documented
here rather than silently promised as hot; the existing agent
reconnect loop already covers this in practice.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from cryptography import x509
from cryptography.hazmat.primitives import hashes

from gcon.transport import tls as _tls

TRUST_BUNDLE_FILE = "trusted_cas.pem"
CA_MANIFEST_FILE = "ca_manifest.json"
REVOCATION_FILE = "revoked_fingerprints.json"


# --------------------------------------------------------------- CA rotation
def _manifest_path(cert_dir: str) -> Path:
    return Path(cert_dir) / CA_MANIFEST_FILE


def _load_manifest(cert_dir: str) -> dict:
    path = _manifest_path(cert_dir)
    if not path.exists():
        return {"retired_cas": []}  # list of {"fingerprint", "retired_at"}
    return json.loads(path.read_text())


def _save_manifest(cert_dir: str, manifest: dict) -> None:
    _manifest_path(cert_dir).write_text(json.dumps(manifest, indent=2))


def rotate_ca(cert_dir: str, hostname: str = "localhost") -> str:
    """
    Replaces the active CA with a freshly generated one. The old CA
    certificate (not its private key -- that's simply discarded,
    it's no longer needed once nothing new is being signed with it)
    is appended to `trusted_cas.pem` and recorded in the manifest, so
    certificates already issued under it keep verifying via the trust
    bundle (see `load_trust_bundle`) until `prune_expired_trust`
    removes it after the grace period.

    Also immediately reissues the coordinator's and dashboard's own
    leaf certs under the new CA. This isn't optional/best-effort the
    way reissuing every agent cert is: `load_agent_channel_
    credentials` (an agent's client-side trust root, which -- unlike
    the server's dynamic, bundle-aware credentials -- is NOT hot-
    reloadable and is always just the single current CA file, see
    this module's docstring) means any agent that reconnects after
    this call will only trust the new CA. If the server kept
    presenting its old-CA-signed cert, every such reconnect would
    fail server-certificate verification. Existing agent certs are
    untouched here -- they don't need to change for the server to
    keep accepting them (the server verifies incoming client certs
    against the full trust bundle, old CA included, during the grace
    period) -- only the server's own presented identity has to move
    immediately.

    Returns the new CA certificate's SHA-256 fingerprint.
    """
    ca_key_path = os.path.join(cert_dir, _tls.CA_KEY_FILE)
    ca_cert_path = os.path.join(cert_dir, _tls.CA_CERT_FILE)

    if os.path.exists(ca_cert_path):
        old_fingerprint = _tls.cert_fingerprint(ca_cert_path)
        old_cert_pem = Path(ca_cert_path).read_bytes()
        bundle_path = Path(cert_dir) / TRUST_BUNDLE_FILE
        with open(bundle_path, "ab") as f:
            f.write(b"\n" if bundle_path.exists() and bundle_path.stat().st_size else b"")
            f.write(old_cert_pem)

        manifest = _load_manifest(cert_dir)
        manifest["retired_cas"].append({
            "fingerprint": old_fingerprint,
            "retired_at": datetime.now(timezone.utc).isoformat(),
        })
        _save_manifest(cert_dir, manifest)

        # Remove the *active* CA files so ensure_ca() (which reuses
        # ca.key.pem/ca.cert.pem if present) generates a genuinely new
        # CA rather than seeing the old one and no-opping.
        os.remove(ca_key_path)
        os.remove(ca_cert_path)

    new_ca = _tls.ensure_ca(
        cert_dir,
        # Unique CN per generation -- see ensure_ca's docstring for
        # why a shared subject name across CA generations breaks
        # OpenSSL's path building once both are in the trust bundle
        # together.
        common_name=f"GCON Cluster CA ({datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-"
                    f"{os.urandom(3).hex()})",
    )

    reissue_coordinator_cert(cert_dir, hostname=hostname)
    reissue_leaf_cert(
        cert_dir, file_prefix="dashboard", common_name=hostname,
        san_dns_names=["localhost", hostname], san_ip_addresses=["127.0.0.1"],
    )

    return _tls.cert_fingerprint(new_ca.cert_path)


def load_trust_bundle(cert_dir: str) -> bytes:
    """Current CA cert + every not-yet-pruned retired CA cert,
    concatenated -- the root_certificates any verifier (server or
    client) should use during/after a CA rotation, instead of just
    the current CA alone."""
    parts = []
    ca_cert_path = os.path.join(cert_dir, _tls.CA_CERT_FILE)
    if os.path.exists(ca_cert_path):
        parts.append(Path(ca_cert_path).read_bytes())
    bundle_path = Path(cert_dir) / TRUST_BUNDLE_FILE
    if bundle_path.exists():
        parts.append(bundle_path.read_bytes())
    return b"\n".join(parts)


def prune_expired_trust(cert_dir: str, grace_period_days: float = 30) -> int:
    """Removes retired CA certs from the trust bundle once they're
    older than `grace_period_days` past their retirement -- keeps
    the bundle (and thus every verifier's trust set) from growing
    forever across repeated rotations. Returns how many were pruned.
    A retired CA still has certs it signed trusted right up until
    this runs; call it well after every node has actually re-issued
    under the new CA, not on a fixed schedule blind to that."""
    manifest = _load_manifest(cert_dir)
    cutoff = datetime.now(timezone.utc) - timedelta(days=grace_period_days)

    keep = []
    pruned_fingerprints = set()
    for entry in manifest["retired_cas"]:
        retired_at = datetime.fromisoformat(entry["retired_at"])
        if retired_at < cutoff:
            pruned_fingerprints.add(entry["fingerprint"])
        else:
            keep.append(entry)

    if not pruned_fingerprints:
        return 0

    bundle_path = Path(cert_dir) / TRUST_BUNDLE_FILE
    if bundle_path.exists():
        remaining_certs = []
        for pem_block in _split_pem_certs(bundle_path.read_bytes()):
            cert = x509.load_pem_x509_certificate(pem_block)
            fp = cert.fingerprint(hashes.SHA256()).hex()
            if fp not in pruned_fingerprints:
                remaining_certs.append(pem_block)
        bundle_path.write_bytes(b"\n".join(remaining_certs))

    manifest["retired_cas"] = keep
    _save_manifest(cert_dir, manifest)
    return len(pruned_fingerprints)


def _split_pem_certs(data: bytes) -> List[bytes]:
    marker = b"-----BEGIN CERTIFICATE-----"
    blocks = []
    for chunk in data.split(marker)[1:]:
        blocks.append(marker + chunk.split(b"-----END CERTIFICATE-----")[0] + b"-----END CERTIFICATE-----\n")
    return blocks


# ------------------------------------------------------------- leaf rotation
def reissue_leaf_cert(cert_dir: str, file_prefix: str, common_name: str,
                       san_dns_names: Optional[List[str]] = None,
                       san_ip_addresses: Optional[List[str]] = None) -> str:
    """
    Force-regenerates one leaf certificate (coordinator, dashboard,
    or an agent's), signed by the *current* CA -- bypassing
    `_issue_leaf_cert`'s normal idempotent reuse. For routine
    rotation of a single identity's cert without touching the CA.
    Returns the new certificate's SHA-256 fingerprint.
    """
    key_path = os.path.join(cert_dir, f"{file_prefix}.key.pem")
    cert_path = os.path.join(cert_dir, f"{file_prefix}.cert.pem")
    # Remove the existing files so _issue_leaf_cert's reuse-check
    # (which is exactly what routine issuance wants -- see its own
    # docstring) doesn't just hand back the cert we're trying to
    # replace.
    for p in (key_path, cert_path):
        if os.path.exists(p):
            os.remove(p)
    paths = _tls._issue_leaf_cert(
        cert_dir, common_name=common_name, file_prefix=file_prefix,
        san_dns_names=san_dns_names, san_ip_addresses=san_ip_addresses,
    )
    return _tls.cert_fingerprint(paths.cert_path)


def reissue_agent_cert(cert_dir: str, node_id: str) -> str:
    return reissue_leaf_cert(cert_dir, file_prefix=f"agent-{node_id}", common_name=node_id)


def reissue_coordinator_cert(cert_dir: str, hostname: str = "localhost") -> str:
    return reissue_leaf_cert(
        cert_dir, file_prefix="coordinator", common_name=hostname,
        san_dns_names=["localhost", hostname], san_ip_addresses=["127.0.0.1"],
    )


# --------------------------------------------------------------- revocation
def _revocation_path(cert_dir: str) -> Path:
    return Path(cert_dir) / REVOCATION_FILE


def _load_revocations(cert_dir: str) -> dict:
    path = _revocation_path(cert_dir)
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def revoke_node_cert(cert_dir: str, node_id: str, reason: str = "") -> str:
    """
    Immediately (no grace period, no waiting for a reconnect cycle --
    enforced by grpc_transport.py's Register handler on the node's
    *next* handshake) stops trusting this node's current certificate,
    by fingerprint. Does not touch the cert files themselves or
    anyone else's trust -- this is a denylist entry, not a CA
    operation. Returns the fingerprint that was revoked.

    Raises FileNotFoundError if this node has no issued certificate
    on file (nothing to revoke).
    """
    cert_path = os.path.join(cert_dir, f"agent-{node_id}.cert.pem")
    fingerprint = _tls.cert_fingerprint(cert_path)  # raises if missing
    revocations = _load_revocations(cert_dir)
    revocations[fingerprint] = {
        "node_id": node_id, "reason": reason,
        "revoked_at": datetime.now(timezone.utc).isoformat(),
    }
    _revocation_path(cert_dir).write_text(json.dumps(revocations, indent=2))
    return fingerprint


def is_fingerprint_revoked(cert_dir: str, fingerprint: str) -> bool:
    return fingerprint in _load_revocations(cert_dir)


# ---------------------------------------------------------- hot server creds
def load_dynamic_server_credentials(cert_dir: str, hostname: str = "localhost"):
    """
    Like `tls.load_server_credentials`, but hot-reloadable: every new
    incoming connection re-reads the coordinator's leaf cert and the
    full trust bundle from disk at handshake time, via `grpc.dynamic_
    ssl_server_credentials`. A `rotate_ca` or `reissue_coordinator_
    cert` call takes effect for the next connection with no
    coordinator restart and no dropped existing connections. See this
    module's docstring for the client-side (agent channel) limitation.
    """
    import grpc

    def build_config():
        paths = _tls.issue_coordinator_cert(cert_dir, hostname=hostname)
        with open(paths.key_path, "rb") as f:
            private_key = f.read()
        with open(paths.cert_path, "rb") as f:
            cert_chain = f.read()
        root_certificates = load_trust_bundle(cert_dir)
        return grpc.ssl_server_certificate_configuration(
            [(private_key, cert_chain)], root_certificates=root_certificates,
        )

    def fetch():
        # Called on every new connection; returning a fresh config
        # each time is what makes rotation hot -- see module
        # docstring. Never returns None (which would mean "keep the
        # current config"): a rotate_ca/reissue call between
        # connections should always be picked up.
        return build_config()

    return grpc.dynamic_ssl_server_credentials(
        build_config(), fetch, require_client_authentication=True,
    )
