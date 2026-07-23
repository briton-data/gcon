"""
TLS/mTLS certificate utilities for the gRPC transport.

Why gRPC over HTTP/2 with TLS
------------------------------
GCON's transport requirement is: a coordinator process and a
potentially large, dynamic set of worker-machine agent daemons,
talking over an untrusted network, needing request/response (job
submit, cancel, receipt upload), server push (job assignment to an
already-connected agent), and streaming (log tail) -- all with mutual
authentication so a coordinator only accepts job results from agents
it recognizes, and an agent only accepts job assignments from the
coordinator it's paired with. gRPC over HTTP/2 is the standard fit:
one connection multiplexes the unary calls and the bidirectional
control stream (no separate polling/long-poll machinery), HTTP/2
flow control and keepalives give cheap liveness detection for free,
and protobuf gives a versioned, strongly-typed wire contract instead
of hand-rolled JSON framing. TLS (mutual, via client certificates) is
gRPC's native, first-class credential mechanism
(`grpc.ssl_server_credentials(..., require_client_auth=True)`), so
mTLS costs nothing extra architecturally. No alternative transport
was substituted.

What this module provides
--------------------------
A minimal, self-contained certificate authority for issuing the
coordinator's server certificate and each agent's client certificate,
built entirely on `cryptography` (already a GCON dependency -- see
`gcon.management.key_manager`, which uses it for Ed25519 receipt
signing keys). This is for standing up a working mTLS deployment
without an external CA/PKI dependency; operators who already run a
CA can point `tls_cert_dir` at certificates issued by that CA instead
-- nothing here requires GCON's own CA specifically, only that the
coordinator and its agents share a common trust root and each agent
has a distinct client certificate (the certificate's Common Name
becomes the node's authenticated identity; see `grpc_transport.py`).
"""

from __future__ import annotations

import datetime
import ipaddress
import os
from dataclasses import dataclass
from typing import List, Optional

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


CA_KEY_FILE = "ca.key.pem"
CA_CERT_FILE = "ca.cert.pem"


@dataclass(frozen=True)
class CertPaths:
    key_path: str
    cert_path: str
    ca_cert_path: str


def _write_private_key(path: str, key) -> None:
    with open(path, "wb") as f:
        f.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    os.chmod(path, 0o600)


def _write_cert(path: str, cert: x509.Certificate) -> None:
    with open(path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))


def _new_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def ensure_ca(cert_dir: str) -> CertPaths:
    """Create (or reuse) a development/self-managed CA under
    `cert_dir`. Idempotent: if the CA already exists, it is reused
    rather than regenerated, so already-issued node certificates
    stay valid."""
    os.makedirs(cert_dir, exist_ok=True)
    ca_key_path = os.path.join(cert_dir, CA_KEY_FILE)
    ca_cert_path = os.path.join(cert_dir, CA_CERT_FILE)

    if os.path.exists(ca_key_path) and os.path.exists(ca_cert_path):
        return CertPaths(ca_key_path, ca_cert_path, ca_cert_path)

    key = _new_key()
    subject = issuer = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "GCON Cluster CA")]
    )
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=True,
                crl_sign=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )

    _write_private_key(ca_key_path, key)
    _write_cert(ca_cert_path, cert)
    return CertPaths(ca_key_path, ca_cert_path, ca_cert_path)


def _issue_leaf_cert(
    cert_dir: str,
    common_name: str,
    file_prefix: str,
    san_dns_names: Optional[List[str]] = None,
    san_ip_addresses: Optional[List[str]] = None,
) -> CertPaths:
    ca = ensure_ca(cert_dir)

    with open(ca.key_path, "rb") as f:
        ca_key = serialization.load_pem_private_key(f.read(), password=None)
    with open(ca.cert_path, "rb") as f:
        ca_cert = x509.load_pem_x509_certificate(f.read())

    key = _new_key()
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])

    san_entries: List[x509.GeneralName] = [x509.DNSName(n) for n in (san_dns_names or [])]
    san_entries += [x509.IPAddress(ipaddress.ip_address(ip)) for ip in (san_ip_addresses or [])]
    if not san_entries:
        san_entries = [x509.DNSName(common_name)]

    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=825))
        .add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [x509.oid.ExtendedKeyUsageOID.SERVER_AUTH, x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    key_path = os.path.join(cert_dir, f"{file_prefix}.key.pem")
    cert_path = os.path.join(cert_dir, f"{file_prefix}.cert.pem")
    _write_private_key(key_path, key)
    _write_cert(cert_path, cert)
    return CertPaths(key_path, cert_path, ca.cert_path)


def issue_coordinator_cert(
    cert_dir: str, hostname: str = "localhost", extra_dns_names: Optional[List[str]] = None
) -> CertPaths:
    dns_names = ["localhost", hostname] + (extra_dns_names or [])
    return _issue_leaf_cert(
        cert_dir,
        common_name=hostname,
        file_prefix="coordinator",
        san_dns_names=list(dict.fromkeys(dns_names)),
        san_ip_addresses=["127.0.0.1"],
    )


def issue_agent_cert(cert_dir: str, node_id: str) -> CertPaths:
    """
    The issued certificate's Common Name is the node_id. The
    coordinator's server-side identity check (`grpc_transport.py`)
    extracts this CN from the peer certificate presented during the
    mTLS handshake and treats it as the node's authenticated identity
    -- a `Register` call claiming a different node_id than its own
    certificate's CN is rejected.
    """
    return _issue_leaf_cert(cert_dir, common_name=node_id, file_prefix=f"agent-{node_id}")


def cert_common_name(cert_path: str) -> str:
    with open(cert_path, "rb") as f:
        cert = x509.load_pem_x509_certificate(f.read())
    attrs = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    if not attrs:
        raise ValueError(f"Certificate at {cert_path} has no Common Name")
    return attrs[0].value


def cert_fingerprint(cert_path: str) -> str:
    """SHA-256 fingerprint of the certificate, used as the durable
    `nodes.auth_fingerprint` identity in the control plane -- stable
    across reconnects, distinct per issued agent certificate."""
    with open(cert_path, "rb") as f:
        cert = x509.load_pem_x509_certificate(f.read())
    return cert.fingerprint(hashes.SHA256()).hex()


def load_server_credentials(cert_dir: str, hostname: str = "localhost"):
    """
    Build gRPC server credentials for the coordinator: presents the
    coordinator's own certificate, and *requires* (not just accepts)
    a client certificate signed by the same CA -- this is what makes
    the channel mutually authenticated rather than just server-side
    TLS.
    """
    import grpc

    paths = issue_coordinator_cert(cert_dir, hostname=hostname)
    with open(paths.key_path, "rb") as f:
        private_key = f.read()
    with open(paths.cert_path, "rb") as f:
        cert_chain = f.read()
    with open(paths.ca_cert_path, "rb") as f:
        root_ca = f.read()

    return grpc.ssl_server_credentials(
        [(private_key, cert_chain)],
        root_certificates=root_ca,
        require_client_auth=True,
    )


def load_agent_channel_credentials(cert_dir: str, node_id: str):
    """
    Build gRPC channel (client) credentials for an agent daemon:
    presents the agent's own certificate (its authenticated identity)
    and verifies the coordinator's certificate against the shared CA.
    """
    import grpc

    paths = issue_agent_cert(cert_dir, node_id)
    with open(paths.ca_cert_path, "rb") as f:
        root_ca = f.read()
    with open(paths.key_path, "rb") as f:
        private_key = f.read()
    with open(paths.cert_path, "rb") as f:
        cert_chain = f.read()

    return grpc.ssl_channel_credentials(
        root_certificates=root_ca,
        private_key=private_key,
        certificate_chain=cert_chain,
    )
