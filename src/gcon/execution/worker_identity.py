"""
Per-node Ed25519 identity for genuine worker attestation -- each node
generates and holds its OWN keypair (private key never leaves it),
signs its own job results before ever sending them, and registers its
public key with the coordinator at connection time (see
RegisterRequest.ed25519_public_key_pem in gcon_transport.proto). This
is what lets a receipt's worker_attestation block be checked by
anyone holding the node's public key, without needing the
coordinator's HMAC secret or trusting the coordinator not to have
forged it -- real per-worker attestation, not just a transport-
authenticated claim about who's on the other end of a connection.

Deliberately NOT gcon.management.key_manager.KeyManager: that module
manages ONE static, file-based keypair shared by every signer
(receipt.py's ReceiptGenerator hardcodes "signer": "gcon-agent-001"
regardless of which node actually ran a job) -- adequate for nothing
that needs to answer "which specific worker signed this", which is
the entire point here; reusing it as-is would mean anyone with that
one file could forge any node's signature. See agent_daemon.py's
_run_job docstring for the history of what shipping that design
un-scoped already broke once (a second, incompatible receipt silently
overwriting the correct one on every coordinator restart). This
module doesn't touch that one, and isn't a replacement for it -- it's
a new, separate, per-node scheme.
"""
import base64
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


def ensure_node_keypair(cert_dir: str, node_id: str) -> Tuple[Ed25519PrivateKey, str]:
    """
    Loads this node's Ed25519 keypair from `cert_dir` -- the same
    per-node persistent directory agent_daemon.py already uses for
    its mTLS certificate/key -- generating one on first use. Returns
    (private_key, public_key_pem). The private key never leaves this
    process; only the returned PEM-encoded public key is ever sent
    anywhere (see RegisterRequest.ed25519_public_key_pem).
    """
    os.makedirs(cert_dir, exist_ok=True)
    key_path = Path(cert_dir) / f"agent-{node_id}.ed25519.key.pem"

    if key_path.exists():
        private_key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
    else:
        private_key = Ed25519PrivateKey.generate()
        key_path.write_bytes(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        try:
            os.chmod(key_path, 0o600)
        except OSError:
            pass  # best-effort; not every platform/filesystem supports this

    public_key_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    return private_key, public_key_pem


def build_attestation_payload(
    *, job_id: str, attempt_id: str, node_id: str, job_spec_hash: str,
    output_hash: str, status: str, timestamp: str,
    execution_backend: Optional[str] = None,
) -> Dict[str, Any]:
    """
    The exact fields a worker attestation commits to.
    `job_spec_hash` binds the specific command this node actually
    received in its own JobAssign -- not whatever the coordinator
    later claims it dispatched. `output_hash` is the node's own
    attestation of its own stdout, computed independently of (though
    expected to agree with) whatever the coordinator separately
    computes from the same bytes once the result arrives.

    `execution_backend` ("docker" or "subprocess") is how the node ran
    the job: inside a container, or as a raw host subprocess with the
    worker's own privileges. Signing it makes it the NODE's statement,
    which neither the coordinator nor anyone relaying the receipt can
    alter or invent without breaking the signature. It is still the
    node's own claim -- a dishonest node could sign "docker" while
    running the job raw -- so it proves who said it and that it wasn't
    tampered with, not that it is true. Left out of the payload
    entirely when not given (an older worker), so a payload from before
    this field existed is byte-for-byte what it always was, and every
    receipt already issued keeps verifying: verification checks the
    payload stored with the receipt, exactly as signed.
    """
    payload = {
        "job_id": job_id,
        "attempt_id": attempt_id,
        "node_id": node_id,
        "job_spec_hash": job_spec_hash,
        "output_hash": output_hash,
        "status": status,
        "timestamp": timestamp,
    }
    if execution_backend is not None:
        payload["execution_backend"] = execution_backend
    return payload


def _canonical_bytes(payload: Dict[str, Any]) -> bytes:
    # Matches gcon.execution.verifier.ExecutionVerifier's own
    # canonicalization (json.dumps(sort_keys=True)) -- not because
    # this ever signs the same payload the HMAC scheme does (it
    # doesn't; this is an entirely separate, additive scheme), but so
    # a reader auditing both doesn't have to wonder whether the two
    # canonicalizations could ever quietly disagree.
    return json.dumps(payload, sort_keys=True).encode()


def sign_attestation(private_key: Ed25519PrivateKey, payload: Dict[str, Any]) -> str:
    """Returns a base64-encoded Ed25519 signature over `payload`,
    made by `private_key`."""
    signature = private_key.sign(_canonical_bytes(payload))
    return base64.b64encode(signature).decode()


def verify_attestation(public_key_pem: str, payload: Dict[str, Any], signature_b64: str) -> bool:
    """
    True iff `signature_b64` is a valid Ed25519 signature, made by
    the holder of the private key matching `public_key_pem`, over
    exactly `payload`. False for any malformed input (bad PEM, bad
    base64, wrong key type, altered payload, wrong signature) rather
    than raising -- a verifier should get a clean yes/no, not need to
    catch half a dozen different exception types itself.
    """
    try:
        public_key = serialization.load_pem_public_key(public_key_pem.encode())
        if not isinstance(public_key, Ed25519PublicKey):
            return False
        signature = base64.b64decode(signature_b64)
        public_key.verify(signature, _canonical_bytes(payload))
        return True
    except (InvalidSignature, ValueError, TypeError):
        return False
