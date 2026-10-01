import pytest

from gcon.execution.worker_identity import (
    build_attestation_payload,
    ensure_node_keypair,
    sign_attestation,
    verify_attestation,
)


def _payload(**overrides):
    base = dict(
        job_id="job-1", attempt_id="attempt-1", node_id="node-1",
        job_spec_hash="spec-hash", output_hash="out-hash",
        status="success", timestamp="2026-01-01T00:00:00",
    )
    base.update(overrides)
    return build_attestation_payload(**base)


def test_valid_signature_verifies(tmp_path):
    priv, pub_pem = ensure_node_keypair(str(tmp_path), "node-1")
    payload = _payload()
    sig = sign_attestation(priv, payload)
    assert verify_attestation(pub_pem, payload, sig) is True


def test_altered_payload_fails_verification(tmp_path):
    priv, pub_pem = ensure_node_keypair(str(tmp_path), "node-1")
    payload = _payload()
    sig = sign_attestation(priv, payload)

    tampered = dict(payload)
    tampered["output_hash"] = "a-different-hash"
    assert verify_attestation(pub_pem, tampered, sig) is False


def test_wrong_public_key_fails_verification(tmp_path):
    priv, _ = ensure_node_keypair(str(tmp_path / "a"), "node-1")
    _, other_pub_pem = ensure_node_keypair(str(tmp_path / "b"), "node-2")
    payload = _payload()
    sig = sign_attestation(priv, payload)
    assert verify_attestation(other_pub_pem, payload, sig) is False


def test_malformed_signature_fails_cleanly_not_raises(tmp_path):
    _, pub_pem = ensure_node_keypair(str(tmp_path), "node-1")
    payload = _payload()
    assert verify_attestation(pub_pem, payload, "not-valid-base64!!!") is False
    assert verify_attestation("not a pem", payload, "AAAA") is False


def test_keypair_persists_across_calls(tmp_path):
    _, pub_pem_1 = ensure_node_keypair(str(tmp_path), "node-1")
    _, pub_pem_2 = ensure_node_keypair(str(tmp_path), "node-1")
    assert pub_pem_1 == pub_pem_2


def test_different_nodes_get_different_keys(tmp_path):
    _, pub_pem_a = ensure_node_keypair(str(tmp_path), "node-a")
    _, pub_pem_b = ensure_node_keypair(str(tmp_path), "node-b")
    assert pub_pem_a != pub_pem_b


# ------------------------------------------------ execution_backend is signed
def test_payload_omits_the_backend_when_not_given():
    """An older worker's payload must be exactly what it always was."""
    assert "execution_backend" not in _payload()
    assert sorted(_payload()) == [
        "attempt_id", "job_id", "job_spec_hash", "node_id", "output_hash", "status", "timestamp",
    ]


def test_payload_carries_the_backend_when_given():
    assert _payload(execution_backend="docker")["execution_backend"] == "docker"
    assert _payload(execution_backend="subprocess")["execution_backend"] == "subprocess"


def test_the_signature_covers_the_backend(tmp_path):
    """The point of putting it in the payload: nobody downstream can change
    what the node said about how it ran the job."""
    key, pub = ensure_node_keypair(str(tmp_path), "node-1")
    payload = _payload(execution_backend="docker")
    signature = sign_attestation(key, payload)
    assert verify_attestation(pub, payload, signature) is True

    claiming_sandbox_it_did_not_have = dict(payload, execution_backend="docker")
    hiding_that_it_ran_raw = dict(payload, execution_backend="subprocess")
    removed = {k: v for k, v in payload.items() if k != "execution_backend"}
    assert verify_attestation(pub, claiming_sandbox_it_did_not_have, signature) is True
    assert verify_attestation(pub, hiding_that_it_ran_raw, signature) is False
    assert verify_attestation(pub, removed, signature) is False


def test_a_payload_signed_before_the_field_existed_still_verifies(tmp_path):
    """Receipts issued before this change must keep verifying: verification
    checks the payload stored with the receipt, exactly as it was signed."""
    key, pub = ensure_node_keypair(str(tmp_path), "node-1")
    legacy = _payload()  # no execution_backend key at all
    assert verify_attestation(pub, legacy, sign_attestation(key, legacy)) is True
