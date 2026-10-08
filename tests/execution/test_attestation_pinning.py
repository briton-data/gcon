"""
The attestation key embedded next to a signature proves nothing about who
signed: an attacker can generate a keypair, sign whatever they like and embed
the matching public key. The coordinator therefore pins verification to the key
the node enrolled with, and the attestation must agree with the receipt.
"""
import pytest
from cryptography.hazmat.primitives import serialization

from gcon.execution.verifier import ExecutionVerifier
from gcon.execution.worker_identity import build_attestation_payload, ensure_node_keypair, sign_attestation


def _keypair(tmp_path, name):
    d = tmp_path / name
    d.mkdir()
    return ensure_node_keypair(str(d), "node-1")


def _receipt(private_key, public_pem, *, job_id="j1", node_id="node-1", status="success", output_hash="oh",
             receipt_status="success", receipt_output_hash="oh", agent_id="node-1"):
    payload = build_attestation_payload(
        job_id=job_id, attempt_id="a1", node_id=node_id, job_spec_hash="h",
        output_hash=output_hash, status=status, timestamp="t", execution_backend="docker",
    )
    return {
        "job_id": "j1", "agent_id": agent_id, "status": receipt_status, "output_hash": receipt_output_hash,
        "worker_attestation": {
            "payload": payload, "signature": sign_attestation(private_key, payload), "public_key_pem": public_pem,
        },
    }


@pytest.fixture
def keys(tmp_path):
    honest = _keypair(tmp_path, "honest")
    attacker = _keypair(tmp_path, "attacker")
    return honest, attacker


def test_an_honest_attestation_verifies_against_the_registered_key(keys):
    (key, pem), _ = keys
    ok, msg = ExecutionVerifier.validate_worker_attestation(
        _receipt(key, pem), registered_public_key_pem=pem, require_registered_key=True)
    assert ok, msg


def test_an_attestation_signed_with_an_attackers_own_key_is_rejected(keys):
    (_, honest_pem), (attacker_key, attacker_pem) = keys
    forged = _receipt(attacker_key, attacker_pem)             # self-consistent: signature matches embedded key
    # With no pinned key it still "verifies" -- which is exactly the hole.
    assert ExecutionVerifier.validate_worker_attestation(forged)[0] is True
    ok, msg = ExecutionVerifier.validate_worker_attestation(forged, registered_public_key_pem=honest_pem)
    assert not ok and "registered" in msg


def test_a_node_with_no_registered_key_cannot_be_verified_when_one_is_required(keys):
    (key, pem), _ = keys
    ok, msg = ExecutionVerifier.validate_worker_attestation(_receipt(key, pem), require_registered_key=True)
    assert not ok and "No registered key" in msg


@pytest.mark.parametrize("overrides,word", [
    ({"receipt_output_hash": "tampered"}, "output_hash"),
    ({"receipt_status": "failed"}, "status"),
    ({"agent_id": "someone-else"}, "node_id"),
])
def test_the_attestation_must_agree_with_the_receipt(keys, overrides, word):
    (key, pem), _ = keys
    ok, msg = ExecutionVerifier.validate_worker_attestation(
        _receipt(key, pem, **overrides), registered_public_key_pem=pem, require_registered_key=True)
    assert not ok and word in msg


class TestReceiptsDoNotExpire:
    def _proof(self, verifier, timestamp):
        proof = {"job_id": "j1", "agent_id": "n", "output_hash": "o", "timestamp": timestamp, "key_id": None}
        proof["signature"] = verifier.sign_data({k: v for k, v in proof.items()})
        return proof

    def test_an_old_receipt_is_still_valid(self):
        from datetime import datetime, timedelta, UTC
        verifier = ExecutionVerifier(secret_key="k")
        old = (datetime.now(UTC) - timedelta(days=400)).isoformat()
        proof = self._proof(verifier, old)
        assert verifier.validate_proof(proof)[0] is True
        assert verifier.validate_proof(proof, max_age_seconds=3600) == (False, "Proof timestamp is too old")

    def test_a_malformed_timestamp_is_still_refused(self):
        verifier = ExecutionVerifier(secret_key="k")
        assert verifier.validate_proof(self._proof(verifier, "not-a-date"))[0] is False


class TestReceiptFieldsAreCoveredBySignature:
    def _receipt(self, verifier, **result):
        return verifier.create_receipt(
            "j1", "node-1", {"status": "success", "metrics": {}, "runtime_seconds": 1, **result},
            input_hash="ih", output_hash="oh",
        )

    def test_a_fresh_receipt_validates_and_its_fields_match(self):
        verifier = ExecutionVerifier(secret_key="k")
        receipt = self._receipt(verifier)
        assert verifier.validate_proof(receipt["proof"])[0]
        assert verifier.validate_receipt_fields(receipt)[0]

    @pytest.mark.parametrize("field,value", [
        ("status", "success-but-edited"), ("agent_id", "another-node"),
        ("output_hash", "edited"), ("input_hash", "edited"), ("job_id", "another-job"),
    ])
    def test_editing_a_top_level_field_is_detected(self, field, value):
        verifier = ExecutionVerifier(secret_key="k")
        receipt = self._receipt(verifier)
        receipt[field] = value
        ok, msg = verifier.validate_receipt_fields(receipt)
        assert not ok and field in msg

    def test_a_receipt_made_before_agent_id_and_status_were_signed_still_validates(self):
        verifier = ExecutionVerifier(secret_key="k")
        receipt = self._receipt(verifier)
        legacy_proof = {k: v for k, v in receipt["proof"].items() if k not in ("agent_id", "status", "signature")}
        legacy_proof["signature"] = verifier.sign_data(legacy_proof)
        receipt["proof"] = legacy_proof
        assert verifier.validate_proof(legacy_proof)[0] and verifier.validate_receipt_fields(receipt)[0]
