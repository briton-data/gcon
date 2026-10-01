"""
Unit tests for gcon.execution.assurance.synthesize_assurance -- pure
function, no coordinator involved. Covers every precedence level and
confirms absence of an optional signal (attestation/replication) never
by itself demotes the result below "verified".
"""
from gcon.execution.assurance import synthesize_assurance


def test_all_signals_pass_is_verified_and_assured():
    result = synthesize_assurance(
        crypto_verified=True,
        crypto_message="Proof is valid",
        worker_attestation={"verified": True, "verification_message": "ok"},
        policy_report={"trusted": True, "checks": []},
        execution_proof={"agreement": True, "mismatches": []},
    )
    assert result["level"] == "verified"
    assert result["assured"] is True
    assert result["signals"]["crypto"] == {"applicable": True, "passed": True}
    assert result["signals"]["worker_attestation"] == {"applicable": True, "passed": True}
    assert result["signals"]["policy"] == {"applicable": True, "passed": True}
    assert result["signals"]["replication"] == {"applicable": True, "passed": True}


def test_no_optional_signals_present_is_still_verified():
    """A plain single-node job with no attestation block and no
    replication -- neither is a failure, both are just not present."""
    result = synthesize_assurance(
        crypto_verified=True,
        crypto_message="Proof is valid",
        worker_attestation=None,
        policy_report=None,
        execution_proof=None,
    )
    assert result["level"] == "verified"
    assert result["assured"] is True
    assert result["signals"]["worker_attestation"] == {"applicable": False, "passed": None}
    assert result["signals"]["replication"] == {"applicable": False, "passed": None}


def test_invalid_crypto_signature_wins_over_everything_else():
    """Even if attestation/policy/replication would all otherwise
    pass, a bad signature means nothing else can be trusted."""
    result = synthesize_assurance(
        crypto_verified=False,
        crypto_message="Invalid signature",
        worker_attestation={"verified": True, "verification_message": "ok"},
        policy_report={"trusted": True, "checks": []},
        execution_proof={"agreement": True, "mismatches": []},
    )
    assert result["level"] == "invalid"
    assert result["assured"] is False
    assert result["reasons"] == ["Invalid signature"]


def test_invalid_crypto_with_no_message_gets_a_default_reason():
    result = synthesize_assurance(crypto_verified=False, crypto_message=None)
    assert result["level"] == "invalid"
    assert result["reasons"] == ["Cryptographic signature is invalid"]


def test_replica_disagreement_outranks_policy_violation():
    result = synthesize_assurance(
        crypto_verified=True,
        crypto_message="Proof is valid",
        worker_attestation=None,
        policy_report={"trusted": False, "checks": [
            {"name": "Runtime Policy", "passed": False, "message": "too slow"}
        ]},
        execution_proof={"agreement": False, "mismatches": [{"field": "output_hash", "values": ["a", "b"]}]},
    )
    assert result["level"] == "disputed"
    assert result["assured"] is False
    assert "output_hash" in result["reasons"][0]


def test_attestation_mismatch_outranks_policy_violation_but_not_dispute():
    result = synthesize_assurance(
        crypto_verified=True,
        crypto_message="Proof is valid",
        worker_attestation={"verified": False, "verification_message": "signature invalid"},
        policy_report={"trusted": False, "checks": [
            {"name": "Runtime Policy", "passed": False, "message": "too slow"}
        ]},
        execution_proof=None,
    )
    assert result["level"] == "attestation_mismatch"
    assert result["reasons"] == ["signature invalid"]


def test_policy_violation_alone_is_lowest_severity_failure():
    result = synthesize_assurance(
        crypto_verified=True,
        crypto_message="Proof is valid",
        worker_attestation={"verified": True, "verification_message": "ok"},
        policy_report={"trusted": False, "checks": [
            {"name": "Runtime Policy", "passed": False, "message": "too slow"},
            {"name": "CPU Policy", "passed": True, "message": "fine"},
        ]},
        execution_proof={"agreement": True, "mismatches": []},
    )
    assert result["level"] == "policy_violation"
    assert result["reasons"] == ["too slow"]


def test_policy_violation_with_no_failed_check_messages_gets_a_default_reason():
    result = synthesize_assurance(
        crypto_verified=True,
        policy_report={"trusted": False, "checks": []},
    )
    assert result["level"] == "policy_violation"
    assert result["reasons"] == ["Policy evaluation reported this job as untrusted"]


def test_agreeing_replication_does_not_block_verified():
    result = synthesize_assurance(
        crypto_verified=True,
        crypto_message="Proof is valid",
        execution_proof={"agreement": True, "mismatches": []},
    )
    assert result["level"] == "verified"
    assert result["assured"] is True
