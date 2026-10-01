"""
Suite-wide test configuration.

GCON_SANDBOX_POLICY defaults to "required" in real deployments, which
only dispatches jobs to workers running the docker backend. Almost every
test here dispatches to an in-process GCONAgent (raw subprocess, no
container), so the suite declares the explicit "trusted" policy -- the
same opt-in an operator running only their own workloads would make.

Tests that exercise the sandbox policy itself override this per test with
monkeypatch.setenv("GCON_SANDBOX_POLICY", "required") -- see
tests/cluster/test_sandbox_policy.py. The variable is set through
monkeypatch so it is inherited by any subprocess a test spawns (the
multiprocess integration tests) and restored afterwards.
"""
import pytest


@pytest.fixture(autouse=True)
def _trusted_sandbox_policy_for_tests(monkeypatch):
    monkeypatch.setenv("GCON_SANDBOX_POLICY", "trusted")
