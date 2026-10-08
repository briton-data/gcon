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


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_sandbox: use the real GCONAgent.sandboxed detection (no test-wide override)",
    )
    config.addinivalue_line(
        "markers",
        "real_ssrf_guard: keep the outbound-URL safety check fully on (no loopback allowance)",
    )


@pytest.fixture(autouse=True)
def _tests_may_call_local_webhook_servers(request, monkeypatch):
    """Webhook tests run a receiver on 127.0.0.1; the outbound-URL guard
    (gcon.transport.url_safety) refuses loopback unless told otherwise. Tests of
    the guard itself opt out with @pytest.mark.real_ssrf_guard."""
    if not request.node.get_closest_marker("real_ssrf_guard"):
        monkeypatch.setenv("GCON_WEBHOOK_ALLOW_PRIVATE_TARGETS", "1")


@pytest.fixture(autouse=True)
def _in_process_agents_count_as_sandboxed(request, monkeypatch):
    """
    Public-API and organization-attributed jobs are only ever dispatched to a
    worker that runs jobs in a container, whatever GCON_SANDBOX_POLICY says
    (see GCONCoordinator._must_sandbox). Nearly every test here dispatches to an
    in-process GCONAgent (a raw subprocess, no Docker daemon), and what those
    tests exercise is orchestration, not isolation -- so they declare the
    in-process agent sandboxed.

    Tests of the sandbox rule itself opt out with @pytest.mark.real_sandbox
    and use the agent's real answer.
    """
    if request.node.get_closest_marker("real_sandbox"):
        return
    from gcon.execution.agent import GCONAgent
    monkeypatch.setattr(GCONAgent, "sandboxed", True)
