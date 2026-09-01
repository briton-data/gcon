"""
Thin re-export. PolicyEngine now lives in production code at
gcon.execution.policy_engine (it was moved there and wired into
GCONCoordinator._run_job -- see that module's docstring for the full
history and design rationale). This file exists only so tests written
against `from tests.support.policy import PolicyEngine` keep working
unchanged.
"""
from gcon.execution.policy_engine import PolicyEngine  # noqa: F401
