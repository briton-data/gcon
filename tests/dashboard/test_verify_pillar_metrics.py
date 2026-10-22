"""
Verify pillar counts: only independent replicated execution is
"verified". A job that merely finished (and got a signed receipt) is not.
"""
from types import SimpleNamespace

from gcon.dashboard.presentation import PresentationLayer


def _metrics(jobs):
    coordinator = SimpleNamespace(
        get_jobs=lambda: jobs, get_nodes=lambda: [], autoscaler=None,
    )
    return PresentationLayer(coordinator).get_dashboard_metrics()["verification"]


def test_finished_jobs_without_verify_are_not_verified():
    jobs = [{"status": "completed"} for _ in range(5)]
    assert _metrics(jobs) == {
        "verified": 0, "disputed": 0, "unavailable": 0,
        "awaiting": 0, "not_requested": 5,
    }


def test_replicated_outcomes_are_counted_separately():
    verify = {"replicas": 2}
    jobs = [
        {"status": "completed", "verify": verify, "verification": {"outcome": "agreed"}},
        {"status": "completed", "verify": verify, "verification": {"outcome": "disputed"}},
        {"status": "completed", "verify": verify, "verification": {"outcome": "unavailable"}},
        {"status": "completed", "verify": verify, "verification": None},
        {"status": "pending", "verify": verify, "verification": None},
        {"status": "running", "verify": verify, "verification": None},
        {"status": "failed", "verify": verify, "verification": None},
        {"status": "completed"},
    ]
    assert _metrics(jobs) == {
        "verified": 1, "disputed": 1, "unavailable": 2,
        "awaiting": 2, "not_requested": 1,
    }
