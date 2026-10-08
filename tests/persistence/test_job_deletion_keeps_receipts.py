"""
receipts.job_id is ON DELETE CASCADE, so deleting a job used to delete its
signed receipts: setting only GCON_DB_MAX_TERMINAL_JOBS=2 took receipts from 6
to 2, although receipt retention is meant to be its own, opt-in setting. Every
job deletion now skips a job that still has a receipt.
"""
import pytest

from gcon.persistence.control_plane import ControlPlane
from gcon.persistence.retention import RetentionPolicy


@pytest.fixture
def plane(tmp_path):
    p = ControlPlane(path=str(tmp_path / "cp.db"))
    for i in range(6):
        p.jobs.ensure_exists(f"job-{i}", "echo hi", org_id="org-a")
        p.jobs.set_status(f"job-{i}", "completed")
        p.receipts.upload(f"job-{i}", {"n": i}, f"hash-{i}")
    p.jobs.ensure_exists("no-receipt", "echo hi", org_id="org-a")
    p.jobs.set_status("no-receipt", "completed")
    yield p
    p.close()


def test_job_count_retention_does_not_delete_receipts(plane, monkeypatch):
    monkeypatch.setenv("GCON_DB_MAX_TERMINAL_JOBS", "2")
    monkeypatch.delenv("GCON_DB_MAX_RECEIPTS", raising=False)
    monkeypatch.delenv("GCON_DB_RETENTION_DAYS", raising=False)
    RetentionPolicy().sweep(plane)
    assert plane.receipts.count_all() == 6                      # all receipts survive


def test_age_based_job_purge_skips_jobs_with_receipts(plane):
    removed = plane.jobs.purge_terminal_older_than("2999-01-01T00:00:00+00:00")
    assert removed == 1                                         # only the job with no receipt
    assert plane.receipts.count_all() == 6
    assert plane.jobs.get("job-0") is not None


def test_clear_by_status_and_owned_delete_skip_jobs_with_receipts(plane):
    assert plane.jobs.delete_by_status("completed") == 1        # only "no-receipt"
    assert plane.jobs.delete_owned_terminal("job-1", "org-a", ["completed"]) == 0
    assert plane.receipts.count_all() == 6


def test_once_the_receipt_is_purged_its_job_becomes_deletable(plane):
    plane.receipts.purge_older_than("2999-01-01T00:00:00+00:00")
    assert plane.receipts.count_all() == 0
    assert plane.jobs.purge_terminal_older_than("2999-01-01T00:00:00+00:00") == 7


def test_ensure_exists_never_adopts_another_orgs_job(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "cp.db"))
    try:
        plane.jobs.ensure_exists("shared-id", "echo a", org_id="org-a")
        plane.jobs.ensure_exists("shared-id", "echo a", org_id="org-a")        # same owner: no-op
        with pytest.raises(ValueError, match="already exists"):
            plane.jobs.ensure_exists("shared-id", "echo b", org_id="org-b")
        assert plane.jobs.get("shared-id")["org_id"] == "org-a"
    finally:
        plane.close()
