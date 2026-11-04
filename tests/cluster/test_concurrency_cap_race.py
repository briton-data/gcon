"""
The concurrent-job cap and the duplicate-id check are decided and applied as one step.

Before, submit_job() counted the organization's in-flight jobs, released the lock, and inserted the job later, so
simultaneous submissions all saw room and all got in: a cap of 3 could be exceeded by dozens. The same gap let two
submissions of one job id both pass the "already exists" check, the second silently replacing the first.
"""
import threading
import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator, ConcurrentJobLimitError
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def coord(tmp_path):
    c = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    c.pause_scheduler()
    c.org_job_cap_resolver = lambda org_id: 3
    yield c
    c.shutdown()


def _submit_all(coord, ids, org="acme"):
    barrier = threading.Barrier(len(ids))
    outcome = {}

    def go(job_id):
        barrier.wait()
        try:
            coord.submit_job(job_id, "echo hi", org_id=org)
            outcome[job_id] = "accepted"
        except ConcurrentJobLimitError:
            outcome[job_id] = "capped"
        except Exception as exc:                      # anything else is a failure of the test, not the cap
            outcome[job_id] = f"error: {exc!r}"

    threads = [threading.Thread(target=go, args=(i,)) for i in ids]
    [t.start() for t in threads]
    [t.join(30) for t in threads]
    return outcome


class TestCapHoldsUnderSimultaneousSubmission:
    def test_a_burst_never_gets_more_than_the_cap_in(self, coord):
        outcome = _submit_all(coord, [f"j{i}" for i in range(40)])
        assert not [v for v in outcome.values() if v.startswith("error")]
        assert coord.count_inflight_jobs("acme") == 3
        assert list(outcome.values()).count("accepted") == 3

    def test_even_when_a_thread_switch_lands_exactly_between_the_count_and_the_insert(self, coord, monkeypatch):
        real_count = coord.count_inflight_jobs

        def count_then_yield(org_id):
            n = real_count(org_id)
            time.sleep(0.02)                   # every submitter has now looked and seen the same number
            return n

        monkeypatch.setattr(coord, "count_inflight_jobs", count_then_yield)
        outcome = _submit_all(coord, [f"j{i}" for i in range(20)])
        assert real_count("acme") == 3
        assert list(outcome.values()).count("accepted") == 3

    def test_other_organizations_have_their_own_room(self, coord):
        _submit_all(coord, [f"a{i}" for i in range(10)], org="acme")
        outcome = _submit_all(coord, [f"g{i}" for i in range(10)], org="globex")
        assert list(outcome.values()).count("accepted") == 3
        assert coord.count_inflight_jobs("acme") == 3 and coord.count_inflight_jobs("globex") == 3

    def test_a_job_that_finishes_frees_room(self, coord):
        _submit_all(coord, ["a", "b", "c"])
        coord.jobs["a"]["status"] = "completed"
        coord.submit_job("d", "echo hi", org_id="acme")
        assert coord.count_inflight_jobs("acme") == 3


class TestOneJobIdIsAcceptedOnce:
    def test_simultaneous_submissions_of_one_id_admit_exactly_one(self, coord, monkeypatch, tmp_path):
        coord.org_job_cap_resolver = None
        data = tmp_path / "input.txt"
        data.write_text("x")
        # Look at the "already exists" answer, then let every other submitter look too.
        real_register = coord.artifact_registry.register_artifact

        def slow_register(*a, **k):
            time.sleep(0.02)
            return real_register(*a, **k)

        monkeypatch.setattr(coord.artifact_registry, "register_artifact", slow_register)
        barrier = threading.Barrier(12)
        accepted, refused = [], []

        def go(n):
            barrier.wait()
            try:
                coord.submit_job("same-id", f"echo {n}", org_id="acme", artifacts=[str(data)])
                accepted.append(n)
            except ValueError:
                refused.append(n)

        threads = [threading.Thread(target=go, args=(i,)) for i in range(12)]
        [t.start() for t in threads]
        [t.join(30) for t in threads]
        assert len(accepted) == 1 and len(refused) == 11
        assert coord.jobs["same-id"]["command"] == f"echo {accepted[0]}"
