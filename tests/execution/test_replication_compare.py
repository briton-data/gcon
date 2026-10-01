"""
compare_results() -- replicated-execution agreement.

Agreement is decided by output_hash alone. Timing is reported but never
decides it: measured on the real coordinator, 18 of 20 identical `echo hi`
runs on two healthy nodes used to come out "disputed" purely because
runtime_seconds differed by more than 2%.
"""
import pytest

from gcon.execution.replication import compare_results

H = "a" * 64
OTHER = "b" * 64


def _r(output_hash=H, runtime=1.0):
    result = {}
    if output_hash is not None:
        result["output_hash"] = output_hash
    result["metrics"] = {} if runtime is None else {"runtime_seconds": runtime}
    return result


class TestAgreementIsDecidedByOutput:
    def test_identical_output_agrees(self):
        r = compare_results([_r(), _r()])
        assert r["agree"] is True
        assert r["compared_fields"] == ["output_hash"]
        assert r["mismatches"] == []

    def test_different_output_disagrees(self):
        r = compare_results([_r(H), _r(OTHER)])
        assert r["agree"] is False
        assert [m["field"] for m in r["mismatches"]] == ["output_hash"]

    @pytest.mark.parametrize("a,b", [(1.0, 1.03), (0.010, 0.013), (120.0, 131.0), (0.001, 5.0)])
    def test_runtime_difference_never_causes_disagreement(self, a, b):
        assert compare_results([_r(runtime=a), _r(runtime=b)])["agree"] is True

    def test_one_odd_output_among_three_disagrees(self):
        assert compare_results([_r(H), _r(H), _r(OTHER)])["agree"] is False


class TestRuntimeIsStillReported:
    def test_large_spread_is_listed_as_an_outlier_but_not_a_mismatch(self):
        r = compare_results([_r(runtime=1.0), _r(runtime=2.0)])
        assert r["agree"] is True and r["mismatches"] == []
        assert r["max_deviation"] == pytest.approx(0.5)
        assert [o["field"] for o in r["metric_outliers"]] == ["runtime_seconds"]

    def test_small_spread_is_not_an_outlier(self):
        r = compare_results([_r(runtime=1.0), _r(runtime=1.01)])
        assert r["metric_outliers"] == []

    def test_tolerance_only_moves_the_outlier_threshold(self):
        strict = compare_results([_r(runtime=1.0), _r(runtime=1.5)], tolerance=0.01)
        loose = compare_results([_r(runtime=1.0), _r(runtime=1.5)], tolerance=1000)
        assert strict["metric_outliers"] and not loose["metric_outliers"]
        assert strict["agree"] is loose["agree"] is True  # never decides agreement

    def test_deviation_does_not_depend_on_replica_order(self):
        # Measured against results[0] this pair was 2.01% one way and
        # 1.97% the other, so the verdict flipped with completion order.
        forward = compare_results([_r(runtime=1.0), _r(runtime=1.0201)])
        backward = compare_results([_r(runtime=1.0201), _r(runtime=1.0)])
        assert forward["max_deviation"] == pytest.approx(backward["max_deviation"])
        assert bool(forward["metric_outliers"]) == bool(backward["metric_outliers"])

    def test_zero_runtimes_do_not_divide_by_zero(self):
        r = compare_results([_r(runtime=0.0), _r(runtime=0.0)])
        assert r["agree"] is True and r["max_deviation"] == 0.0


class TestNothingComparedIsNotAgreement:
    def test_no_output_hash_on_any_replica(self):
        r = compare_results([_r(None, 1.0), _r(None, 1.0)])
        assert r["agree"] is False
        assert r["compared_fields"] == []
        assert "nothing could be compared" in r["mismatches"][0]["reason"]

    def test_output_hash_missing_on_only_one_replica(self):
        r = compare_results([_r(H), _r(None)])
        assert r["agree"] is False
        assert r["compared_fields"] == []

    def test_no_data_at_all(self):
        assert compare_results([{}, {}])["agree"] is False


class TestTooFewReplicas:
    @pytest.mark.parametrize("n", [0, 1])
    def test_cannot_agree_with_nobody(self, n):
        r = compare_results([_r() for _ in range(n)])
        assert r["agree"] is False
        assert "need at least 2" in r["mismatches"][0]["reason"]
        assert r["metric_outliers"] == []
