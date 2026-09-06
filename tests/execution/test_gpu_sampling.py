"""
Tests for periodic GPU sampling during job execution (agent.py's
_watch_gpu_metrics / _merge_gpu_samples) -- previously GPU metrics
were a single post-completion snapshot; a job that spiked and settled
was invisible to its own receipt. These tests use a fake detect_gpu()
so the "spike and settle" pattern is deterministic instead of relying
on real hardware being present in the test environment.
"""

import os
import time

import pytest

from gcon.execution.agent import GCONAgent


@pytest.fixture
def fast_sampling(monkeypatch):
    monkeypatch.setenv("GCON_GPU_SAMPLE_INTERVAL_SECONDS", "0.1")


class TestPeriodicGpuSampling:
    def test_multiple_samples_taken_over_a_running_job(self, fast_sampling):
        agent = GCONAgent("node-1")
        result = agent.execute_job("job-1", "sleep 0.6", timeout=5)

        assert result["status"] == "success"
        assert result["metrics"]["gpu_sample_count"] >= 3, (
            "a 0.6s job at a 0.1s sample interval should have several samples, "
            "not just one snapshot at completion"
        )

    def test_spike_and_settle_is_captured_in_peak(self, fast_sampling, monkeypatch):
        agent = GCONAgent("node-1")

        # Fake a GPU that spikes to 90% load partway through, then
        # settles back down -- the exact scenario a single
        # post-completion snapshot would miss entirely.
        call_count = {"n": 0}
        real_detect_gpu = agent.detect_gpu

        def fake_detect_gpu():
            call_count["n"] += 1
            load = 0.9 if call_count["n"] == 3 else 0.1
            return {
                "gpu_id": 0, "gpu_name": "Fake GPU", "memory_total": 16000,
                "memory_available": 8000, "memory_used": 8000 if call_count["n"] == 3 else 1000,
                "load": load, "temperature": 60,
            }

        monkeypatch.setattr(agent, "detect_gpu", fake_detect_gpu)

        result = agent.execute_job("job-2", "sleep 0.6", timeout=5)

        # The FINAL sample (whatever it lands on) would show low
        # load/memory if this were still a single end-of-job
        # snapshot -- the peak fields must reflect the spike even
        # though it wasn't the last reading taken.
        assert result["metrics"]["gpu_utilization_percent_peak"] == 90.0
        assert result["metrics"]["gpu_memory_peak"] == 8000
        assert any(s["gpu_utilization_percent"] == 90.0 for s in result["metrics"]["gpu_samples"])

    def test_partial_samples_survive_a_timeout(self, fast_sampling):
        agent = GCONAgent("node-1")
        result = agent.execute_job("job-3", "sleep 5", timeout=1)

        assert result["status"] == "timeout"
        # Real, partial GPU evidence gathered before the kill --
        # previously the timeout branch had no "metrics" key at all.
        assert "metrics" in result
        assert result["metrics"]["gpu_sample_count"] >= 1

    def test_sample_retention_is_bounded(self, monkeypatch):
        monkeypatch.setenv("GCON_GPU_SAMPLE_INTERVAL_SECONDS", "0.05")
        monkeypatch.setenv("GCON_MAX_GPU_SAMPLES_IN_RECEIPT", "5")
        agent = GCONAgent("node-1")

        result = agent.execute_job("job-4", "sleep 0.8", timeout=5)

        assert result["metrics"]["gpu_sample_count"] > 5, (
            "more samples than the retention cap should have actually been taken"
        )
        assert len(result["metrics"]["gpu_samples"]) <= 5, (
            "but the raw series retained in the result/receipt must be bounded"
        )

    def test_gpu_utilization_and_temperature_are_no_longer_discarded(self):
        agent = GCONAgent("node-1")
        metrics = agent.collect_metrics("job-5")
        d = metrics.to_dict()
        assert "gpu_utilization_percent" in d
        assert "gpu_temperature_c" in d

    def test_execution_metrics_backward_compatible_without_new_fields(self):
        # Existing callers that construct ExecutionMetrics without
        # the two new fields must still work (defaults apply).
        from gcon.execution.agent import ExecutionMetrics
        m = ExecutionMetrics(
            job_id="job-6", gpu_name="RTX 4090", gpu_memory_total=24576,
            gpu_memory_used=12288, cpu_percent=45.5, memory_percent=60.0,
            runtime_seconds=120.5, timestamp="2026-01-01T00:00:00",
        )
        assert m.gpu_utilization_percent == 0.0
        assert m.gpu_temperature_c == 0.0


class TestGpuDetectionCaching:
    """
    Regression test for a real failure found running the full suite
    on Windows: 150 concurrently-submitted jobs across a small shared
    node pool each spawned a GPU-sampling thread calling detect_gpu(),
    which shells out to nvidia-smi via GPUtil -- a subprocess spawn,
    expensive enough under that load to make
    test_submit_many_jobs_all_reach_terminal_state fail outright on
    Windows (subprocess creation there is much heavier than POSIX
    fork). Fixed with a short TTL cache in detect_gpu() -- these tests
    cover the cache itself, not the original failing scenario (that's
    covered by re-running stress_test1.py::TestLoad, unchanged).
    """

    def test_concurrent_calls_within_ttl_share_one_real_detection(self, monkeypatch):
        monkeypatch.setenv("GCON_GPU_CACHE_TTL_SECONDS", "5.0")
        agent = GCONAgent("node-1")

        call_count = {"n": 0}
        orig = agent._detect_gpu_uncached
        def counted():
            call_count["n"] += 1
            return orig()
        agent._detect_gpu_uncached = counted

        for _ in range(20):
            agent.detect_gpu()

        assert call_count["n"] == 1, "20 calls within one TTL window should mean exactly 1 real detection"

    def test_cache_expires_after_ttl(self, monkeypatch):
        monkeypatch.setenv("GCON_GPU_CACHE_TTL_SECONDS", "0.1")
        agent = GCONAgent("node-1")

        call_count = {"n": 0}
        orig = agent._detect_gpu_uncached
        def counted():
            call_count["n"] += 1
            return orig()
        agent._detect_gpu_uncached = counted

        agent.detect_gpu()
        time.sleep(0.15)
        agent.detect_gpu()

        assert call_count["n"] == 2, "a call after the TTL expires must trigger a fresh real detection"

    def test_cached_result_has_the_same_shape_as_a_fresh_one(self):
        agent = GCONAgent("node-1")
        fresh = agent._detect_gpu_uncached()
        cached = agent.detect_gpu()
        assert set(fresh.keys()) == set(cached.keys())

