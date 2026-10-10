"""A job's output is customer-controlled: it is capped at the worker and again
where the coordinator accepts it, instead of growing the coordinator by hundreds
of megabytes (3 jobs x 30 MB stdout took RSS from 59 to 419 MB)."""
import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.execution.output_limits import cap_result, cap_text, max_job_output_bytes


def test_small_output_is_untouched():
    assert cap_text("hello") == "hello" and cap_text("") == "" and cap_text(None) is None


def test_large_output_is_cut_with_a_marker(monkeypatch):
    monkeypatch.setenv("GCON_MAX_JOB_OUTPUT_BYTES", "1000")
    out = cap_text("x" * 5000)
    assert out.startswith("x" * 1000) and "4000 of 5000 bytes dropped" in out
    assert len(out) < 1100


def test_multibyte_text_is_cut_on_a_character_boundary(monkeypatch):
    monkeypatch.setenv("GCON_MAX_JOB_OUTPUT_BYTES", "10")
    out = cap_text("é" * 50)
    out.encode("utf-8")                                     # still valid text, never a split character
    assert "truncated" in out


@pytest.mark.parametrize("value", ["", "abc", "0", "-5"])
def test_a_bad_setting_falls_back_to_the_default(monkeypatch, value):
    monkeypatch.setenv("GCON_MAX_JOB_OUTPUT_BYTES", value)
    assert max_job_output_bytes() == 5 * 1024 * 1024


def test_cap_result_caps_both_streams_and_leaves_the_rest(monkeypatch):
    monkeypatch.setenv("GCON_MAX_JOB_OUTPUT_BYTES", "100")
    result = cap_result({"status": "success", "return_code": 0, "stdout": "a" * 500, "stderr": "b" * 500})
    assert "truncated" in result["stdout"] and "truncated" in result["stderr"]
    assert result["status"] == "success" and result["return_code"] == 0


def test_a_real_job_that_floods_stdout_comes_back_capped(monkeypatch):
    monkeypatch.setenv("GCON_MAX_JOB_OUTPUT_BYTES", "2000")
    coord = GCONCoordinator()
    try:
        coord.register_agent(GCONAgent(node_id="n1"))
        coord.submit_job("flood", "python3 -c \"print('y' * 200000)\"")
        deadline = time.time() + 20
        while time.time() < deadline and coord.jobs["flood"]["status"] != "completed":
            time.sleep(0.05)
        job = coord.jobs["flood"]
        assert job["status"] == "completed"
        assert len(job["result"]["stdout"]) < 2200 and "truncated" in job["result"]["stdout"]
    finally:
        coord.shutdown()


# ---- the worker holds only the cap, not the whole output -------------------

from gcon.execution.output_limits import BoundedCapture


def test_bounded_capture_keeps_the_first_bytes_and_counts_the_rest():
    capture = BoundedCapture(limit=10)
    for chunk in ("abcdef", "ghijkl", "mnop"):
        capture.feed(chunk)
    text = capture.text()
    assert text.startswith("abcdefghij") and "6 of 16 bytes dropped" in text


def test_bounded_capture_leaves_output_that_fits_untouched():
    capture = BoundedCapture(limit=100)
    capture.feed("hello ")
    capture.feed("world")
    assert capture.text() == "hello world"


def test_bounded_capture_cuts_on_a_character_boundary():
    capture = BoundedCapture(limit=5)
    capture.feed("é" * 10)              # 2 bytes each
    capture.text().encode("utf-8")      # never a split character
    assert "truncated" in capture.text()


def test_a_flood_far_over_the_cap_does_not_grow_the_worker(monkeypatch):
    """communicate() held the whole output (and a decoded copy) before any cap
    ran. 150 MB printed under a 1 MiB cap must now cost the worker about the cap."""
    import psutil

    monkeypatch.setenv("GCON_MAX_JOB_OUTPUT_BYTES", str(1024 * 1024))
    agent = GCONAgent(node_id="flood-worker")
    proc = psutil.Process()
    before = proc.memory_info().rss
    result = agent.execute_job(
        "flood-mem",
        "python3 -c \"import sys\nfor _ in range(150): sys.stdout.write('y' * 1048576)\"",
        timeout=60,
    )
    grown_mb = (proc.memory_info().rss - before) / (1024 * 1024)
    assert result["status"] == "success" and result["return_code"] == 0
    assert "truncated" in result["stdout"]
    assert f"{150 * 1048576 - 1048576} of {150 * 1048576} bytes dropped" in result["stdout"]
    assert grown_mb < 60, f"worker grew {grown_mb:.0f} MB for a 1 MiB cap"
