"""Structured logging: leveled coordinator lines and JSON output with correlation ids."""
import io
import json
import logging

from gcon.cluster import coordinator as coordinator_module
from gcon.cluster.coordinator import GCONCoordinator
from gcon.monitoring.logfmt import JsonFormatter, apply_log_format_from_env


def _capture(formatter=None):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    if formatter:
        handler.setFormatter(formatter)
    logger = logging.getLogger("gcon.coordinator")
    logger.addHandler(handler)
    old_level = logger.level
    logger.setLevel(logging.INFO)
    return stream, handler, logger, old_level


def test_log_line_keeps_text_and_picks_level(caplog):
    with caplog.at_level(logging.INFO, logger="gcon.coordinator"):
        coordinator_module._log_line("[QUEUE] Dispatching job-1")
        coordinator_module._log_line("[WARN] Could not look up key for 'n1'")
        coordinator_module._log_line("Recovery failed for 'j' -- requeuing.")
        coordinator_module._log_line("[QUARANTINE] node 'n1' auto-quarantined")
    levels = [(r.levelname, r.getMessage()) for r in caplog.records if r.name == "gcon.coordinator"]
    assert levels == [
        ("INFO", "[QUEUE] Dispatching job-1"),
        ("WARNING", "[WARN] Could not look up key for 'n1'"),
        ("WARNING", "Recovery failed for 'j' -- requeuing."),
        ("WARNING", "[QUARANTINE] node 'n1' auto-quarantined"),
    ]


def test_coordinator_has_no_bare_print_calls():
    import ast, inspect
    tree = ast.parse(inspect.getsource(coordinator_module))
    calls = [n for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "print"]
    assert calls == []


def test_json_format_carries_correlation_ids_from_telemetry(tmp_path):
    from gcon.persistence import ControlPlane
    c = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "t.db")))
    stream, handler, logger, old = _capture(JsonFormatter())
    try:
        c.telemetry.warning("dispatch trouble", event_type="job_dispatch_failed",
                            trace_id="trace-9", job_id="job-9", node_id="node-9")
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old)
        c.shutdown()
    # (the job/node don't exist, so telemetry also logs its own persistence
    # warning after ours -- pick our line by message, not by position)
    entries = [json.loads(line) for line in stream.getvalue().strip().splitlines()]
    entry = next(e for e in entries if e["message"] == "dispatch trouble")
    assert entry["level"] == "WARNING"
    assert (entry["trace_id"], entry["job_id"], entry["node_id"]) == ("trace-9", "job-9", "node-9")
    assert entry["event_type"] == "job_dispatch_failed"


def test_json_format_omits_absent_ids():
    record = logging.LogRecord("gcon.x", logging.INFO, __file__, 1, "plain line", None, None)
    entry = json.loads(JsonFormatter().format(record))
    assert entry["message"] == "plain line"
    assert "job_id" not in entry and "trace_id" not in entry


def test_env_switch_only_when_requested(monkeypatch):
    root = logging.getLogger()
    handler = logging.StreamHandler(io.StringIO())
    root.addHandler(handler)
    try:
        monkeypatch.delenv("GCON_LOG_FORMAT", raising=False)
        assert apply_log_format_from_env() is False
        assert not isinstance(handler.formatter, JsonFormatter)
        monkeypatch.setenv("GCON_LOG_FORMAT", "json")
        assert apply_log_format_from_env() is True
        assert isinstance(handler.formatter, JsonFormatter)
    finally:
        root.removeHandler(handler)
