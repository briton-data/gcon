"""
Optional JSON log output with correlation IDs.

Set GCON_LOG_FORMAT=json and every log line becomes one JSON object, so a
log aggregator can filter by job_id / trace_id / node_id instead of grepping
free text. Lifecycle events already carry those ids (telemetry.py attaches
them as record attributes); this only changes how they are rendered. With
the variable unset, log output is exactly what it was.
"""
import json
import logging
import os
from datetime import datetime, UTC

# Attributes telemetry.py attaches to a log record for correlation.
CORRELATION_FIELDS = ("trace_id", "job_id", "node_id", "org_id", "attempt_id", "event_type")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in CORRELATION_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                entry[field] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def apply_log_format_from_env() -> bool:
    """Switch the root handlers to JSON when GCON_LOG_FORMAT=json. Returns True if switched."""
    if os.environ.get("GCON_LOG_FORMAT", "").strip().lower() != "json":
        return False
    formatter = JsonFormatter()
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)
    return True
