"""
A job's stdout/stderr is customer-controlled and unbounded. Held whole in memory,
sent in one gRPC message (64 MB maximum) and stored with the job, a single job
printing tens of megabytes grew the coordinator by hundreds of megabytes and
could fail result delivery outright. Output is therefore capped, at the worker
that produced it and again where the coordinator accepts it (older workers, or
a worker that does not cap).

GCON_MAX_JOB_OUTPUT_BYTES (default 5 MiB, per stream) sets the cap. Over the
cap, the FIRST bytes are kept and a marker says how much was dropped; the job's
status and exit code are never changed by truncation.
"""
import os

DEFAULT_MAX_JOB_OUTPUT_BYTES = 5 * 1024 * 1024


def max_job_output_bytes() -> int:
    raw = os.environ.get("GCON_MAX_JOB_OUTPUT_BYTES", "").strip()
    try:
        value = int(raw) if raw else DEFAULT_MAX_JOB_OUTPUT_BYTES
    except ValueError:
        return DEFAULT_MAX_JOB_OUTPUT_BYTES
    return value if value > 0 else DEFAULT_MAX_JOB_OUTPUT_BYTES


def cap_text(text, limit=None):
    """Return `text` unchanged if it fits, else its first `limit` bytes (cut on a
    character boundary) followed by a truncation marker."""
    if not isinstance(text, str) or not text:
        return text
    limit = limit or max_job_output_bytes()
    raw = text.encode("utf-8", errors="replace")
    if len(raw) <= limit:
        return text
    kept = raw[:limit].decode("utf-8", errors="ignore")
    return kept + f"\n[output truncated: {len(raw) - limit} of {len(raw)} bytes dropped]"


def cap_result(result):
    """Cap the stdout/stderr of an execution-result dict (in place); returns it."""
    if isinstance(result, dict):
        for field in ("stdout", "stderr"):
            if field in result:
                result[field] = cap_text(result[field])
    return result
