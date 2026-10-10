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


class BoundedCapture:
    """Collects a stream's text but only ever HOLDS the first `limit` bytes.

    cap_text() above bounds what is sent and stored, but it runs after the whole
    output is already in memory: `Popen.communicate()` buffers everything the job
    printed first. A job printing gigabytes therefore exhausted the WORKER's
    memory (a customer's own machine, or a shared-pool host) before any cap ran.
    This is fed chunk by chunk while the job runs, keeps the first `limit`
    bytes, and only counts the rest, so memory stays at the cap however much the
    job prints. The result is the same shape cap_text() produces (the kept bytes
    plus a marker saying how much was dropped).
    """

    def __init__(self, limit=None):
        self.limit = limit or max_job_output_bytes()
        self._kept = []
        self._kept_bytes = 0
        self._total_bytes = 0

    def feed(self, chunk):
        if not chunk:
            return
        raw = chunk.encode("utf-8", errors="replace")
        self._total_bytes += len(raw)
        room = self.limit - self._kept_bytes
        if room <= 0:
            return
        if len(raw) <= room:
            self._kept.append(chunk)
            self._kept_bytes += len(raw)
        else:
            # Cut on a character boundary, as cap_text does.
            self._kept.append(raw[:room].decode("utf-8", errors="ignore"))
            self._kept_bytes += room

    def text(self):
        kept = "".join(self._kept)
        dropped = self._total_bytes - self._kept_bytes
        if dropped > 0:
            kept += f"\n[output truncated: {dropped} of {self._total_bytes} bytes dropped]"
        return kept


def drain_stream(stream, capture, chunk_size=65536):
    """Read `stream` to EOF into `capture`. Always keeps reading (discarding what
    does not fit), so the job never blocks on a full pipe because of the cap."""
    try:
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            capture.feed(chunk)
    except (ValueError, OSError):
        # The pipe was closed under us (the process was killed): nothing more to read.
        pass
