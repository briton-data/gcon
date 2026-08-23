#!/usr/bin/env python3
"""
DEPRECATED -- kept only so existing commands/docs referencing this
filename keep working. The maintained agent entry point is
scripts/run_worker.py (env var support, --hostname, --capability,
graceful SIGTERM/SIGINT handling -- all present here too now, via
delegation, so nothing is lost by using either name).

Use `python scripts/run_worker.py --help` for current usage; this
file just forwards straight to it.
"""

import sys

sys.path.insert(0, "src")

from run_worker import main  # noqa: E402  (after sys.path setup, by design)

if __name__ == "__main__":
    print(
        "[deprecated] gcon_agent_daemon.py is now a thin wrapper around "
        "run_worker.py -- use scripts/run_worker.py directly.",
        file=sys.stderr,
    )
    main()
