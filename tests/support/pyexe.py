"""
Portable Python-interpreter resolution for test job commands.

Tests submit shell commands like "python3 -c '...'" to be run by
GCONAgent via subprocess(shell=True). Hardcoding "python3" breaks on
machines that only ship "python.exe" (e.g. Windows without a python3
shim). PY resolves to the actual interpreter running the test suite,
quoted so paths containing spaces (very common on Windows, e.g.
"Program Files") don't break the shell command.
"""
import shlex
import sys

PY = shlex.quote(sys.executable)
