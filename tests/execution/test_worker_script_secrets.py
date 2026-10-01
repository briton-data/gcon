"""
The worker launch scripts write the node's private key to disk from a base64
environment variable. Two things used to leave it reachable by job code:

  * docker/entrypoint.sh wrote it with the default umask (typically 0644 --
    readable by every process in the container) and never chmod'd it.
  * both scripts left the base64 variables set, so the private key stayed
    readable by any same-user process via /proc/<pid>/environ -- which is where
    a subprocess-backend job runs.

These run the real secret-handling section of each script (up to and
including its `unset`) under the umask a container typically has.
"""
import os
import shutil
import stat
import subprocess
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX sh")


def _secret_section(script, start_marker):
    """Lines of `script` from start_marker through the `unset GCON_CA_CERT_B64...` line."""
    lines = (ROOT / script).read_text().splitlines()
    begin = next(i for i, line in enumerate(lines) if line.startswith(start_marker))
    end = next(i for i, line in enumerate(lines) if line.startswith("unset GCON_CA_CERT_B64"))
    return "\n".join(lines[begin:end + 1])


def _run(script, marker, tmp_path):
    cert_dir = tmp_path / "certs"
    harness = textwrap.dedent(f"""
        set -e
        export GCON_NODE_ID=n1 GCON_TLS_CERT_DIR="{cert_dir}"
        export GCON_CA_CERT_B64=$(printf CA | base64) GCON_AGENT_CERT_B64=$(printf CERT | base64)
        export GCON_AGENT_KEY_B64=$(printf SECRETKEY | base64)
        umask 022
        {_secret_section(script, marker)}
        echo "umask_after=$(umask)"
        echo "key_env=${{GCON_AGENT_KEY_B64:-unset}} cert_env=${{GCON_AGENT_CERT_B64:-unset}} ca_env=${{GCON_CA_CERT_B64:-unset}}"
    """)
    done = subprocess.run(["sh", "-c", harness], capture_output=True, text=True, check=True)
    return cert_dir, done.stdout


@pytest.mark.parametrize("script,marker", [
    ("docker/entrypoint.sh", "CERT_DIR="),
    ("scripts/worker_bootstrap.sh", "CERT_DIR="),
])
class TestPrivateKeyHandling:
    def test_key_file_is_private(self, script, marker, tmp_path):
        cert_dir, _ = _run(script, marker, tmp_path)
        key = cert_dir / "agent-n1.key.pem"
        assert key.read_text().strip() == "SECRETKEY"
        assert stat.S_IMODE(key.stat().st_mode) == 0o600

    def test_no_group_or_world_access_even_to_the_cert_files(self, script, marker, tmp_path):
        cert_dir, _ = _run(script, marker, tmp_path)
        for name in ("agent-n1.key.pem", "agent-n1.cert.pem", "ca.cert.pem"):
            assert stat.S_IMODE((cert_dir / name).stat().st_mode) & 0o077 == 0, name

    def test_the_base64_variables_are_gone_afterwards(self, script, marker, tmp_path):
        _, out = _run(script, marker, tmp_path)
        assert "key_env=unset cert_env=unset ca_env=unset" in out

    def test_the_restrictive_umask_does_not_leak_to_later_commands(self, script, marker, tmp_path):
        """Jobs the worker starts afterwards must not inherit a changed umask."""
        _, out = _run(script, marker, tmp_path)
        assert "umask_after=0022" in out
