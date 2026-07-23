"""
Configuration precedence for the transport layer: environment
variables override database settings, which override hardcoded
defaults. No operational value (ports, intervals, timeouts, cert
paths) is hardcoded anywhere else in `gcon.transport` -- every one of
them is read through `TransportConfig`.

Precedence, highest to lowest:
  1. Environment variable (e.g. `GCON_GRPC_PORT`)
  2. `settings` table in the control-plane database (e.g. key
     `grpc_port`, set via `ControlPlane.settings.set(...)`)
  3. Hardcoded default (`_DEFAULTS` below)

This lets an operator override a value cluster-wide by writing to the
database (no redeploy needed) while still allowing a per-process
environment variable to win when that's what's actually set (e.g. in
a container, or for a one-off debugging run).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from gcon.persistence.control_plane import ControlPlane

# Hardcoded defaults -- the last-resort fallback only. Every one of
# these is overridable via environment variable or database setting;
# nothing downstream should ever hardcode one of these values itself.
_DEFAULTS = {
    "grpc_host": "0.0.0.0",
    "grpc_port": "50051",
    "tls_cert_dir": "keys/grpc",
    "heartbeat_interval_seconds": "5",
    "heartbeat_miss_threshold": "3",
    "job_dispatch_timeout_seconds": "3600",
    "reconnect_initial_backoff_seconds": "1",
    "reconnect_max_backoff_seconds": "30",
    "reconnect_backoff_multiplier": "2",
    "grpc_max_message_bytes": str(64 * 1024 * 1024),
    "graceful_shutdown_grace_seconds": "30",
}

# Environment variable name for each setting key.
_ENV_NAMES = {
    "grpc_host": "GCON_GRPC_HOST",
    "grpc_port": "GCON_GRPC_PORT",
    "tls_cert_dir": "GCON_TLS_CERT_DIR",
    "heartbeat_interval_seconds": "GCON_HEARTBEAT_INTERVAL_SECONDS",
    "heartbeat_miss_threshold": "GCON_HEARTBEAT_MISS_THRESHOLD",
    "job_dispatch_timeout_seconds": "GCON_JOB_DISPATCH_TIMEOUT_SECONDS",
    "reconnect_initial_backoff_seconds": "GCON_RECONNECT_INITIAL_BACKOFF_SECONDS",
    "reconnect_max_backoff_seconds": "GCON_RECONNECT_MAX_BACKOFF_SECONDS",
    "reconnect_backoff_multiplier": "GCON_RECONNECT_BACKOFF_MULTIPLIER",
    "grpc_max_message_bytes": "GCON_GRPC_MAX_MESSAGE_BYTES",
    "graceful_shutdown_grace_seconds": "GCON_GRACEFUL_SHUTDOWN_GRACE_SECONDS",
}


class ConfigResolver:
    """
    Resolves a single setting key using env -> db -> default
    precedence. `control_plane` is optional: with none supplied,
    resolution is just env -> default (useful for the agent daemon,
    which has no local control-plane database of its own).
    """

    def __init__(self, control_plane: Optional[ControlPlane] = None):
        self.control_plane = control_plane

    def get(self, key: str) -> str:
        if key not in _DEFAULTS:
            raise KeyError(f"Unknown config key: {key!r}")

        env_name = _ENV_NAMES[key]
        env_value = os.environ.get(env_name)
        if env_value is not None and env_value != "":
            return env_value

        if self.control_plane is not None:
            db_value = self.control_plane.settings.get(key)
            if db_value is not None and db_value != "":
                return db_value

        return _DEFAULTS[key]

    def get_int(self, key: str) -> int:
        return int(self.get(key))

    def get_float(self, key: str) -> float:
        return float(self.get(key))


@dataclass
class TransportConfig:
    """Fully-resolved snapshot of transport configuration, computed
    once via `TransportConfig.load()` rather than re-reading env/DB
    on every access."""

    grpc_host: str
    grpc_port: int
    tls_cert_dir: str
    heartbeat_interval_seconds: float
    heartbeat_miss_threshold: int
    job_dispatch_timeout_seconds: float
    reconnect_initial_backoff_seconds: float
    reconnect_max_backoff_seconds: float
    reconnect_backoff_multiplier: float
    grpc_max_message_bytes: int
    graceful_shutdown_grace_seconds: float

    @classmethod
    def load(cls, control_plane: Optional[ControlPlane] = None) -> "TransportConfig":
        r = ConfigResolver(control_plane)
        return cls(
            grpc_host=r.get("grpc_host"),
            grpc_port=r.get_int("grpc_port"),
            tls_cert_dir=r.get("tls_cert_dir"),
            heartbeat_interval_seconds=r.get_float("heartbeat_interval_seconds"),
            heartbeat_miss_threshold=r.get_int("heartbeat_miss_threshold"),
            job_dispatch_timeout_seconds=r.get_float("job_dispatch_timeout_seconds"),
            reconnect_initial_backoff_seconds=r.get_float("reconnect_initial_backoff_seconds"),
            reconnect_max_backoff_seconds=r.get_float("reconnect_max_backoff_seconds"),
            reconnect_backoff_multiplier=r.get_float("reconnect_backoff_multiplier"),
            grpc_max_message_bytes=r.get_int("grpc_max_message_bytes"),
            graceful_shutdown_grace_seconds=r.get_float("graceful_shutdown_grace_seconds"),
        )
