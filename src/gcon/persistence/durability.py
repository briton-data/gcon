"""
Startup check: is the state that must survive a redeploy on a disk that does?

A container's own filesystem is thrown away on every redeploy. Only a mounted
volume survives. GCON keeps several things on local disk, so without a volume
a redeploy silently resets all of them:

  * identity database  (organizations, users, API keys, sessions) -- always
    SQLite under GCON_DATA_DIR, even when the control plane uses Postgres
  * control-plane database (workers, jobs, receipts) -- SQLite under
    GCON_DATA_DIR unless GCON_CONTROL_PLANE_DATABASE_URL points at Postgres
  * receipt-signing key (GCON_HMAC_KEY_PATH, default ./keys/hmac_secret.key)
  * webhook-secret encryption key (GCON_SECRETS_KEY_PATH, default
    ./keys/secrets.key), unless GCON_SECRETS_KEY is set
  * coordinator CA / certificates (GCON_TLS_CERT_DIR)

This module only REPORTS. It never moves, creates or refuses anything, so a
deployment that works today keeps working exactly as before.

Detection is a heuristic: inside a container, a path with no mount point
between it and "/" lives on the container's own writable layer. It cannot tell
a durable volume from a mounted-but-throwaway one (e.g. a Kubernetes emptyDir),
so a quiet startup is not proof of durability.
"""
from __future__ import annotations

import os
from typing import Callable, Iterable, List, Optional, Tuple

from gcon.config import (
    resolve_control_plane_db_path,
    resolve_control_plane_postgres_dsn,
    resolve_legacy_db_path,
)

# (label, absolute path, what is lost if that path is discarded)
Location = Tuple[str, str, str]

_CONTAINER_ENV_MARKERS = ("RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID", "KUBERNETES_SERVICE_HOST")


def running_in_container() -> bool:
    if os.path.exists("/.dockerenv"):
        return True
    return any(os.environ.get(name) for name in _CONTAINER_ENV_MARKERS)


def on_mounted_volume(path: str, is_mount: Callable[[str], bool] = os.path.ismount) -> bool:
    """True if some ancestor of `path` (below "/") is a mount point."""
    current = os.path.abspath(path)
    while True:
        parent = os.path.dirname(current)
        if parent == current:  # reached the root; "/" itself is the container layer
            return False
        if is_mount(current):
            return True
        current = parent


def state_locations(control_plane_db: Optional[str], tls_cert_dir: Optional[str]) -> List[Location]:
    """Every local path whose loss a redeploy would turn into data loss."""
    locations: List[Location] = [
        (
            "identity database",
            os.path.abspath(resolve_legacy_db_path()),
            "organizations, users, API keys and sessions",
        ),
    ]
    if not resolve_control_plane_postgres_dsn():
        locations.append((
            "control-plane database",
            os.path.abspath(resolve_control_plane_db_path(control_plane_db)),
            "workers, jobs and receipts",
        ))
    locations.append((
        "receipt-signing key",
        os.path.abspath(os.environ.get("GCON_HMAC_KEY_PATH", "./keys/hmac_secret.key")),
        "the ability to verify receipts already issued",
    ))
    if not os.environ.get("GCON_SECRETS_KEY", "").strip():
        locations.append((
            "webhook-secret key",
            os.path.abspath(os.environ.get("GCON_SECRETS_KEY_PATH", "./keys/secrets.key")),
            "the stored webhook secrets (they become unreadable)",
        ))
    if tls_cert_dir:
        locations.append((
            "coordinator CA and certificates",
            os.path.abspath(tls_cert_dir),
            "worker enrollment (every worker must enroll again)",
        ))
    return locations


def durability_warnings(
    locations: Iterable[Location],
    in_container: Optional[bool] = None,
    is_mount: Callable[[str], bool] = os.path.ismount,
) -> List[str]:
    """One warning per path that would be discarded on redeploy. Empty outside a container."""
    if in_container is None:
        in_container = running_in_container()
    if not in_container:
        return []
    return [
        f"{label} is at {path}, which is not on a mounted volume. "
        f"A redeploy discards it and with it {lost}."
        for label, path, lost in locations
        if not on_mounted_volume(path, is_mount)
    ]


def log_durable_storage_report(logger, control_plane_db: Optional[str], tls_cert_dir: Optional[str]) -> None:
    """Log where state lives; warn loudly for anything a redeploy would wipe."""
    locations = state_locations(control_plane_db, tls_cert_dir)
    for label, path, _ in locations:
        logger.info("state: %s -> %s", label, path)
    if resolve_control_plane_postgres_dsn():
        logger.info(
            "state: control plane is on Postgres; the identity database above is still "
            "a local SQLite file and does NOT move with it"
        )
    for message in durability_warnings(locations):
        logger.warning("DATA WILL NOT SURVIVE A REDEPLOY: %s", message)
