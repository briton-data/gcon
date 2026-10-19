"""
Shared filesystem configuration for GCON's two SQLite stores:

  * gcon.persistence.db.ControlPlaneDatabase  (cluster state: nodes,
    jobs, receipts, heartbeats, ...) -- historically configured via
    the `--db` CLI flag / GCON_CONTROL_PLANE_DB_PATH, default
    "data/gcon_control_plane.db".
  * gcon.storage.database.Database  (identity/platform state: users,
    API keys, orgs, sessions, ...) -- historically configured via
    GCON_DB_PATH, default "data/gcon.db".

Both stores used to pick their own default path independently, which
meant a deployment could end up with `data/` on one disk/volume and
some other directory on the other without anyone intending that. This
module gives them one shared knob -- GCON_DATA_DIR / --data-dir --
while preserving every existing override and default exactly:

  * An explicit path (CLI --db, or the `path=` constructor arg) always
    wins, same as before.
  * The store-specific env var (GCON_CONTROL_PLANE_DB_PATH /
    GCON_DB_PATH) always wins over the shared data dir, same as
    before -- nothing that already worked stops working.
  * Only once neither of those is set does the shared GCON_DATA_DIR
    (default "data", identical to the old hardcoded default) decide
    where the two well-known filenames live.

Resolution happens lazily (at DB-construction time), not at import
time, so tests that set these env vars via monkeypatch/os.environ
after the module is first imported still take effect -- the previous
per-module `DEFAULT_..._PATH = os.environ.get(...)` constants were
frozen at import time, which is itself a latent config bug this
fixes as a side effect.
"""

from __future__ import annotations

import os

CONTROL_PLANE_DB_FILENAME = "gcon_control_plane.db"
LEGACY_DB_FILENAME = "gcon.db"


def persistent_volume_dir() -> "str | None":
    """The directory of a persistent volume attached to this service, if the platform says there is one.

    Railway sets RAILWAY_VOLUME_MOUNT_PATH when a volume is attached. Everything that must survive a redeploy
    (the databases, the worker certificate authority, the receipt signing key) follows it by default, so
    attaching a volume is all the setup needed. Not on Railway, this is None and nothing changes."""
    path = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
    return path or None


def resolve_data_dir() -> str:
    return os.environ.get("GCON_DATA_DIR") or persistent_volume_dir() or "data"


def resolve_tls_cert_dir() -> str:
    """The worker certificate authority and issued certificates. GCON_TLS_CERT_DIR wins; else the volume; else keys/grpc."""
    explicit = os.environ.get("GCON_TLS_CERT_DIR")
    if explicit:
        return explicit
    volume = persistent_volume_dir()
    return os.path.join(volume, "keys", "grpc") if volume else "keys/grpc"


def resolve_hmac_key_path() -> str:
    """The receipt signing key (also the root of the key that encrypts stored webhook secrets)."""
    explicit = os.environ.get("GCON_HMAC_KEY_PATH")
    if explicit:
        return explicit
    volume = persistent_volume_dir()
    return os.path.join(volume, "keys", "hmac_secret.key") if volume else "./keys/hmac_secret.key"


def describe_persistence() -> dict:
    """Where state lives and whether that place survives a redeploy, for the startup log and the dashboard."""
    on_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))
    volume = persistent_volume_dir()
    paths = {
        "data_dir": resolve_data_dir(),
        # The database files themselves: GCON_CONTROL_PLANE_DB_PATH / GCON_DB_PATH override the data directory, so
        # check where they actually point. A control plane on PostgreSQL has no local file to lose.
        "identity_db": resolve_legacy_db_path(),
        "tls_cert_dir": resolve_tls_cert_dir(),
        "hmac_key_path": resolve_hmac_key_path(),
    }
    if not resolve_control_plane_postgres_dsn():
        paths["control_plane_db"] = resolve_control_plane_db_path()
    durable = True
    reason = "not on Railway; state is on this machine's disk"
    if on_railway:
        def under(p):
            if volume is None:
                return False
            path, root = os.path.abspath(p), os.path.abspath(volume)
            return path == root or path.startswith(root + os.sep)
        missing = [name for name, p in paths.items() if not under(p)]
        durable = not missing
        reason = ("all state is on the attached volume" if durable else
                  "NOT durable: " + ", ".join(missing) + (" is not on a persistent volume" if volume else
                  ": no volume is attached to this service") + ". Organizations, API keys, invoices and the worker "
                  "certificate authority are lost on every redeploy.")
    return {"on_railway": on_railway, "volume": volume, "durable": durable, "reason": reason, **paths}


def resolve_control_plane_db_path(explicit: "str | None" = None) -> str:
    if explicit:
        return explicit
    env_override = os.environ.get("GCON_CONTROL_PLANE_DB_PATH")
    if env_override:
        return env_override
    return os.path.join(resolve_data_dir(), CONTROL_PLANE_DB_FILENAME)


def resolve_legacy_db_path(explicit: "str | None" = None) -> str:
    if explicit:
        return explicit
    env_override = os.environ.get("GCON_DB_PATH")
    if env_override:
        return env_override
    return os.path.join(resolve_data_dir(), LEGACY_DB_FILENAME)


def resolve_control_plane_postgres_dsn() -> "str | None":
    """
    GCON_CONTROL_PLANE_DATABASE_URL, if set, is a libpq connection
    string (e.g. "postgresql://user:pass@host:5432/dbname") for the
    control plane's real, network-shared backend -- the one multiple
    coordinator processes on *separate hosts* can actually share, for
    genuine cross-host leader election/job-state HA. A local SQLite
    file, no matter how it's configured via the functions above, can
    never be that: it's how every coordinator still defaults to
    running today, single-host, when this isn't set. Kept separate
    from resolve_control_plane_db_path (SQLite's own resolution,
    unchanged) rather than folded into it, since this isn't a "where
    does the file live" question -- it's "which engine, and which
    server" -- and ControlPlane.__init__ is what actually decides,
    from this return value, whether to build a PostgresDialect or the
    existing SQLiteDialect default.
    """
    return os.environ.get("GCON_CONTROL_PLANE_DATABASE_URL") or None