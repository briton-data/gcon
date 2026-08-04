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


def resolve_data_dir() -> str:
    return os.environ.get("GCON_DATA_DIR", "data")


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