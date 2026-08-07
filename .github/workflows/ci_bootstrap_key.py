"""
CI-only helper: bootstraps the owner account and mints an API key
against the management DB *before* the coordinator process starts.

Why before, not after: ManagementLayer loads users/keys from the DB
once, at construction. The coordinator's own ManagementLayer instance
(built inside run_coordinator.py / web_server.py) needs this key to
already be a row in the DB by the time it starts, so it gets loaded
into that process's in-memory cache. Running this after the
coordinator is already up would write the key to the DB but the
already-running process would never see it without a restart.

Prints the raw key secret to stdout -- this is the only time it's
ever shown, by design (see api_keys.py).
"""
import sys
sys.path.insert(0, "src")

from gcon.management.management_layer import ManagementLayer, BOOTSTRAP_OWNER_EMAIL

management = ManagementLayer()  # bootstraps the owner account as a side effect

owner = management.user_registry.get_user_by_email(BOOTSTRAP_OWNER_EMAIL)
if owner is None:
    raise SystemExit("Bootstrap owner account was not created -- check GCON_OWNER_PASSWORD / GCON_DB_PATH.")

key = management.api_key_manager.create_key(
    name="ci-e2e-test",
    owner_user_id=owner.user_id,
    scopes=["Submit workflows", "View monitoring"],
    expires_in_days=1,
)

print(key.secret)
