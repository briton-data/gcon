"""PUT /management/users/{id} used to splat the raw body into update_user, so a
request could set password_hash, user_id, stats... and any role string."""
import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.management.management_layer import ManagementLayer


@pytest.fixture
def mgmt(tmp_path):
    coordinator = GCONCoordinator()
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "m.db"))
    user = management.create_user("Vic", "vic@gcon.example", role="Viewer", organization_id=None)
    yield management, user["user_id"]
    coordinator.shutdown()


def test_allowed_fields_still_update(mgmt):
    management, uid = mgmt
    out = management.update_user(uid, name="Victor", role="Operator", status="Active")
    assert out["name"] == "Victor" and out["role"] == "Operator"


@pytest.mark.parametrize("field,value", [
    ("password_hash", "x"), ("user_id", "other"), ("stats", {}), ("created_at", "2020-01-01"),
])
def test_internal_fields_are_refused(mgmt, field, value):
    management, uid = mgmt
    before = management.user_registry.get_user(uid).password_hash
    with pytest.raises(ValueError, match="cannot be updated"):
        management.update_user(uid, **{field: value})
    assert management.user_registry.get_user(uid).password_hash == before


def test_an_unknown_role_is_refused(mgmt):
    management, uid = mgmt
    with pytest.raises(ValueError, match="Invalid role"):
        management.update_user(uid, role="Superuser")
