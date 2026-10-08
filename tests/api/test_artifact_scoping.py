"""
Artifacts belong to an organization.

  * A customer can't make the coordinator register (read, hash, list) a path on
    its own disk: `artifacts` is refused through the API.
  * GET /artifacts shows only the caller's organization's artifacts.
  * dataset_artifacts can't point at another organization's artifact, and the
    error is the same as for an id that doesn't exist.
  * Two different files with the same name are two artifacts, not one.
"""
import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.execution.artifact_registry import ArtifactRegistry
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def env(tmp_path):
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    signups = {
        "acme": management.signup_customer("Acme", "Ann", "ann@acme.example", "correct-horse-1"),
        "globex": management.signup_customer("Globex", "Gil", "gil@globex.example", "correct-horse-1"),
    }
    keys = {name: s["api_key"]["secret"] for name, s in signups.items()}
    keys["acme_org"] = signups["acme"]["organization"]["org_id"]
    yield client, coordinator, keys, tmp_path
    coordinator.shutdown()


def _h(secret):
    return {"X-API-Key": secret}


def test_a_host_path_cannot_be_registered_through_the_api(env):
    client, coordinator, keys, _ = env
    r = client.post("/jobs", json={"command": "echo hi", "artifacts": ["/etc/passwd"]}, headers=_h(keys["acme"]))
    assert r.status_code == 400
    assert coordinator.artifact_registry.list_artifacts() == []
    assert len(coordinator.jobs) == 0


def test_listing_shows_only_the_callers_own_artifacts(env):
    client, coordinator, keys, tmp_path = env
    f1, f2 = tmp_path / "a.bin", tmp_path / "b.bin"
    f1.write_bytes(b"acme data")
    f2.write_bytes(b"globex data")
    a_id = coordinator.artifact_registry.register_artifact(str(f1), org_id="org-acme")
    coordinator.artifact_registry.register_artifact(str(f2), org_id="org-globex")
    # The listing is filtered by the caller's organization id.
    mine = coordinator.get_artifacts(org_id="org-acme", scoped=True)
    assert [a["artifact_id"] for a in mine] == [a_id]
    assert coordinator.get_artifacts(org_id="org-nobody", scoped=True) == []
    assert len(coordinator.get_artifacts()) == 2          # the staff view is unchanged


def test_the_api_listing_never_includes_other_organizations(env):
    client, coordinator, keys, tmp_path = env
    f = tmp_path / "x.bin"
    f.write_bytes(b"x")
    coordinator.artifact_registry.register_artifact(str(f), org_id="some-other-org")
    assert client.get("/artifacts", headers=_h(keys["acme"])).json() == []


def test_dataset_artifacts_cannot_reference_another_orgs_artifact(env):
    client, coordinator, keys, tmp_path = env
    f = tmp_path / "secret.csv"
    f.write_text("secret")
    foreign = coordinator.artifact_registry.register_artifact(str(f), org_id="another-org")
    r = client.post("/jobs", json={"command": "echo hi", "dataset_artifacts": [foreign]}, headers=_h(keys["acme"]))
    unknown = client.post("/jobs", json={"command": "echo hi", "dataset_artifacts": ["ART-999"]}, headers=_h(keys["acme"]))
    assert r.status_code == unknown.status_code == 400
    assert foreign in r.json()["detail"] and "unknown artifact" in r.json()["detail"]   # same wording as a missing id
    assert len(coordinator.jobs) == 0


def test_an_orgs_own_artifact_can_be_referenced(env):
    client, coordinator, keys, tmp_path = env
    org_id = keys["acme_org"]
    f = tmp_path / "mine.csv"
    f.write_text("mine")
    mine = coordinator.artifact_registry.register_artifact(str(f), org_id=org_id)
    r = client.post("/jobs", json={"command": "sleep 5", "dataset_artifacts": [mine]}, headers=_h(keys["acme"]))
    assert r.status_code == 200, r.text


class TestRegistryKeys:
    def test_two_different_files_with_one_name_are_two_artifacts(self, tmp_path):
        (tmp_path / "x").mkdir()
        (tmp_path / "y").mkdir()
        (tmp_path / "x" / "model.bin").write_bytes(b"one")
        (tmp_path / "y" / "model.bin").write_bytes(b"two")
        reg = ArtifactRegistry()
        a = reg.register_artifact(str(tmp_path / "x" / "model.bin"), org_id="o")
        b = reg.register_artifact(str(tmp_path / "y" / "model.bin"), org_id="o")
        assert a != b and reg.get_artifact(a).sha256 != reg.get_artifact(b).sha256

    def test_the_same_file_registers_once_per_org(self, tmp_path):
        f = tmp_path / "d.bin"
        f.write_bytes(b"d")
        reg = ArtifactRegistry()
        assert reg.register_artifact(str(f), org_id="o") == reg.register_artifact(str(f), org_id="o")
        assert reg.register_artifact(str(f), org_id="o") != reg.register_artifact(str(f), org_id="p")

    def test_removing_an_artifact_frees_its_slot(self, tmp_path):
        f = tmp_path / "d.bin"
        f.write_bytes(b"d")
        reg = ArtifactRegistry()
        first = reg.register_artifact(str(f), org_id="o")
        assert reg.remove_artifact(first)
        assert reg.register_artifact(str(f), org_id="o") != first
