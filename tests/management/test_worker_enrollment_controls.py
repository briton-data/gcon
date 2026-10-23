"""Staff controls over workers and how they join: enrollment tokens (a lifetime and a use count, per
organization), archiving workers that are gone, and the per-worker reasons an 'online' worker is not taking
customer jobs."""
from types import SimpleNamespace

import pytest

from gcon.cluster.registry import NodeRegistry, dispatchable, version_below
from tests.management.test_shared_pool_admin import env  # noqa: F401  (fixture)


def _node(node_id, org_id=None, sandboxed=True, connected=True, version="1.0.0"):
    return SimpleNamespace(node_id=node_id, org_id=org_id, sandboxed=sandboxed, agent_version=version,
                           status="idle", is_connected=lambda: connected)


class TestWhoCanBeSentAJob:
    def test_registered_but_not_connected_is_not_idle(self):
        registry = NodeRegistry()
        registry.register(_node("up")); registry.register(_node("no-stream", connected=False))
        assert [n.node_id for n in registry.available_nodes()] == ["up"]
        claimed = registry.claim_best_idle_node(lambda info: 0)
        assert claimed.node_id == "up"
        assert registry.claim_best_idle_node(lambda info: 0) is None     # the other one is never handed out

    def test_a_worker_below_the_minimum_version_is_registered_but_never_dispatched(self):
        registry = NodeRegistry()
        registry.min_worker_version = "1.2.0"
        registry.register(_node("old", version="1.1.9")); registry.register(_node("new", version="1.2.0"))
        registry.register(_node("unknown", version=None))
        assert registry.snapshot()["old"]["outdated"] is True
        assert [n.node_id for n in registry.available_nodes()] == ["new"]

    @pytest.mark.parametrize("have, minimum, below", [
        ("1.0.0", None, False), ("1.0.0", "", False), ("1.2.0", "1.2.0", False), ("1.10.0", "1.9.0", False),
        ("1.9.0", "1.10.0", True), (None, "1.0.0", True), ("dev", "1.0.0", True),
    ])
    def test_version_comparison(self, have, minimum, below):
        assert version_below(have, minimum) is below


class TestPerWorkerReasons:
    def _coordinator_nodes(self, coordinator):
        return {n["node_id"]: n for n in coordinator.get_nodes()}

    def test_online_but_unusable_workers_say_why(self, env):  # noqa: F811
        _clients, _server, coordinator, org_id = env
        coordinator._sandbox_policy = "required"        # the production default (tests run with "trusted")
        reg = coordinator.registry
        reg.register(_node("good", org_id=org_id))
        reg.register(_node("no-sandbox", org_id=org_id, sandboxed=False))
        reg.register(_node("legacy-token"))                                  # no organization, not shared
        reg.register(_node("no-stream", org_id=org_id, connected=False))
        nodes = self._coordinator_nodes(coordinator)
        assert nodes["good"]["can_run_customer_jobs"] and nodes["good"]["blockers"] == []
        assert nodes["good"]["serves"] == f"organization {org_id}"
        assert any("sandbox" in b for b in nodes["no-sandbox"]["blockers"])
        assert any("shared token" in b for b in nodes["legacy-token"]["blockers"])
        assert nodes["legacy-token"]["serves"] == "jobs with no organization only"
        assert any("not connected" in b for b in nodes["no-stream"]["blockers"])
        assert coordinator.get_node_summary()["customer_ready"] == 1
        assert [n.node_id for n in coordinator.get_idle_nodes()] == ["good", "no-sandbox", "legacy-token"]


class TestArchive:
    def test_only_an_offline_worker_can_be_archived(self, env):  # noqa: F811
        clients, _server, coordinator, org_id = env
        coordinator.registry.register(_node("online", org_id=org_id))
        coordinator.registry.register(_node("gone", org_id=org_id))
        coordinator.registry.mark_offline("gone")
        coordinator.control_plane.nodes.upsert(node_id="gone", hostname="gone", status="offline", org_id=org_id)

        refused = clients["Owner"].post("/management/nodes/online/archive")
        assert refused.status_code == 400 and "offline" in refused.json()["detail"]
        assert clients["Owner"].post("/management/nodes/gone/archive").status_code == 200
        assert "gone" not in coordinator.registry.snapshot() and "online" in coordinator.registry.snapshot()
        assert coordinator.control_plane.nodes.get("gone")["archived_at"] is not None
        assert coordinator.control_plane.nodes.count_active_for_org(org_id) == 0
        assert clients["Owner"].post("/management/nodes/never-heard-of-it/archive").status_code == 400

    def test_archive_all_offline_leaves_the_live_ones(self, env):  # noqa: F811
        clients, _server, coordinator, _org = env
        for name in ("a", "b", "live"):
            coordinator.registry.register(_node(name))
        coordinator.registry.mark_offline("a"); coordinator.registry.mark_offline("b")
        r = clients["Owner"].post("/management/nodes/archive-offline")
        assert r.status_code == 200 and sorted(r.json()["archived"]) == ["a", "b"]
        assert list(coordinator.registry.snapshot()) == ["live"]

    def test_an_operator_cannot_archive(self, env):  # noqa: F811
        clients, _server, coordinator, _org = env
        coordinator.registry.register(_node("a")); coordinator.registry.mark_offline("a")
        assert clients["Operator"].post("/management/nodes/a/archive").status_code in (401, 403)
        assert "a" in coordinator.registry.snapshot()


class TestEnrollTokens:
    def test_a_minted_token_always_has_a_lifetime_and_a_use_count(self, env):  # noqa: F811
        clients, _server, coordinator, org_id = env
        made = clients["Owner"].post("/management/enroll-tokens", json={"org_id": org_id, "label": "codespaces"})
        assert made.status_code == 200, made.text
        body = made.json()
        assert body["token"].startswith("gcon_enroll_") and body["max_uses"] == 5 and body["expires_at"]
        listed = clients["Owner"].get("/management/enroll-tokens").json()
        assert len(listed) == 1 and "token" not in listed[0] and "token_hash" not in listed[0]
        assert listed[0]["status"] == "active" and listed[0]["org_name"] == "Acme" and listed[0]["uses"] == 0
        assert coordinator.control_plane.enroll_tokens.lookup_org_id(body["token"]) == org_id

    @pytest.mark.parametrize("bad", [{"expires_in_hours": 0}, {"expires_in_hours": 99999},
                                      {"max_uses": 0}, {"max_uses": "many"}])
    def test_silly_figures_are_refused(self, env, bad):  # noqa: F811
        clients, _server, _c, org_id = env
        assert clients["Owner"].post("/management/enroll-tokens", json={"org_id": org_id, **bad}).status_code == 400

    def test_revoking_stops_the_token(self, env):  # noqa: F811
        clients, _server, coordinator, org_id = env
        token = clients["Owner"].post("/management/enroll-tokens", json={"org_id": org_id}).json()["token"]
        token_id = clients["Owner"].get("/management/enroll-tokens").json()[0]["token_id"]
        assert clients["Owner"].post(f"/management/enroll-tokens/{token_id}/revoke").status_code == 200
        assert clients["Owner"].get("/management/enroll-tokens").json()[0]["status"] == "revoked"
        assert coordinator.control_plane.enroll_tokens.lookup_org_id(token) is None

    def test_only_owner_or_administrator(self, env):  # noqa: F811
        clients, _server, _c, org_id = env
        assert clients["Administrator"].post("/management/enroll-tokens", json={"org_id": org_id}).status_code == 200
        assert clients["Operator"].post("/management/enroll-tokens", json={"org_id": org_id}).status_code in (401, 403)
        assert clients["Operator"].get("/management/enroll-tokens").status_code in (401, 403)

    def test_it_is_audit_logged_without_the_token(self, env):  # noqa: F811
        clients, server, _c, org_id = env
        token = clients["Owner"].post("/management/enroll-tokens", json={"org_id": org_id}).json()["token"]
        entries = str(server.management.audit_logger.get_logs() if hasattr(server.management.audit_logger, "get_logs")
                      else server.management.audit_logger.entries)
        assert "enrollment token" in entries and token not in entries
