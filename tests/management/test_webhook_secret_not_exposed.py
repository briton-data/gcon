"""Webhook delivery history used to return each delivery's plaintext HMAC secret,
which lets anyone who can read history forge signed callbacks."""
from gcon.cluster.coordinator import GCONCoordinator
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane


def test_delivery_history_masks_the_signing_secret(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_WEBHOOK_ALLOW_PRIVATE_TARGETS", "1")
    plane = ControlPlane(path=str(tmp_path / "cp.db"))
    coord = GCONCoordinator(control_plane=plane)
    try:
        layer = ManagementLayer(coordinator=coord, db_path=str(tmp_path / "m.db"))
        plane.jobs.ensure_exists("j1", "echo hi", org_id="org-a")
        real_secret = "ab" * 32
        plane.webhooks.enqueue_delivery(
            "job.completed", {"x": 1}, "http://127.0.0.1:1/h", real_secret,
            org_id="org-a", job_id="j1",
        )
        history = layer.get_webhook_deliveries()
        by_job = layer.get_webhook_deliveries(job_id="j1")
        for row in history + by_job:
            assert row["secret"] != real_secret and "*" in row["secret"]
        assert history or by_job
    finally:
        coord.shutdown()
        plane.close()
