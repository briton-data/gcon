import pytest

from gcon.cluster.communication import CommunicationManager
from gcon.transport.errors import NodeUnavailableError
from gcon.transport.interfaces import Transport
from gcon.transport.local_transport import LocalTransport


class _FakeNode:
    def __init__(self, node_id, result=None, cancel_result=True):
        self.node_id = node_id
        self._result = result or {"status": "success", "output": "ok"}
        self._cancel_result = cancel_result
        self.executed_with = None

    def execute_job(self, job_id, command, timeout=None, usage_report_path=None):
        self.executed_with = (job_id, command, timeout, usage_report_path)
        return self._result

    def cancel(self):
        return self._cancel_result


def test_local_transport_is_a_transport():
    assert isinstance(LocalTransport(), Transport)


def test_communication_manager_defaults_to_local_transport():
    manager = CommunicationManager()
    assert isinstance(manager.transport, LocalTransport)


def test_register_get_list_nodes():
    manager = CommunicationManager()
    node = _FakeNode("node-1")
    manager.register_node(node)
    assert manager.get_node("node-1") is node
    assert manager.list_nodes() == ["node-1"]


def test_unregister_node():
    manager = CommunicationManager()
    manager.register_node(_FakeNode("node-1"))
    manager.unregister_node("node-1")
    with pytest.raises(NodeUnavailableError):
        manager.get_node("node-1")


def test_get_unknown_node_raises():
    manager = CommunicationManager()
    with pytest.raises(NodeUnavailableError):
        manager.get_node("does-not-exist")


def test_send_job_delegates_and_wraps_result():
    manager = CommunicationManager()
    node = _FakeNode("node-1", result={"status": "success", "stdout": "hi"})
    manager.register_node(node)

    response = manager.send_job("node-1", "job-1", "echo hi", timeout=30)

    assert response == {"status": "success", "result": {"status": "success", "stdout": "hi"}}
    job_id, command, timeout, usage_report_path = node.executed_with
    assert (job_id, command, timeout) == ("job-1", "echo hi", 30)
    # Previously LocalTransport never generated a usage_report_path at
    # all, so GCONAgent.execute_job's usage_report_path parameter was
    # unreachable for every job run through LocalTransport (confirmed
    # empirically pre-fix: a real job's receipt always had usage: null).
    # This asserts the real fix: a real, job-specific path is now always
    # passed, not just tolerated by a wider function signature.
    assert usage_report_path is not None
    assert "job-1" in usage_report_path


def test_send_job_to_unknown_node_raises():
    manager = CommunicationManager()
    with pytest.raises(NodeUnavailableError):
        manager.send_job("ghost", "job-1", "echo hi")


def test_cancel_job_delegates_to_node():
    manager = CommunicationManager()
    node = _FakeNode("node-1", cancel_result=True)
    manager.register_node(node)
    assert manager.cancel_job("node-1", "job-1") is True


def test_cancel_job_false_when_node_has_no_cancel_support():
    class _NoCancel:
        node_id = "node-2"

        def execute_job(self, *a, **k):
            return {}

    manager = CommunicationManager()
    manager.register_node(_NoCancel())
    assert manager.cancel_job("node-2", "job-1") is False


def test_shutdown_clears_nodes():
    manager = CommunicationManager()
    manager.register_node(_FakeNode("node-1"))
    manager.shutdown()
    assert manager.list_nodes() == []


def test_local_transport_thread_safety():
    import threading

    transport = LocalTransport()
    errors = []

    def register(i):
        try:
            transport.register_node(_FakeNode(f"node-{i}"))
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=register, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(transport.list_node_ids()) == 50
