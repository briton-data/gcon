import grpc
import pytest

from gcon.transport import tls


def test_ensure_ca_is_idempotent(tmp_path):
    d = str(tmp_path)
    ca1 = tls.ensure_ca(d)
    with open(ca1.cert_path, "rb") as f:
        first_bytes = f.read()
    ca2 = tls.ensure_ca(d)
    with open(ca2.cert_path, "rb") as f:
        second_bytes = f.read()
    assert first_bytes == second_bytes


def test_agent_and_coordinator_certs_share_ca(tmp_path):
    d = str(tmp_path)
    coord = tls.issue_coordinator_cert(d, hostname="localhost")
    agent = tls.issue_agent_cert(d, "node-1")
    assert coord.ca_cert_path == agent.ca_cert_path


def test_agent_cert_common_name_is_node_id(tmp_path):
    d = str(tmp_path)
    agent = tls.issue_agent_cert(d, "worker-42")
    assert tls.cert_common_name(agent.cert_path) == "worker-42"


def test_different_agents_get_different_fingerprints(tmp_path):
    d = str(tmp_path)
    a = tls.issue_agent_cert(d, "node-a")
    b = tls.issue_agent_cert(d, "node-b")
    assert tls.cert_fingerprint(a.cert_path) != tls.cert_fingerprint(b.cert_path)


def test_credentials_are_real_grpc_credential_objects(tmp_path):
    d = str(tmp_path)
    server_creds = tls.load_server_credentials(d, hostname="localhost")
    client_creds = tls.load_agent_channel_credentials(d, "node-1")
    assert isinstance(server_creds, grpc.ServerCredentials)
    assert isinstance(client_creds, grpc.ChannelCredentials)
