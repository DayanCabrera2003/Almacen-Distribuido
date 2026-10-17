"""GET /cluster/status: one node's view of the cluster, for debugging and demos."""
from collections.abc import Iterator
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from almacen.cluster.membership import NodeState, NodeStatus
from almacen.config import Peer, Settings
from almacen.main import create_app
from tests.helpers import free_port


def _members(body) -> dict[str, dict]:
    return {member["node_id"]: member for member in body["members"]}


def test_a_single_node_reports_itself_settings_and_counts(tmp_path: Path):
    settings = Settings(
        data_dir=tmp_path / "blobs",
        db_path=tmp_path / "metadata.db",
        gossip_interval_seconds=0,
    )
    client = TestClient(create_app(settings))
    kept = client.post("/files", files={"file": ("a.txt", b"one")}, data={"name": "a"})
    gone = client.post("/files", files={"file": ("b.txt", b"two")}, data={"name": "b"})
    client.delete(f"/files/{gone.json()['file_id']}")
    assert kept.status_code == 201

    response = client.get("/cluster/status")

    assert response.status_code == 200
    body = response.json()
    assert body["node_id"] == "node1"
    assert body["members"] == [
        {
            "node_id": "node1",
            "address": None,
            "state": "alive",
            "incarnation": 0,
            "since": None,
            "is_self": True,
        }
    ]
    assert body["replication"] == {
        "replication_factor": 3,
        "write_quorum": 2,
        "rpc_timeout_seconds": settings.rpc_timeout_seconds,
    }
    assert body["gossip"] == {
        "enabled": False,
        "interval_seconds": 0,
        "fanout": settings.gossip_fanout,
        "suspicion_timeout_seconds": settings.suspicion_timeout_seconds,
        "blob_catchup_budget": settings.blob_catchup_budget,
    }
    assert body["gc"] == {
        "grace_ttl_seconds": settings.grace_ttl_seconds,
        "budget": settings.gc_budget,
    }
    # The deleted file's blob stays until GC collects it.
    assert body["storage"] == {"live_records": 1, "tombstones": 1, "blobs": 2}


def test_peer_states_reflect_the_failure_detector(tmp_path: Path):
    peers = (
        Peer("node1", "127.0.0.1:59991"),
        Peer("node2", "127.0.0.1:59992"),
        Peer("node3", "127.0.0.1:59993"),
    )
    settings = Settings(
        data_dir=tmp_path / "blobs",
        db_path=tmp_path / "metadata.db",
        node_id="node1",
        peers=peers,
        gossip_interval_seconds=0,
        suspicion_timeout_seconds=60,
    )
    app = create_app(settings)
    membership = app.state.membership
    now = datetime.now(timezone.utc)
    # Long past the suspicion timeout: dead. Just now: still only suspect.
    membership.record_unreachable("node2", now - timedelta(hours=1))
    membership.record_unreachable("node3", now)
    # Someone claimed node1 was suspect; it refutes with a higher incarnation.
    membership.merge({"node1": NodeStatus(NodeState.SUSPECT, 0, now)}, now)

    body = TestClient(app).get("/cluster/status").json()

    members = _members(body)
    assert list(members) == ["node1", "node2", "node3"], "sorted by node id"
    assert members["node1"]["is_self"] is True
    assert members["node1"]["incarnation"] == 1
    assert members["node2"]["state"] == "dead"
    assert members["node2"]["address"] == "127.0.0.1:59992"
    assert members["node3"]["state"] == "suspect"
    assert members["node3"]["since"] is not None
    assert members["node3"]["is_self"] is False


@pytest.fixture
def three_nodes(tmp_path: Path) -> Iterator[dict[str, TestClient]]:
    ports = {f"node{i}": free_port() for i in range(1, 4)}
    peers = tuple(Peer(n, f"127.0.0.1:{p}") for n, p in ports.items())
    with ExitStack() as stack:
        clients = {}
        for node_id, port in ports.items():
            settings = Settings(
                data_dir=tmp_path / node_id / "blobs",
                db_path=tmp_path / node_id / "metadata.db",
                node_id=node_id,
                grpc_port=port,
                peers=peers,
                replication_factor=3,
                write_quorum=2,
                gossip_interval_seconds=0,
            )
            clients[node_id] = stack.enter_context(TestClient(create_app(settings)))
        yield clients


def test_every_node_reports_itself_and_the_replicated_state(three_nodes):
    upload = three_nodes["node1"].post(
        "/files", files={"file": ("a.txt", b"replicated")}, data={"name": "a.txt"}
    )
    assert upload.status_code == 201

    for node_id, client in three_nodes.items():
        body = client.get("/cluster/status").json()
        assert body["node_id"] == node_id
        members = _members(body)
        assert set(members) == {"node1", "node2", "node3"}
        assert [m for m in members.values() if m["is_self"]] == [members[node_id]]
        assert all(m["state"] == "alive" for m in members.values())
        # Metadata is replicated in full and R equals the cluster size, so
        # every node holds the record and the blob.
        assert body["storage"] == {"live_records": 1, "tombstones": 0, "blobs": 1}
