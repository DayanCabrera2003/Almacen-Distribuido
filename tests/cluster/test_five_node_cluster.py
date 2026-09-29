"""End-to-end: a five-node cluster behaves like one store from any entry point."""
import hashlib
from collections.abc import Iterator
from contextlib import ExitStack
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from almacen.cluster.placement import replica_set
from almacen.config import Peer, Settings
from almacen.main import create_app
from tests.helpers import free_port

NODE_COUNT = 5


@pytest.fixture
def cluster(tmp_path: Path) -> Iterator[dict[str, TestClient]]:
    """Five nodes, each with its own storage, all sharing one membership list.

    Ports are allocated before any app is built so that every node's peer list
    contains every other node's real address.
    """
    ports = {f"node{i}": free_port() for i in range(1, NODE_COUNT + 1)}
    peers = tuple(
        Peer(node_id=node_id, address=f"127.0.0.1:{port}")
        for node_id, port in ports.items()
    )

    with ExitStack() as stack:
        clients: dict[str, TestClient] = {}
        for node_id, port in ports.items():
            settings = Settings(
                data_dir=tmp_path / node_id / "blobs",
                db_path=tmp_path / node_id / "metadata.db",
                node_id=node_id,
                grpc_port=port,
                peers=peers,
                replication_factor=3,
                write_quorum=2,
            )
            # Entering the TestClient context runs the lifespan, which starts the
            # node's real gRPC server.
            clients[node_id] = stack.enter_context(TestClient(create_app(settings)))
        yield clients


def test_a_file_uploaded_to_one_node_downloads_from_every_other(cluster):
    content = b"the same bytes everywhere"

    upload = cluster["node1"].post(
        "/files",
        files={"file": ("report.txt", content, "text/plain")},
        data={"name": "report.txt", "tags": "invoice,urgent"},
    )
    assert upload.status_code == 201
    file_id = upload.json()["file_id"]

    for node_id, client in cluster.items():
        download = client.get(f"/files/{file_id}")
        assert download.status_code == 200, f"{node_id} could not serve the content"
        assert download.content == content


def _payload_not_replicated_on(coordinator: str, node_ids: list[str]) -> bytes:
    """Find content whose replica set excludes `coordinator`.

    Picking a payload by hand is a trap: whether the uploading node happens to
    be one of its own replicas depends on the SHA-256 of the literal. If it is,
    the "exactly three holders" assertion below passes even against a
    coordinator that keeps a copy of everything, because that copy would be a
    legitimate replica anyway — and the test silently stops guarding the
    property it looks like it guards.
    """
    for index in range(2000):
        content = f"sharded content {index}".encode()
        digest = hashlib.sha256(content).hexdigest()
        if coordinator not in replica_set(digest, node_ids, r=3):
            return content
    raise AssertionError(f"no payload found whose replica set excludes {coordinator}")


def test_content_lands_on_exactly_three_of_five_nodes(cluster, tmp_path: Path):
    # Upload through a node that is deliberately NOT one of the replicas, so
    # three holders can only mean the coordinator kept nothing for itself.
    content = _payload_not_replicated_on("node2", list(cluster))

    upload = cluster["node2"].post(
        "/files",
        files={"file": ("a.txt", content, "text/plain")},
        data={"name": "a.txt"},
    )
    content_hash = upload.json()["content_hash"]

    holders = {
        node_id
        for node_id in cluster
        if (tmp_path / node_id / "blobs" / content_hash).exists()
    }

    assert "node2" not in holders, (
        "the coordinator is not a replica for this content, so holding a copy "
        "means it hoards every blob it routes"
    )
    assert len(holders) == 3, f"expected R=3 replicas, content is on {holders}"
    assert holders == set(replica_set(content_hash, list(cluster), r=3))


def test_metadata_is_queryable_from_every_node(cluster):
    cluster["node3"].post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "invoice,draft"},
    )
    cluster["node4"].post(
        "/files",
        files={"file": ("b.txt", b"b", "text/plain")},
        data={"name": "b.txt", "tags": "invoice"},
    )

    for node_id, client in cluster.items():
        both = client.get("/files", params={"tags": "invoice", "mode": "or"})
        assert {f["name"] for f in both.json()} == {"a.txt", "b.txt"}, (
            f"{node_id} has an incomplete metadata replica"
        )

        intersection = client.get("/files", params={"tags": "invoice,draft"})
        assert [f["name"] for f in intersection.json()] == ["a.txt"]


def test_a_tag_added_on_one_node_is_visible_on_the_others(cluster):
    upload = cluster["node1"].post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "draft"},
    )
    file_id = upload.json()["file_id"]

    cluster["node5"].post(f"/files/{file_id}/tags", json={"tags": ["final"]})

    for node_id, client in cluster.items():
        tags = client.get(f"/files/{file_id}/tags").json()["tags"]
        assert sorted(tags) == ["draft", "final"], f"{node_id} missed the tag change"


def test_a_delete_on_one_node_tombstones_the_file_everywhere(cluster):
    upload = cluster["node1"].post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]

    assert cluster["node2"].delete(f"/files/{file_id}").status_code == 204

    for node_id, client in cluster.items():
        assert client.get(f"/files/{file_id}").status_code == 404, node_id
        assert client.get("/files").json() == [], node_id


def test_content_update_propagates_to_the_new_replica_set(cluster):
    upload = cluster["node1"].post(
        "/files",
        files={"file": ("a.txt", b"version one", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]

    patch = cluster["node3"].patch(
        f"/files/{file_id}", files={"file": ("a.txt", b"version two", "text/plain")}
    )
    assert patch.status_code == 200

    for node_id, client in cluster.items():
        assert client.get(f"/files/{file_id}").content == b"version two", node_id


def test_an_empty_file_uploads_and_downloads_across_the_cluster(cluster):
    upload = cluster["node1"].post(
        "/files",
        files={"file": ("empty.txt", b"", "text/plain")},
        data={"name": "empty.txt"},
    )
    assert upload.status_code == 201, upload.text
    file_id = upload.json()["file_id"]

    for node_id, client in cluster.items():
        download = client.get(f"/files/{file_id}")
        assert download.status_code == 200, f"{node_id} could not serve the empty file"
        assert download.content == b""
