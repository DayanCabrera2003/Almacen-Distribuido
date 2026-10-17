"""What a client sees while content and metadata disagree, before gossip repairs it.

Content is placed on R=3 nodes with a W=2 quorum; metadata is committed locally
and pushed to every peer best effort. So a node can hold a record that is
older than the cluster's, a record whose blob it does not hold, or a blob whose
record has not reached it yet. These tests pin down the client-visible outcome
of each case through the HTTP API of a real four-node cluster (four, so that a
node outside a blob's replica set exists), with gossip and GC driven by hand on
an explicit clock: no background threads, no sleeping.

A node that misses a push is modelled by stopping its gRPC server, which cuts
every inbound call to it. Its own outbound calls still work, which is what lets
it later initiate the gossip round that repairs it.
"""
import hashlib
from collections.abc import Iterator
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from almacen.cluster.gc import collect
from almacen.cluster.gossip import gossip_once
from almacen.cluster.metadata_replicator import MetadataReplicator
from almacen.cluster.placement import replica_set
from almacen.config import Peer, Settings
from almacen.main import create_app
from almacen.storage.gc_state import BlobGCState
from tests.helpers import free_port

NODES = ("node1", "node2", "node3", "node4")
GRACE = timedelta(hours=1)
JUST = timedelta(seconds=1)


@pytest.fixture
def cluster(tmp_path: Path) -> Iterator[dict[str, TestClient]]:
    ports = {node_id: free_port() for node_id in NODES}
    peers = tuple(Peer(node_id, f"127.0.0.1:{port}") for node_id, port in ports.items())
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
                gossip_interval_seconds=0,
            )
            clients[node_id] = stack.enter_context(TestClient(create_app(settings)))
        gc_states = {
            node_id: BlobGCState(tmp_path / node_id / "gc_state.db") for node_id in NODES
        }
        stack.callback(lambda: [state.close() for state in gc_states.values()])
        for node_id, client in clients.items():
            client.app.state.test_gc_state = gc_states[node_id]
        yield clients


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def replicas_of(content: bytes) -> list[str]:
    return replica_set(sha256(content), NODES, r=3)


def content_not_placed_on(node_id: str, label: str) -> bytes:
    """Bytes whose HRW replica set leaves out `node_id`.

    Deterministic: the first candidate that qualifies is always the same one.
    """
    for i in range(100):
        content = f"{label} #{i}".encode()
        if node_id not in replicas_of(content):
            return content
    raise AssertionError("no candidate content found")  # pragma: no cover


def cut_off(client: TestClient) -> None:
    """Stop the node receiving anything: pushes, PutBlob and GetBlob alike."""
    client.app.state.grpc_server.stop(0).wait()


def gossip(initiator: TestClient, responder: str) -> None:
    state = initiator.app.state
    peer = next(p for p in state.settings.peers if p.node_id == responder)
    assert gossip_once(
        peer,
        settings=state.settings,
        store=state.local_metadata_store,
        membership=state.membership,
        channels=state.channels,
        now=datetime.now(timezone.utc),
    )


def run_gc(client: TestClient, now: datetime) -> None:
    state = client.app.state
    collect(
        store=state.local_metadata_store,
        blob_store=state.blob_store,
        gc_state=state.test_gc_state,
        now=now,
        grace=GRACE,
        budget=100,
    )


def upload(client: TestClient, content: bytes) -> str:
    response = client.post(
        "/files", files={"file": ("a.txt", content)}, data={"name": "a.txt"}
    )
    assert response.status_code == 201, response.text
    return response.json()["file_id"]


def patch_content(client: TestClient, file_id: str, content: bytes) -> str:
    response = client.patch(f"/files/{file_id}", files={"file": ("a.txt", content)})
    assert response.status_code == 200, response.text
    return response.json()["content_hash"]


def test_a_node_that_missed_a_remote_patch_serves_the_old_content(cluster):
    # The old content is placed so that node4 is not one of its replicas:
    # node4 has to fetch it over the network, which is the case where the old
    # blob's lifetime on other nodes matters.
    old = content_not_placed_on("node4", "version one")
    file_id = upload(cluster["node1"], old)

    cut_off(cluster["node4"])
    patch_content(cluster["node1"], file_id, b"version two")

    # The writer and every node the push reached serve the new content ...
    for node_id in ("node1", "node2", "node3"):
        assert cluster[node_id].get(f"/files/{file_id}").content == b"version two"
    # ... while node4 still points at the old hash and serves the old bytes,
    # fetched from one of the old blob's replicas: stale, but not an error.
    stale = cluster["node4"].get(f"/files/{file_id}")
    assert stale.status_code == 200
    assert stale.content == old

    # One gossip round, initiated by node4, brings it up to date.
    gossip(cluster["node4"], "node1")
    assert cluster["node4"].get(f"/files/{file_id}").content == b"version two"


def test_within_the_grace_ttl_the_old_content_stays_readable(cluster):
    old = content_not_placed_on("node4", "version one")
    file_id = upload(cluster["node1"], old)
    cut_off(cluster["node4"])
    patch_content(cluster["node1"], file_id, b"version two")

    # The PATCH orphaned the old blob on its replicas: GC makes it a candidate
    # now, and deletes it no sooner than one grace TTL later.
    orphaned_at = datetime.now(timezone.utc)
    for node_id in ("node1", "node2", "node3"):
        run_gc(cluster[node_id], orphaned_at)
        run_gc(cluster[node_id], orphaned_at + GRACE - JUST)

    assert cluster["node4"].get(f"/files/{file_id}").content == old


def test_beyond_the_grace_ttl_a_stale_node_answers_503_until_gossip_reaches_it(
    cluster,
):
    old = content_not_placed_on("node4", "version one")
    file_id = upload(cluster["node1"], old)
    cut_off(cluster["node4"])
    patch_content(cluster["node1"], file_id, b"version two")

    orphaned_at = datetime.now(timezone.utc)
    for node_id in ("node1", "node2", "node3"):
        run_gc(cluster[node_id], orphaned_at)
        run_gc(cluster[node_id], orphaned_at + GRACE)

    # The record still exists on node4, so 404 would be wrong; its bytes are
    # gone from every replica, so it cannot serve them. It never serves wrong
    # bytes: a blob is addressed by its hash.
    assert cluster["node4"].get(f"/files/{file_id}").status_code == 503

    gossip(cluster["node4"], "node1")
    assert cluster["node4"].get(f"/files/{file_id}").content == b"version two"


def test_a_stale_node_that_is_a_replica_keeps_the_old_blob_it_still_references(
    cluster,
):
    # Reference counts are local: the stale node's record still points at the
    # old blob, so its own GC never collects it, whatever the time. The stale
    # node is a replica of the old content other than the writer, node1.
    old = content_not_placed_on("node1", "version one")
    holder = replicas_of(old)[0]
    file_id = upload(cluster["node1"], old)
    cut_off(cluster[holder])
    patch_content(cluster["node1"], file_id, b"version two")

    now = datetime.now(timezone.utc)
    run_gc(cluster[holder], now)
    run_gc(cluster[holder], now + 10 * GRACE)

    assert cluster[holder].get(f"/files/{file_id}").content == old


def test_a_node_the_tombstone_has_not_reached_still_serves_the_file(cluster):
    file_id = upload(cluster["node1"], b"soon deleted")

    cut_off(cluster["node4"])
    assert cluster["node1"].delete(f"/files/{file_id}").status_code == 204

    assert cluster["node1"].get(f"/files/{file_id}").status_code == 404
    assert cluster["node4"].get(f"/files/{file_id}").content == b"soon deleted"
    assert [f["file_id"] for f in cluster["node4"].get("/files").json()] == [file_id]

    gossip(cluster["node4"], "node1")
    assert cluster["node4"].get(f"/files/{file_id}").status_code == 404
    assert cluster["node4"].get("/files").json() == []


def test_content_whose_record_has_not_arrived_is_invisible_and_not_collected_early(
    cluster, monkeypatch: pytest.MonkeyPatch
):
    content = content_not_placed_on("node1", "record in flight")
    content_hash = sha256(content)
    replica = replicas_of(content)[0]
    # The window between steps 3 and 4 of an upload: the blob has reached its
    # replicas, the record has not yet been pushed anywhere.
    with monkeypatch.context() as patched:
        patched.setattr(MetadataReplicator, "replicate", lambda self, record: None)
        file_id = upload(cluster["node1"], content)

    assert cluster[replica].app.state.blob_store.exists(content_hash)
    assert cluster[replica].get(f"/files/{file_id}").status_code == 404
    assert cluster[replica].get("/files").json() == []

    # The replica's GC sees an unreferenced blob and starts its clock, but the
    # grace TTL leaves time for the record to arrive ...
    seen_at = datetime.now(timezone.utc)
    run_gc(cluster[replica], seen_at)
    assert cluster[replica].app.state.blob_store.exists(content_hash)

    gossip(cluster[replica], "node1")
    assert cluster[replica].get(f"/files/{file_id}").content == content

    # ... and once it has, the blob is referenced again and survives.
    run_gc(cluster[replica], seen_at + GRACE)
    assert cluster[replica].app.state.blob_store.exists(content_hash)
