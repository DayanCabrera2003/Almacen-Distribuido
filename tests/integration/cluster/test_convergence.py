"""Concurrent edits made on different nodes converge to the same state.

This is the assignment's "tratamiento de actualizaciones concurrentes"
requirement, exercised through real gRPC servers rather than by calling merge
directly: what is under test is that the whole path — encode, send, decode,
merge, persist — preserves the CRDT properties.

Phase 2 would fail most of these: it overwrote the local record with whatever
arrived, so the outcome depended on arrival order.
"""
from pathlib import Path

import pytest

from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.record_codec import record_to_message
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port

import grpc

NODE_COUNT = 3


@pytest.fixture
def nodes(tmp_path: Path):
    """Three real nodes, each with its own store and gRPC server."""
    stores: dict[str, MetadataStore] = {}
    servers = []
    stubs: dict[str, pb_grpc.ClusterStub] = {}
    channels = []

    for index in range(1, NODE_COUNT + 1):
        node_id = f"node{index}"
        port = free_port()
        store = MetadataStore(tmp_path / node_id / "metadata.db")
        server = build_server(
            blob_store=BlobStore(tmp_path / node_id / "blobs"),
            metadata_store=store,
            node_id=node_id,
            port=port,
        )
        server.start()
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        stores[node_id] = store
        servers.append(server)
        channels.append(channel)
        stubs[node_id] = pb_grpc.ClusterStub(channel)

    yield stores, stubs

    for channel in channels:
        channel.close()
    for server in servers:
        server.stop(0).wait()


def _seed_everywhere(stores, record: FileRecord) -> None:
    for store in stores.values():
        store.insert(record.copy())


def _push(stubs, record: FileRecord, *, to: list[str]) -> None:
    message = record_to_message(record)
    for node_id in to:
        stubs[node_id].ReplicateRecord(message)


def test_concurrent_tag_additions_on_two_nodes_both_survive(nodes):
    stores, stubs = nodes
    base = FileRecord.new(
        name="a.txt", content_hash="h", node_id="node1", tags={"shared"}
    )
    _seed_everywhere(stores, base)

    # Each node edits its own copy without seeing the other's.
    on_node1 = stores["node1"].mutate(
        base.file_id, lambda r: r.add_tag("from-node1", "node1")
    )
    on_node2 = stores["node2"].mutate(
        base.file_id, lambda r: r.add_tag("from-node2", "node2")
    )

    # The partition heals: each pushes to everyone else.
    _push(stubs, on_node1, to=["node2", "node3"])
    _push(stubs, on_node2, to=["node1", "node3"])

    expected = {"shared", "from-node1", "from-node2"}
    for node_id, store in stores.items():
        assert store.get(base.file_id).tags == expected, f"{node_id} diverged"


def test_a_concurrent_rename_resolves_the_same_way_on_every_node(nodes):
    stores, stubs = nodes
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    _seed_everywhere(stores, base)

    on_node1 = stores["node1"].mutate(
        base.file_id, lambda r: r.rename("from-node1.txt", "node1")
    )
    on_node2 = stores["node2"].mutate(
        base.file_id, lambda r: r.rename("from-node2.txt", "node2")
    )

    _push(stubs, on_node1, to=["node2", "node3"])
    _push(stubs, on_node2, to=["node1", "node3"])

    names = {store.get(base.file_id).name for store in stores.values()}
    assert len(names) == 1, f"nodes disagree on the winning name: {names}"


def test_a_rename_on_one_node_and_a_tag_edit_on_another_both_survive(nodes):
    stores, stubs = nodes
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    _seed_everywhere(stores, base)

    renamed = stores["node1"].mutate(
        base.file_id, lambda r: r.rename("renamed.txt", "node1")
    )
    tagged = stores["node2"].mutate(base.file_id, lambda r: r.add_tag("new", "node2"))

    _push(stubs, renamed, to=["node2", "node3"])
    _push(stubs, tagged, to=["node1", "node3"])

    for node_id, store in stores.items():
        record = store.get(base.file_id)
        assert record.name == "renamed.txt", f"{node_id} lost the rename"
        assert record.tags == {"new"}, f"{node_id} lost the tag"


def test_a_delete_on_one_node_outlasts_an_edit_on_another(nodes):
    stores, stubs = nodes
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    _seed_everywhere(stores, base)

    deleted = stores["node1"].mutate(base.file_id, lambda r: r.mark_deleted("node1"))
    edited = stores["node2"].mutate(base.file_id, lambda r: r.add_tag("late", "node2"))

    _push(stubs, deleted, to=["node2", "node3"])
    _push(stubs, edited, to=["node1", "node3"])

    for node_id, store in stores.items():
        assert store.get(base.file_id).tombstone is True, f"{node_id} resurrected it"
        assert store.list_live() == [], node_id


def test_a_concurrent_remove_and_add_of_the_same_tag_keeps_it(nodes):
    # The OR-Set's defining behaviour, end to end: node1 removes the tag it can
    # see while node2 re-adds it. The add wins, because a remove can only cancel
    # the occurrences it observed.
    stores, stubs = nodes
    base = FileRecord.new(
        name="a.txt", content_hash="h", node_id="node1", tags={"draft"}
    )
    _seed_everywhere(stores, base)

    removed = stores["node1"].mutate(
        base.file_id, lambda r: r.remove_tag("draft", "node1")
    )
    re_added = stores["node2"].mutate(
        base.file_id, lambda r: r.add_tag("draft", "node2")
    )

    _push(stubs, removed, to=["node2", "node3"])
    _push(stubs, re_added, to=["node1", "node3"])

    for node_id, store in stores.items():
        assert "draft" in store.get(base.file_id).tags, f"{node_id} lost the re-add"


def test_delivery_order_does_not_change_the_outcome(nodes):
    """The regression Phase 2 could not pass.

    node3 receives the two updates in one order; node2 receives them in the
    other. Under Phase 2's overwrite the later arrival won, so the two nodes
    ended up with different tag sets. Under a merge they agree.
    """
    stores, stubs = nodes
    base = FileRecord.new(
        name="a.txt", content_hash="h", node_id="node1", tags={"shared"}
    )
    _seed_everywhere(stores, base)

    first = stores["node1"].mutate(base.file_id, lambda r: r.add_tag("alpha", "node1"))
    second = stores["node2"].mutate(base.file_id, lambda r: r.add_tag("beta", "node2"))

    _push(stubs, first, to=["node3"])
    _push(stubs, second, to=["node3"])
    _push(stubs, second, to=["node2"])
    _push(stubs, first, to=["node2"])

    assert stores["node3"].get(base.file_id).tags == {"shared", "alpha", "beta"}
    assert stores["node2"].get(base.file_id).tags == {"shared", "alpha", "beta"}


def test_replaying_the_same_update_changes_nothing(nodes):
    stores, stubs = nodes
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    _seed_everywhere(stores, base)

    edited = stores["node1"].mutate(base.file_id, lambda r: r.add_tag("x", "node1"))

    _push(stubs, edited, to=["node2"])
    once = stores["node2"].get(base.file_id)
    _push(stubs, edited, to=["node2"])
    _push(stubs, edited, to=["node2"])
    thrice = stores["node2"].get(base.file_id)

    assert once.tags == thrice.tags
    assert once.vector_clock.counters == thrice.vector_clock.counters
