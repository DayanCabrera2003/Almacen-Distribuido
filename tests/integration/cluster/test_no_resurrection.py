"""A deleted file must not come back: the hazard the grace TTL exists to prevent.

These run over the real anti-entropy path — two gRPC servers and `gossip_once`
— rather than calling merge directly, because the hazard lives in the
interaction: a purged tombstone is indistinguishable from a file this node has
never seen, so a peer that still holds the file live teaches it back as live.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from almacen.cluster.channels import ChannelPool
from almacen.cluster.gc import collect
from almacen.cluster.gossip import gossip_once
from almacen.cluster.membership import Membership
from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.gc_state import BlobGCState
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port

PEERS = ("node1", "node2")
GRACE = timedelta(hours=1)


@pytest.fixture
def pair(tmp_path: Path):
    ports = {n: free_port() for n in PEERS}
    peers = tuple(Peer(n, f"127.0.0.1:{p}") for n, p in ports.items())

    nodes = {}
    channels = ChannelPool()
    for node_id in PEERS:
        root = tmp_path / node_id
        store = MetadataStore(root / "metadata.db")
        blob_store = BlobStore(root / "blobs")
        membership = Membership(node_id, PEERS, timedelta(seconds=5))
        server = build_server(
            blob_store=blob_store,
            metadata_store=store,
            node_id=node_id,
            membership=membership,
            port=ports[node_id],
        )
        server.start()
        nodes[node_id] = {
            "store": store,
            "blob_store": blob_store,
            "gc_state": BlobGCState(root / "gc_state.db"),
            "membership": membership,
            "server": server,
            "settings": Settings(
                data_dir=root / "blobs",
                db_path=root / "metadata.db",
                node_id=node_id,
                peers=peers,
            ),
        }

    def gossip(initiator: str) -> None:
        other = next(p for p in peers if p.node_id != initiator)
        node = nodes[initiator]
        assert gossip_once(
            other,
            settings=node["settings"],
            store=node["store"],
            membership=node["membership"],
            channels=channels,
            now=datetime.now(timezone.utc),
        )

    def run_gc(node_id: str, now: datetime) -> None:
        node = nodes[node_id]
        collect(
            store=node["store"],
            blob_store=node["blob_store"],
            gc_state=node["gc_state"],
            now=now,
            grace=GRACE,
            budget=100,
        )

    yield nodes, gossip, run_gc

    channels.close()
    for node in nodes.values():
        node["server"].stop(0).wait()
        node["gc_state"].close()


def share_one_file(nodes) -> FileRecord:
    """A file both nodes know about, as if replicated before any partition."""
    record = FileRecord.new(name="secret.txt", content_hash="h", node_id="node1")
    for node in nodes.values():
        node["store"].insert(record.copy())
    return record


def delete_on_node1(nodes, record: FileRecord) -> datetime:
    deleted = nodes["node1"]["store"].mutate(
        record.file_id, lambda r: r.mark_deleted("node1")
    )
    return deleted.tombstone_at


def live_ids(node) -> set:
    return {r.file_id for r in node["store"].list_live()}


def test_purging_before_the_peer_saw_the_delete_resurrects_the_file(pair):
    # The one test in the suite that asserts a bug exists. It documents what the
    # grace TTL buys: purge too early — here forced by bypassing the TTL — and a
    # partitioned peer undoes the user's delete. If this ever stops failing in
    # the way it asserts, the hazard moved, and the TTL's justification with it.
    nodes, gossip, _ = pair
    record = share_one_file(nodes)
    delete_on_node1(nodes, record)  # node2 is partitioned and never sees it

    nodes["node1"]["store"].purge(record.file_id)
    assert nodes["node1"]["store"].get(record.file_id) is None

    gossip("node2")  # the partition heals

    resurrected = nodes["node1"]["store"].get(record.file_id)
    assert resurrected is not None
    assert resurrected.tombstone is False
    assert live_ids(nodes["node1"]) == {record.file_id}


def test_gc_within_the_ttl_keeps_the_tombstone_so_the_delete_wins(pair):
    # The same partition as above, but purging goes through the collector. Any
    # GC round inside the TTL must keep the tombstone, so when the partition
    # heals the peer learns the delete instead of undoing it. This is the test
    # that fails if the grace check is taken out of `records_to_purge`.
    nodes, gossip, run_gc = pair
    record = share_one_file(nodes)
    deleted_at = delete_on_node1(nodes, record)

    run_gc("node1", deleted_at + GRACE / 2)
    assert nodes["node1"]["store"].get(record.file_id) is not None

    gossip("node2")

    for node_id, node in nodes.items():
        assert live_ids(node) == set(), node_id
        assert node["store"].get(record.file_id).tombstone is True, node_id


def test_a_purge_after_the_peer_saw_the_tombstone_stays_deleted(pair):
    # node1 purges after the TTL while node2, which has the tombstone, has not
    # collected yet. Gossip re-teaches node1 the record — as a tombstone, which
    # is harmless: it is not served, and node1 purges it again later.
    nodes, gossip, run_gc = pair
    record = share_one_file(nodes)
    deleted_at = delete_on_node1(nodes, record)
    gossip("node1")
    assert nodes["node2"]["store"].get(record.file_id).tombstone is True

    run_gc("node1", deleted_at + GRACE)
    assert nodes["node1"]["store"].get(record.file_id) is None

    gossip("node2")

    for node_id, node in nodes.items():
        assert live_ids(node) == set(), node_id


def test_with_the_ttl_respected_both_sides_end_with_nothing(pair):
    nodes, gossip, run_gc = pair
    record = share_one_file(nodes)
    deleted_at = delete_on_node1(nodes, record)
    gossip("node1")

    for node_id in PEERS:
        run_gc(node_id, deleted_at + GRACE)
    gossip("node1")
    gossip("node2")

    for node_id, node in nodes.items():
        assert node["store"].list_all() == [], node_id
