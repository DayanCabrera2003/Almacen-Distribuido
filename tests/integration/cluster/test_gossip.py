"""One gossip round between two real nodes."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from almacen.cluster.channels import ChannelPool
from almacen.cluster.gossip import gossip_once
from almacen.cluster.membership import Membership, NodeState
from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
PEERS = ("node1", "node2")


@pytest.fixture
def pair(tmp_path: Path):
    ports = {n: free_port() for n in PEERS}
    peers = tuple(Peer(n, f"127.0.0.1:{p}") for n, p in ports.items())

    nodes = {}
    channels = ChannelPool()
    for node_id in PEERS:
        store = MetadataStore(tmp_path / node_id / "metadata.db")
        membership = Membership(node_id, PEERS, timedelta(seconds=5))
        server = build_server(
            blob_store=BlobStore(tmp_path / node_id / "blobs"),
            metadata_store=store,
            node_id=node_id,
            membership=membership,
            port=ports[node_id],
        )
        server.start()
        nodes[node_id] = {
            "store": store,
            "membership": membership,
            "server": server,
            "settings": Settings(
                data_dir=tmp_path / node_id / "blobs",
                db_path=tmp_path / node_id / "metadata.db",
                node_id=node_id,
                peers=peers,
            ),
        }

    yield nodes, peers, channels

    channels.close()
    for node in nodes.values():
        node["server"].stop(0).wait()


def run_round(nodes, peers, channels, initiator: str) -> bool:
    other = next(p for p in peers if p.node_id != initiator)
    node = nodes[initiator]
    return gossip_once(
        other,
        settings=node["settings"],
        store=node["store"],
        membership=node["membership"],
        channels=channels,
        now=NOW,
    )


def test_one_round_converges_a_pair(pair):
    nodes, peers, channels = pair
    mine = FileRecord.new(name="mine.txt", content_hash="h1", node_id="node1")
    theirs = FileRecord.new(name="theirs.txt", content_hash="h2", node_id="node2")
    nodes["node1"]["store"].insert(mine)
    nodes["node2"]["store"].insert(theirs)

    assert run_round(nodes, peers, channels, "node1") is True

    for node_id in PEERS:
        known = {r.file_id for r in nodes[node_id]["store"].list_all()}
        assert known == {mine.file_id, theirs.file_id}, node_id


def test_concurrent_edits_merge_in_one_round(pair):
    nodes, peers, channels = pair
    base = FileRecord.new(
        name="a.txt", content_hash="h", node_id="node1", tags={"base"}
    )
    for node_id in PEERS:
        nodes[node_id]["store"].insert(base.copy())

    nodes["node1"]["store"].mutate(base.file_id, lambda r: r.add_tag("one", "node1"))
    nodes["node2"]["store"].mutate(base.file_id, lambda r: r.add_tag("two", "node2"))

    run_round(nodes, peers, channels, "node1")

    for node_id in PEERS:
        record = nodes[node_id]["store"].get(base.file_id)
        assert record.tags == {"base", "one", "two"}, node_id


def test_a_tombstone_propagates(pair):
    nodes, peers, channels = pair
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    for node_id in PEERS:
        nodes[node_id]["store"].insert(record.copy())
    nodes["node1"]["store"].mutate(record.file_id, lambda r: r.mark_deleted("node1"))

    run_round(nodes, peers, channels, "node1")

    assert nodes["node2"]["store"].get(record.file_id).tombstone is True


def test_an_unreachable_peer_is_reported_not_raised(pair):
    nodes, peers, channels = pair
    nodes["node2"]["server"].stop(0).wait()

    assert run_round(nodes, peers, channels, "node1") is False
    assert (
        nodes["node1"]["membership"].state_of("node2", NOW) is NodeState.SUSPECT
    )


def test_a_successful_round_marks_the_peer_alive_again(pair):
    nodes, peers, channels = pair
    nodes["node1"]["membership"].record_unreachable("node2", NOW)

    assert run_round(nodes, peers, channels, "node1") is True
    assert nodes["node1"]["membership"].state_of("node2", NOW) is NodeState.ALIVE


def test_gossip_is_idempotent(pair):
    nodes, peers, channels = pair
    record = FileRecord.new(
        name="a.txt", content_hash="h", node_id="node1", tags={"x"}
    )
    nodes["node1"]["store"].insert(record)

    for _ in range(3):
        run_round(nodes, peers, channels, "node1")

    after = nodes["node2"]["store"].get(record.file_id)
    assert after.tags == {"x"}
    assert after.vector_clock.counters == record.vector_clock.counters


def test_a_peer_that_dies_mid_exchange_is_reported_not_raised(pair, monkeypatch):
    """The peer answers the digest, then goes away before the pushes land.

    This is the scenario the whole phase exists for, and it runs on a background
    thread: an exception escaping `gossip_once` would stop this node gossiping
    for good, with nothing in the logs to say why.
    """
    import grpc

    from almacen.cluster import gossip as gossip_module

    nodes, peers, channels = pair
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    nodes["node1"]["store"].insert(record)  # node2 will ask for it

    real_stub_factory = gossip_module.pb_grpc.ClusterStub

    class DiesAfterAnswering:
        def __init__(self, channel):
            self._inner = real_stub_factory(channel)

        def Gossip(self, request, timeout=None):
            return self._inner.Gossip(request, timeout=timeout)

        def ReplicateRecord(self, request, timeout=None):
            raise grpc.RpcError("peer went away after answering")

    monkeypatch.setattr(gossip_module.pb_grpc, "ClusterStub", DiesAfterAnswering)

    assert run_round(nodes, peers, channels, "node1") is False


def test_an_undecodable_record_is_reported_not_raised(pair, monkeypatch):
    from almacen.cluster import gossip as gossip_module

    nodes, peers, channels = pair
    nodes["node2"]["store"].insert(
        FileRecord.new(name="a.txt", content_hash="h", node_id="node2")
    )

    def explode(_message):
        raise ValueError("malformed record from a buggy peer")

    monkeypatch.setattr(gossip_module, "message_to_record", explode)

    assert run_round(nodes, peers, channels, "node1") is False
