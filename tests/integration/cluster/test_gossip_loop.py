"""The driver. Every test here injects the clock and the peer choice, so none
of them sleep and none of them depend on a real timer."""
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from almacen.cluster.channels import ChannelPool
from almacen.cluster.gossip import GossipLoop
from almacen.cluster.membership import Membership, NodeState
from almacen.cluster.replication_client import ReplicationClient
from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
NODES = ("node1", "node2", "node3")


@pytest.fixture
def cluster(tmp_path: Path):
    ports = {n: free_port() for n in NODES}
    peers = tuple(Peer(n, f"127.0.0.1:{p}") for n, p in ports.items())
    channels = ChannelPool()
    nodes = {}

    for node_id in NODES:
        blob_store = BlobStore(tmp_path / node_id / "blobs")
        store = MetadataStore(tmp_path / node_id / "metadata.db")
        membership = Membership(node_id, NODES, timedelta(seconds=5))
        server = build_server(
            blob_store=blob_store,
            metadata_store=store,
            node_id=node_id,
            membership=membership,
            port=ports[node_id],
        )
        server.start()
        settings = Settings(
            data_dir=tmp_path / node_id / "blobs",
            db_path=tmp_path / node_id / "metadata.db",
            node_id=node_id,
            peers=peers,
        )
        nodes[node_id] = {
            "store": store,
            "blob_store": blob_store,
            "membership": membership,
            "server": server,
            "settings": settings,
            "client": ReplicationClient(settings, blob_store, channels),
        }

    yield nodes, peers, channels

    channels.close()
    for node in nodes.values():
        node["server"].stop(0).wait()


def make_loop(node, peers, channels, targets, now=NOW) -> GossipLoop:
    """A loop that gossips with exactly `targets`, at exactly `now`."""
    chosen = [p for p in peers if p.node_id in targets]
    return GossipLoop(
        settings=node["settings"],
        store=node["store"],
        blob_store=node["blob_store"],
        membership=node["membership"],
        channels=channels,
        replication_client=node["client"],
        choose_peers=lambda _now: chosen,
        now=lambda: now,
    )


def test_one_tick_reconciles_with_the_chosen_peers(cluster):
    nodes, peers, channels = cluster
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    nodes["node1"]["store"].insert(record)

    make_loop(nodes["node1"], peers, channels, {"node2", "node3"}).run_once()

    for node_id in ("node2", "node3"):
        assert nodes[node_id]["store"].get(record.file_id) is not None, node_id


def test_one_unreachable_peer_does_not_skip_the_other(cluster):
    nodes, peers, channels = cluster
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    nodes["node1"]["store"].insert(record)
    nodes["node2"]["server"].stop(0).wait()

    make_loop(nodes["node1"], peers, channels, {"node2", "node3"}).run_once()

    assert nodes["node3"]["store"].get(record.file_id) is not None
    assert nodes["node1"]["membership"].state_of("node2", NOW) is NodeState.SUSPECT


def test_an_unexpected_failure_does_not_end_the_round(cluster, monkeypatch):
    # gossip_once handles the expected failures; this is the backstop. One bad
    # peer must not stop the loop, because a thread that dies dies silently.
    from almacen.cluster import gossip as gossip_module

    nodes, peers, channels = cluster
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    nodes["node1"]["store"].insert(record)

    calls = []
    real = gossip_module.gossip_once

    def flaky(peer, **kwargs):
        calls.append(peer.node_id)
        if peer.node_id == "node2":
            raise RuntimeError("something nobody anticipated")
        return real(peer, **kwargs)

    monkeypatch.setattr(gossip_module, "gossip_once", flaky)

    make_loop(nodes["node1"], peers, channels, {"node2", "node3"}).run_once()

    assert calls == ["node2", "node3"], "the round continued past the failure"
    assert nodes["node3"]["store"].get(record.file_id) is not None


def test_start_then_stop_leaves_no_live_thread(cluster):
    nodes, peers, channels = cluster
    before = threading.active_count()

    loop = make_loop(nodes["node1"], peers, channels, {"node2"})
    loop.start()
    loop.stop(timeout=5.0)

    assert threading.active_count() == before


def test_the_loop_does_not_tick_before_its_first_interval(cluster):
    # Ticking at startup would make every app construction do network I/O
    # before anything asked it to.
    nodes, peers, channels = cluster
    ticks = []

    loop = make_loop(nodes["node1"], peers, channels, {"node2"})
    loop.run_once = lambda: ticks.append(1)
    loop.start()
    loop.stop(timeout=5.0)

    assert ticks == []


def test_stop_is_safe_when_never_started(cluster):
    nodes, peers, channels = cluster
    make_loop(nodes["node1"], peers, channels, {"node2"}).stop(timeout=1.0)


def test_starting_twice_is_an_error(cluster):
    nodes, peers, channels = cluster
    loop = make_loop(nodes["node1"], peers, channels, {"node2"})
    loop.start()
    try:
        with pytest.raises(RuntimeError, match="already started"):
            loop.start()
    finally:
        loop.stop(timeout=5.0)


def test_a_deterministic_schedule_converges_three_nodes(cluster):
    """Round-robin rather than random: measured, random selection needs up to
    23 rounds for five nodes, which would make this flaky."""
    nodes, peers, channels = cluster
    base = FileRecord.new(
        name="a.txt", content_hash="h", node_id="node1", tags={"base"}
    )
    for node in nodes.values():
        node["store"].insert(base.copy())
    for index, node_id in enumerate(NODES):
        nodes[node_id]["store"].mutate(
            base.file_id, lambda r, i=index, n=node_id: r.add_tag(f"t{i}", n)
        )

    for initiator in NODES:
        others = {n for n in NODES if n != initiator}
        make_loop(nodes[initiator], peers, channels, others).run_once()

    expected = {"base", "t0", "t1", "t2"}
    for node_id in NODES:
        assert nodes[node_id]["store"].get(base.file_id).tags == expected, node_id
