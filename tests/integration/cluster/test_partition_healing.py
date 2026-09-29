"""A node that missed updates while unreachable catches up once it can talk.

This is the phase's reason to exist. Phase 3 made the merge correct but left
delivery as a single best-effort push, so a node that was down for one write
missed it permanently — making the convergence guarantee vacuous in exactly the
situation it was built for.

A partition is simulated by stopping a node's gRPC server and restarting it on
the same port: deterministic, fast, and it exercises the same code paths as a
real network cut.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import grpc
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
SUSPICION = timedelta(seconds=5)
NODES = ("node1", "node2", "node3")


class Node:
    def __init__(self, node_id: str, tmp_path: Path, port: int, peers, channels):
        self.node_id = node_id
        self.port = port
        self.blob_store = BlobStore(tmp_path / node_id / "blobs")
        self.store = MetadataStore(tmp_path / node_id / "metadata.db")
        self.membership = Membership(node_id, NODES, SUSPICION)
        self.settings = Settings(
            data_dir=tmp_path / node_id / "blobs",
            db_path=tmp_path / node_id / "metadata.db",
            node_id=node_id,
            peers=peers,
        )
        self.client = ReplicationClient(self.settings, self.blob_store, channels)
        self._channels = channels
        self._peers = peers
        self.server = None
        self.start()

    def start(self) -> None:
        self.server = build_server(
            blob_store=self.blob_store,
            metadata_store=self.store,
            node_id=self.node_id,
            membership=self.membership,
            port=self.port,
        )
        # add_insecure_port returns 0 on failure without raising, so a rebind
        # that silently fails would leave a server bound to nothing and produce
        # a baffling failure further down.
        self.server.start()
        with grpc.insecure_channel(f"127.0.0.1:{self.port}") as probe:
            grpc.channel_ready_future(probe).result(timeout=5)

    def stop(self) -> None:
        if self.server is not None:
            self.server.stop(0).wait()
            self.server = None

    @property
    def is_reachable(self) -> bool:
        return self.server is not None

    def gossip_with(self, targets, now=NOW) -> None:
        chosen = [p for p in self._peers if p.node_id in targets]
        GossipLoop(
            settings=self.settings,
            store=self.store,
            blob_store=self.blob_store,
            membership=self.membership,
            channels=self._channels,
            replication_client=self.client,
            choose_peers=lambda _n: chosen,
            now=lambda: now,
        ).run_once()


@pytest.fixture
def cluster(tmp_path: Path):
    ports = {n: free_port() for n in NODES}
    peers = tuple(Peer(n, f"127.0.0.1:{p}") for n, p in ports.items())
    channels = ChannelPool()
    nodes = {n: Node(n, tmp_path, ports[n], peers, channels) for n in NODES}

    yield nodes

    channels.close()
    for node in nodes.values():
        node.stop()


def full_round(nodes, now=NOW) -> None:
    """Every reachable node gossips with every other reachable node.

    Deterministic round-robin rather than random selection: measured, random
    peers need up to 23 rounds to converge five nodes, which would make these
    tests flaky.

    A stopped node is skipped as an *initiator* too. Stopping its server only
    closes the inbound direction, but a real partition cuts both — and leaving
    it gossiping outward would let it pull exactly the updates the test is
    asserting it cannot see.
    """
    reachable = {n for n, node in nodes.items() if node.is_reachable}
    for node_id in reachable:
        nodes[node_id].gossip_with(reachable - {node_id}, now=now)


def test_a_write_made_while_a_node_was_away_reaches_it_on_healing(cluster):
    """The regression Phase 3 could not pass."""
    nodes = cluster
    nodes["node3"].stop()

    record = FileRecord.new(name="written-during.txt", content_hash="h", node_id="node1")
    nodes["node1"].store.insert(record)
    full_round(nodes)
    assert nodes["node3"].store.get(record.file_id) is None

    nodes["node3"].start()
    full_round(nodes)

    assert nodes["node3"].store.get(record.file_id) is not None


def test_edits_on_both_sides_of_a_partition_survive(cluster):
    nodes = cluster
    base = FileRecord.new(
        name="a.txt", content_hash="h", node_id="node1", tags={"base"}
    )
    for node in nodes.values():
        node.store.insert(base.copy())

    nodes["node3"].stop()
    nodes["node1"].store.mutate(base.file_id, lambda r: r.add_tag("majority", "node1"))
    nodes["node3"].store.mutate(base.file_id, lambda r: r.add_tag("isolated", "node3"))

    nodes["node3"].start()
    full_round(nodes)
    full_round(nodes)

    for node_id, node in nodes.items():
        assert node.store.get(base.file_id).tags == {
            "base",
            "majority",
            "isolated",
        }, node_id


def test_a_delete_during_a_partition_reaches_the_isolated_node(cluster):
    nodes = cluster
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    for node in nodes.values():
        node.store.insert(record.copy())

    nodes["node3"].stop()
    nodes["node1"].store.mutate(record.file_id, lambda r: r.mark_deleted("node1"))
    nodes["node3"].start()
    full_round(nodes)

    for node_id, node in nodes.items():
        assert node.store.get(record.file_id).tombstone is True, node_id
        assert node.store.list_live() == [], node_id


def test_a_node_that_missed_an_upload_gains_the_record_and_its_blob(cluster):
    import hashlib

    nodes = cluster
    content = b"uploaded while node3 was away"
    content_hash = hashlib.sha256(content).hexdigest()

    nodes["node3"].stop()
    record = FileRecord.new(name="a.txt", content_hash=content_hash, node_id="node1")
    nodes["node1"].store.insert(record)
    nodes["node1"].blob_store.put(content)
    nodes["node2"].blob_store.put(content)

    nodes["node3"].start()
    full_round(nodes)

    assert nodes["node3"].store.get(record.file_id) is not None
    from almacen.cluster.placement import replica_set

    if "node3" in replica_set(content_hash, list(NODES), r=3):
        assert nodes["node3"].blob_store.get(content_hash) == content


def test_a_node_away_past_the_timeout_is_marked_dead_then_alive_again(cluster):
    nodes = cluster
    nodes["node3"].stop()

    nodes["node1"].gossip_with({"node3"}, now=NOW)
    assert nodes["node1"].membership.state_of("node3", NOW) is NodeState.SUSPECT
    assert nodes["node1"].membership.state_of("node3", NOW + SUSPICION) is NodeState.DEAD

    nodes["node3"].start()
    later = NOW + SUSPICION
    nodes["node1"].gossip_with({"node3"}, now=later)

    assert nodes["node1"].membership.state_of("node3", later) is NodeState.ALIVE


def test_a_wrongly_dead_node_clears_the_claim_itself(cluster):
    """Recovery is victim-initiated: dead peers are not gossiped to."""
    nodes = cluster
    nodes["node1"].membership.record_unreachable("node3", NOW)
    dead_at = NOW + SUSPICION
    assert nodes["node1"].membership.state_of("node3", dead_at) is NodeState.DEAD

    # node3 was alive all along and gossips outward.
    nodes["node3"].gossip_with({"node1"}, now=dead_at)

    assert nodes["node1"].membership.state_of("node3", dead_at) is NodeState.ALIVE


def test_extra_rounds_after_convergence_change_nothing(cluster):
    nodes = cluster
    record = FileRecord.new(
        name="a.txt", content_hash="h", node_id="node1", tags={"x"}
    )
    nodes["node1"].store.insert(record)
    full_round(nodes)

    settled = {
        n: (
            nodes[n].store.get(record.file_id).tags,
            nodes[n].store.get(record.file_id).vector_clock.counters,
        )
        for n in NODES
    }
    for _ in range(3):
        full_round(nodes)

    for node_id in NODES:
        after = nodes[node_id].store.get(record.file_id)
        assert (after.tags, after.vector_clock.counters) == settled[node_id]
