import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from almacen.cluster.channels import ChannelPool
from almacen.cluster.placement import replica_set
from almacen.cluster.replication_client import QuorumNotReached, ReplicationClient
from almacen.config import Peer, Settings
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port

NODE_COUNT = 5


@dataclass
class FakeNode:
    node_id: str
    blob_store: BlobStore
    server: object


@pytest.fixture
def cluster(tmp_path: Path) -> Iterator[tuple[list[FakeNode], tuple[Peer, ...]]]:
    """Five real gRPC nodes on loopback, each with its own blob directory."""
    nodes: list[FakeNode] = []
    peers: list[Peer] = []

    for index in range(1, NODE_COUNT + 1):
        node_id = f"node{index}"
        port = free_port()
        blob_store = BlobStore(tmp_path / node_id / "blobs")
        metadata_store = MetadataStore(tmp_path / node_id / "metadata.db")
        server = build_server(
            blob_store=blob_store,
            metadata_store=metadata_store,
            node_id=node_id,
            port=port,
        )
        server.start()
        nodes.append(FakeNode(node_id, blob_store, server))
        peers.append(Peer(node_id=node_id, address=f"127.0.0.1:{port}"))

    yield nodes, tuple(peers)

    for node in nodes:
        node.server.stop(0).wait()


@pytest.fixture
def client_on(cluster, tmp_path: Path):
    """Build a ReplicationClient that coordinates from a chosen node.

    The channel pool is owned by the fixture and closed on teardown, so channels
    do not leak across tests.
    """
    nodes, peers = cluster
    channels = ChannelPool()

    def build(node_id: str) -> ReplicationClient:
        settings = Settings(
            data_dir=tmp_path / node_id / "blobs",
            db_path=tmp_path / node_id / "metadata.db",
            node_id=node_id,
            peers=peers,
            replication_factor=3,
            write_quorum=2,
        )
        local = next(n for n in nodes if n.node_id == node_id)
        return ReplicationClient(settings, local.blob_store, channels)

    yield build
    channels.close()


def test_put_blob_stores_on_exactly_the_hrw_replica_set(cluster, client_on):
    nodes, peers = cluster
    client = client_on("node1")
    content = b"replicated content"
    content_hash = hashlib.sha256(content).hexdigest()

    returned_hash = client.put_blob(content)

    assert returned_hash == content_hash
    expected = set(replica_set(content_hash, [p.node_id for p in peers], r=3))
    holders = {n.node_id for n in nodes if n.blob_store.exists(content_hash)}
    assert holders == expected, (
        "content must land on exactly the HRW replica set, no more and no fewer"
    )
    assert len(holders) == 3


def test_put_blob_does_not_keep_a_local_copy_when_the_coordinator_is_not_a_replica(
    cluster, client_on
):
    nodes, peers = cluster
    node_ids = [p.node_id for p in peers]

    # Find content whose replica set excludes node1, so node1 coordinates a write
    # it is not a replica for.
    for index in range(2000):
        content = f"payload-{index}".encode()
        content_hash = hashlib.sha256(content).hexdigest()
        if "node1" not in replica_set(content_hash, node_ids, r=3):
            break
    else:
        pytest.fail("could not find content whose replica set excludes node1")

    client = client_on("node1")
    client.put_blob(content)

    local = next(n for n in nodes if n.node_id == "node1")
    assert not local.blob_store.exists(content_hash), (
        "coordinator must not hoard a copy it is not responsible for"
    )


# The coordinator writes its own replica through the local BlobStore, never over
# gRPC, so stopping the coordinator's *server* removes no acknowledgement at all.
# Both tests below therefore disable only REMOTE replicas. Getting this wrong is
# easy and silent: with r=3 over node1..node5, b"not enough replicas" maps to
# ['node3', 'node1', 'node2'], so killing the last two would kill the coordinator
# and the write would still reach quorum (1 local + 1 remote = W).
def _remote_replicas(content_hash: str, peers, coordinator: str) -> list[str]:
    node_ids = [p.node_id for p in peers]
    return [n for n in replica_set(content_hash, node_ids, r=3) if n != coordinator]


def test_put_blob_succeeds_when_exactly_the_write_quorum_is_reachable(
    cluster, client_on
):
    nodes, peers = cluster
    content = b"quorum edge case"
    content_hash = hashlib.sha256(content).hexdigest()

    # Drop one remote replica. Whether or not node1 is itself a replica, exactly
    # two acknowledgements remain, which is exactly W=2.
    doomed_id = _remote_replicas(content_hash, peers, "node1")[-1]
    next(n for n in nodes if n.node_id == doomed_id).server.stop(0).wait()

    client = client_on("node1")
    client.put_blob(content)  # must not raise

    holders = {n.node_id for n in nodes if n.blob_store.exists(content_hash)}
    assert len(holders) == 2


def test_put_blob_raises_when_the_write_quorum_cannot_be_met(cluster, client_on):
    nodes, peers = cluster
    content = b"not enough replicas"
    content_hash = hashlib.sha256(content).hexdigest()

    # Drop every remote replica, leaving at most the coordinator's own local
    # write — one acknowledgement at best, below W=2 either way.
    for node_id in _remote_replicas(content_hash, peers, "node1"):
        next(n for n in nodes if n.node_id == node_id).server.stop(0).wait()

    client = client_on("node1")

    with pytest.raises(QuorumNotReached):
        client.put_blob(content)


def test_get_blob_reads_a_local_copy_without_a_network_call(cluster, client_on):
    nodes, peers = cluster
    local = next(n for n in nodes if n.node_id == "node1")
    content_hash = local.blob_store.put(b"already here")

    client = client_on("node1")

    # Stop every other node: a local read must not need any of them.
    for node in nodes:
        if node.node_id != "node1":
            node.server.stop(0).wait()

    assert client.get_blob(content_hash) == b"already here"


def test_get_blob_fetches_from_a_replica_when_absent_locally(cluster, client_on):
    nodes, peers = cluster
    content = b"fetch me from a peer"

    writer = client_on("node1")
    content_hash = writer.put_blob(content)

    # Read from a node that is definitely not holding it.
    node_ids = [p.node_id for p in peers]
    replicas = replica_set(content_hash, node_ids, r=3)
    outsider = next(n for n in nodes if n.node_id not in replicas)
    reader = client_on(outsider.node_id)

    assert reader.get_blob(content_hash) == content


def test_get_blob_returns_none_when_no_replica_can_serve_it(cluster, client_on):
    client = client_on("node1")
    assert client.get_blob("a" * 64) is None


def test_get_blob_rejects_content_that_fails_its_hash_check(cluster, client_on):
    nodes, peers = cluster
    node_ids = [p.node_id for p in peers]
    bogus_hash = "b" * 64

    # Plant content on a replica under a hash that does not describe it, the way
    # a corrupted disk would.
    holder_id = replica_set(bogus_hash, node_ids, r=3)[0]
    holder = next(n for n in nodes if n.node_id == holder_id)
    (holder.blob_store._root / bogus_hash).write_bytes(b"corrupted payload")

    reader_id = next(
        n.node_id for n in nodes if n.node_id not in replica_set(bogus_hash, node_ids, 3)
    )
    client = client_on(reader_id)

    assert client.get_blob(bogus_hash) is None, (
        "content that does not match its address must be discarded, not served"
    )
