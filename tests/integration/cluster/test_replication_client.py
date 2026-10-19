import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from almacen.cluster.channels import ChannelPool
from almacen.cluster.membership import Membership
from almacen.cluster.placement import replica_set
from almacen.cluster.replication_client import QuorumNotReached, ReplicationClient
from almacen.config import Peer, Settings
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port

NODE_COUNT = 5
SUSPICION_TIMEOUT = timedelta(seconds=60)


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
def declare_dead():
    """Node ids that every client built by `client_on` sees as dead.

    Fill it before building the client: each client gets its own Membership,
    seeded from this set at construction.
    """
    return set()


@pytest.fixture
def client_on(cluster, tmp_path: Path, declare_dead: set[str]):
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
        membership = Membership(
            node_id=node_id,
            peers=tuple(p.node_id for p in peers),
            suspicion_timeout=SUSPICION_TIMEOUT,
        )
        # Suspected long enough ago that the suspicion has aged into death.
        long_ago = datetime.now(timezone.utc) - 2 * SUSPICION_TIMEOUT
        for dead in declare_dead:
            membership.record_unreachable(dead, long_ago)
        local = next(n for n in nodes if n.node_id == node_id)
        return ReplicationClient(settings, local.blob_store, channels, membership)

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


def test_put_blob_replicates_an_empty_blob(cluster, client_on):
    """An empty file is a legitimate file and must reach its replica set.

    `_chunk` must emit one chunk for empty content: a stream with no chunks at
    all is indistinguishable from a malformed request, and the servicer rejects
    it, so every remote replica would refuse and the quorum would fail.
    """
    nodes, peers = cluster
    client = client_on("node1")

    content_hash = client.put_blob(b"")

    assert content_hash == hashlib.sha256(b"").hexdigest()
    holders = {n.node_id for n in nodes if n.blob_store.exists(content_hash)}
    assert holders == set(replica_set(content_hash, [p.node_id for p in peers], r=3))
    assert len(holders) == 3


def test_get_blob_returns_an_empty_blob_as_empty_bytes_not_none(cluster, client_on):
    # b"" is falsy, so any `if content:` check along the path would turn a
    # successful read of an empty file into a spurious 503.
    nodes, peers = cluster
    writer = client_on("node1")
    content_hash = writer.put_blob(b"")

    node_ids = [p.node_id for p in peers]
    outsider = next(
        n for n in nodes if n.node_id not in replica_set(content_hash, node_ids, r=3)
    )
    reader = client_on(outsider.node_id)

    assert reader.get_blob(content_hash) == b""


def test_get_blob_rejects_a_corrupted_local_copy(cluster, client_on):
    """A local blob gets the same integrity check a remote one does.

    Without this, corruption on the coordinator's own disk is served to the
    client, while the identical corruption on a peer is caught and skipped —
    so whether a read is trustworthy would depend on which node answered it.
    """
    nodes, peers = cluster
    node_ids = [p.node_id for p in peers]
    bogus_hash = "d" * 64

    # Corrupt the copy on a node that is itself a replica, and read through it,
    # so the local branch of get_blob is the one exercised.
    holder_id = replica_set(bogus_hash, node_ids, r=3)[0]
    holder = next(n for n in nodes if n.node_id == holder_id)
    (holder.blob_store._root / bogus_hash).write_bytes(b"locally corrupted")

    client = client_on(holder_id)

    assert client.get_blob(bogus_hash) is None


def test_put_blob_does_not_contact_a_replica_declared_dead(
    cluster, client_on, declare_dead
):
    """A replica the failure detector has given up on costs no RPC timeout.

    It counts as a missing acknowledgement, exactly as if the RPC had timed
    out; the blob reaches it later through anti-entropy's blob catch-up.
    """
    nodes, peers = cluster
    content = b"one replica is dead"
    content_hash = hashlib.sha256(content).hexdigest()
    doomed_id = _remote_replicas(content_hash, peers, "node1")[-1]
    declare_dead.add(doomed_id)

    client_on("node1").put_blob(content)  # W=2 is still met

    doomed = next(n for n in nodes if n.node_id == doomed_id)
    assert not doomed.blob_store.exists(content_hash), (
        "the dead replica's server is up only to prove that nothing was sent"
    )


def test_put_blob_falls_back_to_dead_replicas_when_quorum_needs_them(
    cluster, client_on, declare_dead
):
    """A stale death must not refuse a write the network could take.

    Right after a partition heals, membership still reports the far side dead
    for a few gossip ticks. Here every remote replica is declared dead but is
    in fact up, so the write must still reach them and meet W.
    """
    nodes, peers = cluster
    content = b"the dead are back"
    content_hash = hashlib.sha256(content).hexdigest()
    remotes = _remote_replicas(content_hash, peers, "node1")
    declare_dead.update(remotes)

    client_on("node1").put_blob(content)  # must not raise

    for node_id in remotes:
        assert next(n for n in nodes if n.node_id == node_id).blob_store.exists(
            content_hash
        )


def test_get_blob_still_reads_from_a_replica_declared_dead(
    cluster, client_on, declare_dead
):
    """Reads try dead replicas last, but do try them.

    A false death is possible, and skipping the only holder of a blob would turn
    a slow read into a 503.
    """
    nodes, peers = cluster
    node_ids = [p.node_id for p in peers]
    content = b"only on a dead-looking node"
    content_hash = hashlib.sha256(content).hexdigest()
    holder_id = replica_set(content_hash, node_ids, r=3)[0]
    next(n for n in nodes if n.node_id == holder_id).blob_store.put(content)
    declare_dead.add(holder_id)

    reader_id = next(
        n for n in node_ids if n not in replica_set(content_hash, node_ids, r=3)
    )

    assert client_on(reader_id).get_blob(content_hash) == content
