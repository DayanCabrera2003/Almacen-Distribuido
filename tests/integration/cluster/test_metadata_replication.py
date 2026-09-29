from pathlib import Path

import pytest

from almacen.cluster.metadata_replicator import MetadataReplicator
from almacen.cluster.replicated_metadata_store import ReplicatedMetadataStore
from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port

NODE_COUNT = 3


@pytest.fixture
def three_nodes(tmp_path: Path):
    """Three real nodes; index 0 is the coordinator under test."""
    stores: list[MetadataStore] = []
    servers = []
    peers: list[Peer] = []

    for index in range(1, NODE_COUNT + 1):
        node_id = f"node{index}"
        port = free_port()
        metadata_store = MetadataStore(tmp_path / node_id / "metadata.db")
        server = build_server(
            blob_store=BlobStore(tmp_path / node_id / "blobs"),
            metadata_store=metadata_store,
            node_id=node_id,
            port=port,
        )
        server.start()
        stores.append(metadata_store)
        servers.append(server)
        peers.append(Peer(node_id=node_id, address=f"127.0.0.1:{port}"))

    settings = Settings(
        data_dir=tmp_path / "node1" / "blobs",
        db_path=tmp_path / "node1" / "metadata.db",
        node_id="node1",
        peers=tuple(peers),
    )
    replicator = MetadataReplicator(settings)
    replicated = ReplicatedMetadataStore(stores[0], replicator)

    yield replicated, stores, servers

    replicator.close()
    for server in servers:
        server.stop(0).wait()


def test_insert_reaches_every_peer(three_nodes):
    replicated, stores, _ = three_nodes
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})

    replicated.insert(record)

    for index, store in enumerate(stores):
        stored = store.get(record.file_id)
        assert stored is not None, f"node{index + 1} never received the record"
        assert stored.name == "a.txt"
        assert stored.tags == {"x"}


def test_update_reaches_every_peer(three_nodes):
    replicated, stores, _ = three_nodes
    record = FileRecord.new(name="a.txt", content_hash="h1")
    replicated.insert(record)

    record.rename("renamed.txt")
    replicated.update(record)

    for store in stores:
        stored = store.get(record.file_id)
        assert stored is not None and stored.name == "renamed.txt"


def test_delete_propagates_as_a_tombstone(three_nodes):
    replicated, stores, _ = three_nodes
    record = FileRecord.new(name="a.txt", content_hash="h1")
    replicated.insert(record)

    record.mark_deleted()
    replicated.update(record)

    for store in stores:
        assert store.list_live() == []


def test_a_dead_peer_does_not_fail_the_write(three_nodes):
    replicated, stores, servers = three_nodes
    servers[2].stop(0).wait()  # node3 is gone

    record = FileRecord.new(name="a.txt", content_hash="h1")
    replicated.insert(record)  # must not raise

    # The reachable nodes have it; node3's divergence is Phase 4's problem.
    assert stores[0].get(record.file_id) is not None
    assert stores[1].get(record.file_id) is not None


def test_reads_are_served_locally(three_nodes):
    replicated, stores, servers = three_nodes
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})
    replicated.insert(record)

    for server in servers[1:]:
        server.stop(0).wait()

    assert replicated.get(record.file_id) is not None
    assert len(replicated.list_live()) == 1
