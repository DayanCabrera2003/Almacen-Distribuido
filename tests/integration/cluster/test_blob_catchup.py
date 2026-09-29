"""Metadata converging is not enough: a node needs the bytes it is a replica for."""
import hashlib
from pathlib import Path

import pytest

from almacen.cluster.channels import ChannelPool
from almacen.cluster.gossip import catch_up_blobs
from almacen.cluster.placement import replica_set
from almacen.cluster.replication_client import ReplicationClient
from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port

NODE_COUNT = 5
NODES = tuple(f"node{i}" for i in range(1, NODE_COUNT + 1))


@pytest.fixture
def cluster(tmp_path: Path):
    ports = {n: free_port() for n in NODES}
    peers = tuple(Peer(n, f"127.0.0.1:{p}") for n, p in ports.items())
    channels = ChannelPool()
    nodes = {}

    for node_id in NODES:
        blob_store = BlobStore(tmp_path / node_id / "blobs")
        store = MetadataStore(tmp_path / node_id / "metadata.db")
        server = build_server(
            blob_store=blob_store,
            metadata_store=store,
            node_id=node_id,
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
            "blob_store": blob_store,
            "store": store,
            "server": server,
            "settings": settings,
            "client": ReplicationClient(settings, blob_store, channels),
        }

    yield nodes, peers

    channels.close()
    for node in nodes.values():
        node["server"].stop(0).wait()


def seed_everywhere(nodes, content: bytes, holders) -> FileRecord:
    """Give every node the record; give only `holders` the bytes."""
    content_hash = hashlib.sha256(content).hexdigest()
    record = FileRecord.new(name="a.txt", content_hash=content_hash, node_id="node1")
    for node_id, node in nodes.items():
        node["store"].insert(record.copy())
        if node_id in holders:
            node["blob_store"].put(content)
    return record


def run_catch_up(node, budget=8) -> int:
    return catch_up_blobs(
        settings=node["settings"],
        store=node["store"],
        blob_store=node["blob_store"],
        replication_client=node["client"],
        budget=budget,
    )


def test_a_replica_missing_its_blob_fetches_it(cluster):
    nodes, peers = cluster
    content = b"content that should be replicated"
    content_hash = hashlib.sha256(content).hexdigest()
    replicas = replica_set(content_hash, list(NODES), r=3)

    # Only the first replica holds the bytes; the second is behind.
    seed_everywhere(nodes, content, holders={replicas[0]})
    behind = nodes[replicas[1]]
    assert not behind["blob_store"].exists(content_hash)

    assert run_catch_up(behind) == 1
    assert behind["blob_store"].get(content_hash) == content


def test_a_node_that_is_not_a_replica_fetches_nothing(cluster):
    """Otherwise every node ends up holding everything and sharding is undone."""
    nodes, peers = cluster
    content = b"content that should be replicated"
    content_hash = hashlib.sha256(content).hexdigest()
    replicas = replica_set(content_hash, list(NODES), r=3)
    outsider_id = next(n for n in NODES if n not in replicas)

    seed_everywhere(nodes, content, holders={replicas[0]})
    outsider = nodes[outsider_id]

    assert run_catch_up(outsider) == 0
    assert not outsider["blob_store"].exists(content_hash)


def test_a_replica_that_already_holds_the_blob_does_nothing(cluster):
    nodes, peers = cluster
    content = b"content that should be replicated"
    content_hash = hashlib.sha256(content).hexdigest()
    replicas = replica_set(content_hash, list(NODES), r=3)

    seed_everywhere(nodes, content, holders=set(replicas))

    assert run_catch_up(nodes[replicas[0]]) == 0


def test_the_budget_caps_one_round_and_later_rounds_finish_the_job(cluster):
    """A node healing from a long partition must not monopolise the thread."""
    nodes, peers = cluster
    missing = []
    for index in range(6):
        content = f"payload-{index}".encode()
        content_hash = hashlib.sha256(content).hexdigest()
        replicas = replica_set(content_hash, list(NODES), r=3)
        record = FileRecord.new(
            name=f"f{index}.txt", content_hash=content_hash, node_id="node1"
        )
        for node_id, node in nodes.items():
            node["store"].insert(record.copy())
            if node_id == replicas[0]:
                node["blob_store"].put(content)
        missing.append((replicas[1], content_hash))

    target_id = missing[0][0]
    target = nodes[target_id]
    owed = sum(1 for node_id, _ in missing if node_id == target_id)

    first = run_catch_up(target, budget=1)
    assert first == 1, "the budget must cap a single round"

    while run_catch_up(target, budget=1):
        pass
    held = sum(1 for node_id, h in missing if node_id == target_id
               and target["blob_store"].exists(h))
    assert held == owed, "later rounds must finish what the budget deferred"


def test_a_stop_signal_interrupts_the_round(cluster):
    nodes, peers = cluster
    content = b"content that should be replicated"
    content_hash = hashlib.sha256(content).hexdigest()
    replicas = replica_set(content_hash, list(NODES), r=3)
    seed_everywhere(nodes, content, holders={replicas[0]})
    behind = nodes[replicas[1]]

    fetched = catch_up_blobs(
        settings=behind["settings"],
        store=behind["store"],
        blob_store=behind["blob_store"],
        replication_client=behind["client"],
        budget=8,
        should_stop=lambda: True,
    )

    assert fetched == 0
    assert not behind["blob_store"].exists(content_hash)
