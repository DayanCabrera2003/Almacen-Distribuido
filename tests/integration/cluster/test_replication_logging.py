"""Replication failures are logged as events naming the peer that failed."""
import hashlib
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from almacen.cluster.metadata_replicator import MetadataReplicator
from almacen.cluster.replication_client import QuorumNotReached, ReplicationClient
from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.storage.blob_store import BlobStore
from tests.helpers import free_port


@pytest.fixture
def isolated_settings(tmp_path: Path) -> Settings:
    """node1, with two peers on ports nothing is listening on."""
    return Settings(
        data_dir=tmp_path / "blobs",
        db_path=tmp_path / "metadata.db",
        node_id="node1",
        peers=(
            Peer("node1", f"127.0.0.1:{free_port()}"),
            Peer("node2", f"127.0.0.1:{free_port()}"),
            Peer("node3", f"127.0.0.1:{free_port()}"),
        ),
        replication_factor=3,
        write_quorum=2,
        rpc_timeout_seconds=1.0,
        gossip_interval_seconds=0,
    )


def _events(logs, name):
    return [entry for entry in logs if entry["event"] == name]


def test_a_failed_blob_write_logs_each_replica_and_the_missed_quorum(
    isolated_settings,
):
    client = ReplicationClient(isolated_settings, BlobStore(isolated_settings.data_dir))

    with capture_logs() as logs, pytest.raises(QuorumNotReached):
        client.put_blob(b"content")

    failed = _events(logs, "blob_replication_failed")
    assert {entry["replica"] for entry in failed} == {"node2", "node3"}
    assert all(entry["node_id"] == "node1" for entry in failed)
    assert all(entry["error"] == "UNAVAILABLE" for entry in failed)

    [quorum] = _events(logs, "write_quorum_not_reached")
    assert quorum["acknowledged"] == 1
    assert quorum["required"] == 2
    assert quorum["content_hash"] == hashlib.sha256(b"content").hexdigest()[:12]


def test_an_unreachable_replica_on_read_is_logged(isolated_settings):
    client = ReplicationClient(isolated_settings, BlobStore(isolated_settings.data_dir))

    with capture_logs() as logs:
        assert client.get_blob("a" * 64) is None

    misses = _events(logs, "blob_fetch_failed")
    assert {entry["replica"] for entry in misses} == {"node2", "node3"}


def test_a_failed_metadata_push_logs_the_peer(isolated_settings):
    replicator = MetadataReplicator(isolated_settings)
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")

    try:
        with capture_logs() as logs:
            replicator.replicate(record)
    finally:
        replicator.close()

    failed = _events(logs, "record_replication_failed")
    assert {entry["peer"] for entry in failed} == {"node2", "node3"}
    assert all(entry["file_id"] == str(record.file_id) for entry in failed)
    assert all(entry["node_id"] == "node1" for entry in failed)
