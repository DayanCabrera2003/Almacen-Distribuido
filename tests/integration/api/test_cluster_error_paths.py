from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.main import create_app


@pytest.fixture
def client_with_dead_peers(tmp_path: Path) -> TestClient:
    """A node whose two peers do not exist, so W=2 is unreachable."""
    settings = Settings(
        data_dir=tmp_path / "blobs",
        db_path=tmp_path / "metadata.db",
        node_id="node1",
        peers=(
            Peer("node1", "127.0.0.1:59991"),
            Peer("node2", "127.0.0.1:59992"),  # nothing listening
            Peer("node3", "127.0.0.1:59993"),  # nothing listening
        ),
        replication_factor=3,
        write_quorum=2,
    )
    # No lifespan: this node's own gRPC server is irrelevant here, and not
    # binding it keeps the test independent of port availability.
    return TestClient(create_app(settings))


def test_upload_returns_503_when_the_write_quorum_cannot_be_met(client_with_dead_peers):
    response = client_with_dead_peers.post(
        "/files",
        files={"file": ("a.txt", b"content", "text/plain")},
        data={"name": "a.txt"},
    )
    assert response.status_code == 503


def test_no_metadata_is_recorded_when_the_write_quorum_fails(client_with_dead_peers):
    client_with_dead_peers.post(
        "/files",
        files={"file": ("a.txt", b"content", "text/plain")},
        data={"name": "a.txt"},
    )
    # A record pointing at content that is not durable would be a dangling
    # reference — the upload must leave no trace.
    assert client_with_dead_peers.get("/files").json() == []


def test_download_returns_503_when_no_replica_holds_the_blob(tmp_path: Path):
    settings = Settings(data_dir=tmp_path / "blobs", db_path=tmp_path / "metadata.db")
    app = create_app(settings)
    client = TestClient(app)

    # A record whose content was never stored anywhere: the file is known, the
    # bytes are unreachable.
    record = FileRecord.new(name="ghost.txt", content_hash="c" * 64)
    app.state.local_metadata_store.insert(record)

    response = client.get(f"/files/{record.file_id}")

    assert response.status_code == 503, "the file exists, so 404 would be a lie"
