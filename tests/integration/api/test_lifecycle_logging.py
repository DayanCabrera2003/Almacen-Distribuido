from pathlib import Path

from fastapi.testclient import TestClient
from structlog.testing import capture_logs

from almacen.config import Settings
from almacen.main import create_app
from tests.helpers import free_port


def test_startup_and_shutdown_are_logged_as_events(tmp_path: Path):
    port = free_port()
    settings = Settings(
        data_dir=tmp_path / "blobs",
        db_path=tmp_path / "metadata.db",
        node_id="node1",
        grpc_port=port,
        gossip_interval_seconds=0,
    )

    with capture_logs() as logs:
        with TestClient(create_app(settings)):
            pass

    events = {entry["event"]: entry for entry in logs}
    assert events["node_started"]["node_id"] == "node1"
    assert events["node_started"]["grpc_port"] == port
    assert events["node_started"]["gossip_enabled"] is False
    assert events["node_stopped"]["node_id"] == "node1"
