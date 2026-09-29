from pathlib import Path

import pytest

from almacen.config import Peer, Settings


def test_defaults_describe_a_single_node_cluster():
    settings = Settings(data_dir=Path("/tmp/blobs"), db_path=Path("/tmp/m.db"))
    assert settings.node_id == "node1"
    assert settings.node_ids == ["node1"]
    assert settings.replication_factor == 3
    assert settings.write_quorum == 2


def test_from_env_parses_the_peer_list(monkeypatch):
    monkeypatch.setenv("ALMACEN_NODE_ID", "node2")
    monkeypatch.setenv("ALMACEN_GRPC_PORT", "50052")
    monkeypatch.setenv(
        "ALMACEN_PEERS", "node1@node1:50051,node2@node2:50051,node3@node3:50051"
    )
    monkeypatch.setenv("ALMACEN_REPLICATION_FACTOR", "3")
    monkeypatch.setenv("ALMACEN_WRITE_QUORUM", "2")

    settings = Settings.from_env()

    assert settings.node_id == "node2"
    assert settings.grpc_port == 50052
    assert settings.node_ids == ["node1", "node2", "node3"]
    assert settings.peer_address("node3") == "node3:50051"


def test_from_env_tolerates_whitespace_in_the_peer_list(monkeypatch):
    monkeypatch.setenv("ALMACEN_PEERS", " node1@node1:50051 , node2@node2:50051 ")
    settings = Settings.from_env()
    assert settings.node_ids == ["node1", "node2"]


def test_remote_peers_excludes_self():
    peers = (Peer("node1", "node1:50051"), Peer("node2", "node2:50051"))
    settings = Settings(
        data_dir=Path("/tmp/b"), db_path=Path("/tmp/m.db"), node_id="node1", peers=peers
    )
    assert [p.node_id for p in settings.remote_peers] == ["node2"]


def test_rejects_write_quorum_larger_than_replication_factor():
    with pytest.raises(ValueError, match="write_quorum"):
        Settings(
            data_dir=Path("/tmp/b"),
            db_path=Path("/tmp/m.db"),
            replication_factor=2,
            write_quorum=3,
        )


def test_rejects_node_id_missing_from_the_peer_list():
    with pytest.raises(ValueError, match="node_id"):
        Settings(
            data_dir=Path("/tmp/b"),
            db_path=Path("/tmp/m.db"),
            node_id="ghost",
            peers=(Peer("node1", "node1:50051"),),
        )


def test_rejects_a_malformed_peer_entry(monkeypatch):
    monkeypatch.setenv("ALMACEN_PEERS", "node1-no-at-sign")
    with pytest.raises(ValueError, match="peer"):
        Settings.from_env()
