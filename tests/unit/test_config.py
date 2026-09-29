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


def test_rpc_timeout_defaults_and_is_configurable(monkeypatch):
    assert Settings(
        data_dir=Path("/tmp/b"), db_path=Path("/tmp/m.db")
    ).rpc_timeout_seconds == 5.0

    monkeypatch.setenv("ALMACEN_RPC_TIMEOUT_SECONDS", "1.5")
    assert Settings.from_env().rpc_timeout_seconds == 1.5


def test_rejects_a_non_positive_rpc_timeout():
    with pytest.raises(ValueError, match="rpc_timeout_seconds"):
        Settings(
            data_dir=Path("/tmp/b"), db_path=Path("/tmp/m.db"), rpc_timeout_seconds=0
        )


def test_gossip_settings_have_defaults_and_read_the_environment(monkeypatch):
    defaults = Settings(data_dir=Path("/tmp/b"), db_path=Path("/tmp/m.db"))
    assert defaults.gossip_interval_seconds == 5.0
    assert defaults.suspicion_timeout_seconds == 15.0
    assert defaults.gossip_fanout == 2
    assert defaults.blob_catchup_budget == 8
    assert defaults.gossip_enabled is True

    monkeypatch.setenv("ALMACEN_GOSSIP_INTERVAL_SECONDS", "2")
    monkeypatch.setenv("ALMACEN_SUSPICION_TIMEOUT_SECONDS", "6")
    monkeypatch.setenv("ALMACEN_GOSSIP_FANOUT", "3")
    monkeypatch.setenv("ALMACEN_BLOB_CATCHUP_BUDGET", "4")
    from_env = Settings.from_env()
    assert from_env.gossip_interval_seconds == 2.0
    assert from_env.suspicion_timeout_seconds == 6.0
    assert from_env.gossip_fanout == 3
    assert from_env.blob_catchup_budget == 4


def test_a_non_positive_gossip_interval_disables_the_loop():
    # Not a convenience: every multi-node fixture relies on it to avoid starting
    # real gossip threads inside the test suite.
    settings = Settings(
        data_dir=Path("/tmp/b"), db_path=Path("/tmp/m.db"), gossip_interval_seconds=0
    )
    assert settings.gossip_enabled is False


def test_rejects_non_positive_gossip_parameters():
    for field, value in (
        ("suspicion_timeout_seconds", 0),
        ("gossip_fanout", 0),
        ("blob_catchup_budget", 0),
    ):
        with pytest.raises(ValueError, match=field):
            Settings(
                data_dir=Path("/tmp/b"), db_path=Path("/tmp/m.db"), **{field: value}
            )
