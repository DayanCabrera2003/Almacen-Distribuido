"""Gossip rounds leave a structured trace of what they reconciled."""
from structlog.testing import capture_logs

from almacen.domain.file_record import FileRecord
from tests.integration.cluster.test_gossip import pair, run_round  # noqa: F401


def _events(logs, name):
    return [entry for entry in logs if entry["event"] == name]


def test_a_round_logs_what_it_pulled_and_pushed(pair):  # noqa: F811
    nodes, peers, channels = pair
    nodes["node1"]["store"].insert(
        FileRecord.new(name="mine.txt", content_hash="h1", node_id="node1")
    )
    nodes["node2"]["store"].insert(
        FileRecord.new(name="theirs.txt", content_hash="h2", node_id="node2")
    )

    with capture_logs() as logs:
        run_round(nodes, peers, channels, "node1")

    [completed] = _events(logs, "gossip_round_completed")
    assert completed["node_id"] == "node1"
    assert completed["peer"] == "node2"
    assert completed["pulled"] == 1
    assert completed["pushed"] == 1


def test_an_unreachable_peer_logs_a_failed_round(pair):  # noqa: F811
    nodes, peers, channels = pair
    nodes["node2"]["server"].stop(0).wait()

    with capture_logs() as logs:
        run_round(nodes, peers, channels, "node1")

    [failed] = _events(logs, "gossip_round_failed")
    assert failed["node_id"] == "node1"
    assert failed["peer"] == "node2"
    assert failed["stage"] == "exchange"
    assert failed["error"] == "UNAVAILABLE"
    assert failed["log_level"] == "info", "an unreachable peer is routine"


def test_a_failure_after_the_exchange_is_a_warning(pair, monkeypatch):  # noqa: F811
    from almacen.cluster import gossip as gossip_module

    nodes, peers, channels = pair
    nodes["node2"]["store"].insert(
        FileRecord.new(name="a.txt", content_hash="h", node_id="node2")
    )

    def explode(_message):
        raise ValueError("malformed record from a buggy peer")

    monkeypatch.setattr(gossip_module, "message_to_record", explode)

    with capture_logs() as logs:
        run_round(nodes, peers, channels, "node1")

    [failed] = _events(logs, "gossip_round_failed")
    assert failed["stage"] == "reconcile"
    assert failed["log_level"] == "warning"
    assert "malformed record" in failed["error"]
