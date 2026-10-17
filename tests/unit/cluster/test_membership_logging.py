"""Membership logs each state transition once, as it is first observed."""
from datetime import datetime, timedelta, timezone

from structlog.testing import capture_logs

from almacen.cluster.membership import Membership, NodeState, NodeStatus

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
PEERS = ("node1", "node2", "node3")
TIMEOUT = timedelta(seconds=5)


def make() -> Membership:
    return Membership(node_id="node1", peers=PEERS, suspicion_timeout=TIMEOUT)


def transitions(logs):
    return [
        (entry["peer"], entry["from_state"], entry["to_state"], entry["reason"])
        for entry in logs
        if entry["event"] == "member_state_changed"
    ]


def test_a_failed_probe_logs_alive_to_suspect_once():
    membership = make()

    with capture_logs() as logs:
        membership.record_unreachable("node2", T0)
        membership.record_unreachable("node2", T0 + timedelta(seconds=1))

    assert transitions(logs) == [("node2", "alive", "suspect", "probe_failed")]
    assert logs[0]["node_id"] == "node1"
    assert logs[0]["incarnation"] == 0


def test_a_successful_probe_logs_recovery_with_the_new_incarnation():
    membership = make()
    membership.record_unreachable("node2", T0)

    with capture_logs() as logs:
        membership.record_reachable("node2", T0)
        membership.record_reachable("node2", T0)

    assert transitions(logs) == [("node2", "suspect", "alive", "probe_succeeded")]
    assert logs[0]["incarnation"] == 1


def test_death_by_timeout_is_logged_once_however_often_it_is_observed():
    # The SUSPECT -> DEAD transition is lazy: nothing happens at the deadline,
    # a later read just sees it has passed. It must still be logged, but not on
    # every one of the many reads that follow.
    membership = make()
    membership.record_unreachable("node2", T0)

    with capture_logs() as logs:
        for _ in range(3):
            membership.state_of("node2", T0 + TIMEOUT)
        membership.live_peers(T0 + TIMEOUT)
        membership.snapshot(T0 + TIMEOUT)

    assert transitions(logs) == [("node2", "suspect", "dead", "suspicion_timeout")]


def test_a_dead_peer_that_recovers_and_fails_again_is_declared_dead_again():
    membership = make()
    membership.record_unreachable("node2", T0)
    membership.state_of("node2", T0 + TIMEOUT)
    later = T0 + 2 * TIMEOUT

    with capture_logs() as logs:
        membership.record_reachable("node2", later)
        membership.record_unreachable("node2", later)
        membership.state_of("node2", later + TIMEOUT)

    assert transitions(logs) == [
        ("node2", "dead", "alive", "probe_succeeded"),
        ("node2", "alive", "suspect", "probe_failed"),
        ("node2", "suspect", "dead", "suspicion_timeout"),
    ]


def test_news_adopted_from_gossip_is_logged():
    membership = make()

    with capture_logs() as logs:
        membership.merge({"node3": NodeStatus(NodeState.SUSPECT, 0, T0)}, T0)
        # Same news again: nothing changes, nothing is logged.
        membership.merge({"node3": NodeStatus(NodeState.SUSPECT, 0, T0)}, T0)

    assert transitions(logs) == [("node3", "alive", "suspect", "gossip")]


def test_a_refutation_is_logged_with_the_new_incarnation():
    membership = make()

    with capture_logs() as logs:
        membership.merge({"node1": NodeStatus(NodeState.SUSPECT, 0, T0)}, T0)

    [refuted] = [e for e in logs if e["event"] == "member_refuted_suspicion"]
    assert refuted["node_id"] == "node1"
    assert refuted["claimed_state"] == "suspect"
    assert refuted["incarnation"] == 1
