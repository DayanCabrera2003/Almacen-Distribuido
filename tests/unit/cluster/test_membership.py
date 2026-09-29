"""Failure detection, with time as a parameter so nothing here sleeps."""
import threading
import time
from datetime import datetime, timedelta, timezone

from almacen.cluster.membership import Membership, NodeState, NodeStatus

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
PEERS = ("node1", "node2", "node3")
TIMEOUT = timedelta(seconds=5)


def make(node_id: str = "node1") -> Membership:
    return Membership(node_id=node_id, peers=PEERS, suspicion_timeout=TIMEOUT)


def test_every_peer_starts_alive():
    membership = make()
    assert all(membership.state_of(p, T0) is NodeState.ALIVE for p in PEERS)


def test_one_failed_probe_suspects_rather_than_kills():
    # Declaring death on a single missed probe turns one dropped packet into a
    # cluster-wide membership change. Suspicion is the whole point.
    membership = make()
    membership.record_unreachable("node2", T0)
    assert membership.state_of("node2", T0) is NodeState.SUSPECT


def test_suspicion_becomes_death_only_after_the_timeout():
    membership = make()
    membership.record_unreachable("node2", T0)

    assert (
        membership.state_of("node2", T0 + TIMEOUT - timedelta(seconds=1))
        is NodeState.SUSPECT
    )
    assert membership.state_of("node2", T0 + TIMEOUT) is NodeState.DEAD


def test_a_peer_that_answers_again_is_alive_again():
    membership = make()
    membership.record_unreachable("node2", T0)
    membership.record_reachable("node2", T0 + timedelta(seconds=10))
    assert membership.state_of("node2", T0 + timedelta(seconds=10)) is NodeState.ALIVE


def test_a_peer_can_come_back_from_dead():
    # Otherwise the failure detector is a one-way door and a node that was
    # briefly unreachable is excluded forever.
    membership = make()
    membership.record_unreachable("node2", T0)
    assert membership.state_of("node2", T0 + TIMEOUT) is NodeState.DEAD

    membership.record_reachable("node2", T0 + TIMEOUT)
    assert membership.state_of("node2", T0 + TIMEOUT) is NodeState.ALIVE


def test_repeated_failures_do_not_restart_the_suspicion_clock():
    membership = make()
    membership.record_unreachable("node2", T0)
    membership.record_unreachable("node2", T0 + timedelta(seconds=3))
    assert membership.state_of("node2", T0 + TIMEOUT) is NodeState.DEAD


def test_recovery_bumps_the_incarnation_so_the_news_wins():
    membership = make()
    before = membership.snapshot(T0)["node2"].incarnation
    membership.record_unreachable("node2", T0)
    membership.record_reachable("node2", T0)
    assert membership.snapshot(T0)["node2"].incarnation > before


def test_a_node_refutes_a_suspicion_about_itself():
    # One dropped probe must not get a healthy node evicted cluster-wide.
    accuser = make("node1")
    accused = make("node2")
    accuser.record_unreachable("node2", T0)

    accused.merge(accuser.snapshot(T0), T0)
    accuser.merge(accused.snapshot(T0), T0)

    assert accuser.state_of("node2", T0) is NodeState.ALIVE


def test_a_node_never_reports_itself_as_anything_but_alive():
    membership = make("node1")
    assert membership.snapshot(T0)["node1"].state is NodeState.ALIVE


def test_a_higher_incarnation_wins_on_merge():
    local = make("node1")
    remote = make("node3")
    remote.record_unreachable("node2", T0)
    remote.record_reachable("node2", T0)  # incarnation 1, ALIVE

    local.record_unreachable("node2", T0)  # incarnation 0, SUSPECT
    local.merge(remote.snapshot(T0), T0)

    assert local.state_of("node2", T0) is NodeState.ALIVE


def test_at_equal_incarnation_the_worse_state_wins():
    # Suspicion spreads, so a real failure is noticed cluster-wide rather than
    # only by whoever probed first.
    local = make("node1")
    remote = make("node3")
    remote.record_unreachable("node2", T0)

    local.merge(remote.snapshot(T0), T0)

    assert local.state_of("node2", T0) is NodeState.SUSPECT


def test_merge_learns_about_a_peer_it_had_never_heard_of():
    local = Membership("node1", ("node1", "node2"), TIMEOUT)
    remote = Membership("node3", ("node1", "node2", "node3"), TIMEOUT)

    local.merge(remote.snapshot(T0), T0)

    assert local.state_of("node3", T0) is NodeState.ALIVE


def test_an_adopted_status_is_stamped_with_the_local_clock():
    """`since` is compared against a local `now`, so it must be local.

    Adopting a remote machine's timestamp makes the suspicion deadline depend on
    that machine's clock: a peer running ahead yields entries that never age
    into DEAD, one running behind yields instant DEAD.
    """
    local = make("node1")
    far_future = T0 + timedelta(days=365)
    local.merge({"node2": NodeStatus(NodeState.SUSPECT, 3, far_future)}, T0)

    assert local.state_of("node2", T0) is NodeState.SUSPECT
    assert local.state_of("node2", T0 + TIMEOUT) is NodeState.DEAD


def test_live_peers_excludes_the_dead_and_this_node():
    membership = make("node1")
    membership.record_unreachable("node3", T0)
    later = T0 + TIMEOUT

    assert membership.live_peers(later) == ("node2",)


def test_a_snapshot_is_not_corrupted_by_a_concurrent_merge():
    """The gossip thread merges while a gRPC worker builds a snapshot.

    `snapshot` and `live_peers` iterate the view; `merge` can insert a peer the
    node has never heard of. Unlocked, the reader raises
    `RuntimeError: dictionary changed size during iteration` — hundreds of times
    per second — and the gossip round dies with it.
    """
    membership = make("node1")
    failures: list[str] = []
    stop = threading.Event()

    def merging() -> None:
        index = 0
        while not stop.is_set():
            membership.merge(
                {f"newcomer{index}": NodeStatus(NodeState.ALIVE, 1, T0)}, T0
            )
            index += 1
            if index % 50 == 0:
                time.sleep(0)  # yield, or the GIL starves the reader

    def reading() -> None:
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            try:
                membership.snapshot(T0)
                membership.live_peers(T0)
            except RuntimeError as error:
                failures.append(str(error))

    writer = threading.Thread(target=merging)
    reader = threading.Thread(target=reading)
    writer.start()
    reader.start()
    reader.join(timeout=10)
    stop.set()
    writer.join(timeout=5)

    assert failures == [], f"snapshot raced against merge: {failures[0]}"
