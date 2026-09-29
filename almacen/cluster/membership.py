# almacen/cluster/membership.py
"""SWIM-style failure detection (spec §4), as a pure state machine.

Every method takes `now` rather than reading a clock, so the whole protocol is
testable without sleeping. The background loop in `gossip.py` is what supplies
real time.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import IntEnum


class NodeState(IntEnum):
    """Ordered worst-last, so comparing `(incarnation, state)` prefers the more
    pessimistic report when two nodes disagree at the same incarnation."""

    ALIVE = 0
    SUSPECT = 1
    DEAD = 2


@dataclass(frozen=True)
class NodeStatus:
    state: NodeState
    # Bumped by a node about itself, to override stale bad news (see `merge`).
    incarnation: int
    since: datetime


_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


class Membership:
    def __init__(
        self, node_id: str, peers: tuple[str, ...], suspicion_timeout: timedelta
    ) -> None:
        self._node_id = node_id
        self._suspicion_timeout = suspicion_timeout
        self._incarnation = 0
        self._view: dict[str, NodeStatus] = {
            peer: NodeStatus(NodeState.ALIVE, 0, _EPOCH)
            for peer in peers
            if peer != node_id
        }
        # The gossip thread mutates this view while gRPC workers read and merge
        # into it. `snapshot` and `live_peers` iterate it while `merge` can
        # insert a previously unknown peer, which unlocked raises
        # "dictionary changed size during iteration" and kills the round.
        # Reentrant because `snapshot` and `live_peers` both call `state_of`.
        # Never held across a network or store call, so it cannot deadlock
        # against the metadata store's lock.
        self._lock = threading.RLock()

    # ---- direct observation ----

    def record_reachable(self, peer: str, now: datetime) -> None:
        """A probe succeeded: the peer is alive, whatever we believed."""
        with self._lock:
            current = self._view.get(peer)
            if current is None or current.state is not NodeState.ALIVE:
                # A fresh incarnation, so this good news outranks the bad news
                # that may still be circulating about this peer.
                incarnation = 0 if current is None else current.incarnation + 1
                self._view[peer] = NodeStatus(NodeState.ALIVE, incarnation, now)

    def record_unreachable(self, peer: str, now: datetime) -> None:
        """A probe failed: suspect the peer, but do not declare it dead.

        One failed probe is one dropped packet as often as it is a dead node.
        The timeout in `state_of` is what separates the two.
        """
        with self._lock:
            current = self._view.get(peer)
            if current is None:
                self._view[peer] = NodeStatus(NodeState.SUSPECT, 0, now)
            elif current.state is NodeState.ALIVE:
                # Only the first failure starts the clock; later ones must not
                # keep pushing the deadline back, or a peer failing every probe
                # would never be declared dead.
                self._view[peer] = NodeStatus(
                    NodeState.SUSPECT, current.incarnation, now
                )

    def state_of(self, peer: str, now: datetime) -> NodeState:
        """Current state, with suspicion ageing into death lazily."""
        if peer == self._node_id:
            return NodeState.ALIVE
        with self._lock:
            current = self._view.get(peer)
            if current is None:
                return NodeState.ALIVE
            if (
                current.state is NodeState.SUSPECT
                and now - current.since >= self._suspicion_timeout
            ):
                return NodeState.DEAD
            return current.state

    def live_peers(self, now: datetime) -> tuple[str, ...]:
        with self._lock:
            return tuple(
                peer
                for peer in sorted(self._view)
                if self.state_of(peer, now) is not NodeState.DEAD
            )

    # ---- gossiped state ----

    def snapshot(self, now: datetime) -> dict[str, NodeStatus]:
        """This node's view, as it travels on a gossip exchange."""
        with self._lock:
            view = {
                peer: NodeStatus(
                    self.state_of(peer, now), status.incarnation, status.since
                )
                for peer, status in self._view.items()
            }
            # A node always reports itself alive: it is the only authority on
            # that, and this is what makes refutation possible.
            view[self._node_id] = NodeStatus(NodeState.ALIVE, self._incarnation, now)
            return view

    def merge(self, remote_view: dict[str, NodeStatus], now: datetime) -> None:
        with self._lock:
            for peer, theirs in remote_view.items():
                if peer == self._node_id:
                    self._refute(theirs)
                    continue
                mine = self._view.get(peer)
                # Higher incarnation always wins, so a node can defend itself;
                # at equal incarnation the worse state wins, so a real failure
                # spreads instead of being argued away. Both halves are needed.
                if mine is None or (theirs.incarnation, theirs.state) > (
                    mine.incarnation,
                    mine.state,
                ):
                    # Re-stamp with *our* clock. `since` is only ever compared
                    # against a local `now` in `state_of`, so adopting a remote
                    # machine's timestamp makes the suspicion deadline depend on
                    # that machine's clock: a peer running ahead yields entries
                    # that never age into DEAD, one behind yields instant DEAD.
                    self._view[peer] = NodeStatus(
                        theirs.state, theirs.incarnation, now
                    )

    def _refute(self, claim: NodeStatus) -> None:
        """Someone thinks we are suspect or dead. Outrank them.

        Without this, one false suspicion propagates to the whole cluster and
        persists until every other node happens to probe us directly. Caller
        holds the lock.
        """
        if claim.state is not NodeState.ALIVE:
            self._incarnation = max(self._incarnation, claim.incarnation) + 1
