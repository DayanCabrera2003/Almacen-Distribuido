# Phase 4 — Gossip, Partitions and Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the cluster survive nodes going away and coming back. Nodes discover each other's health by gossip instead of assuming a static list is accurate, and a node that missed updates while partitioned catches up automatically once it can talk again — metadata *and* content.

**Architecture:** Two new pure modules in `almacen/cluster/` — `membership.py` (a SWIM-style state machine) and `reconciliation.py` (the digest/diff logic) — plus a thin background driver that calls them on a timer. Both modules take the current time and the peer to contact as *parameters*, so every behaviour in this phase is testable without sleeping or waiting for a real clock. The gossip exchange rides on one new gRPC method; the records it pulls are merged by the Phase 3 CRDT merge, unchanged.

**Tech Stack:** Unchanged. `threading` for the background loop.

**Spec:** §4 (simplified SWIM, heartbeats, suspicion, propagation via gossip), §11 (push-pull anti-entropy, `file_id → vector_clock` digests, blob catch-up), §16 (this is phase 4 of 6).

---

## What this phase fixes

Phase 3 made the *merge* correct. Delivery is still Phase 2's: one direct push to every peer, best-effort, with nothing to repair a push that failed. A node that is down or unreachable for one write misses it permanently, and the cluster stays silently divergent. Phase 3's convergence guarantee is therefore vacuous in exactly the situation it was built for — a partition.

This phase closes that: the CRDTs keep merging correctly, and gossip makes sure every node eventually *sees* everything to merge.

**Out of scope, by phase:**

| Deferred to | What |
|---|---|
| Phase 5 | Tombstone TTL and purge, blob reference counting and GC, OR-Set remove-set pruning. Gossip here spreads tombstones; it never deletes anything. |
| Phase 6 | `GET /cluster/status`, structlog, the Typer CLI, chaos test suite. The membership state is exposed in-process for tests but gets no HTTP endpoint yet. |
| Not planned | Merkle-tree digests (spec §11 explicitly defers them), indirect probing (`ping-req`), and a full SWIM dissemination component with piggyback limits. See Task 4's note on what "simplified" leaves out. |

---

## Design decisions made while planning

The spec names the mechanisms; these mechanics are open, and two of them were settled by measurement rather than judgement.

1. **Time is a parameter, never a `sleep`.** `Membership` and the gossip scheduler take `now` explicitly; the background thread is a dumb driver that supplies it. Every test in this phase calls the logic directly with a chosen instant. Nothing in the suite waits for a real clock — which is the difference between a failure detector that is tested and one that is merely observed not to have broken yet.

2. **Peer selection is injected, random only in production.** Measured on a prototype of the exchange: with peers chosen at random, three nodes converge in a median of 4 rounds but take up to 12, and five nodes take a median of 12 and up to **23**. A test that runs a fixed number of random rounds would therefore be flaky at any round count small enough to be fast. Tests inject a deterministic round-robin chooser instead; production uses `random.sample`.

3. **Push-pull uses one new RPC plus the existing `ReplicateRecord`.** The initiator sends its digest; the responder replies with the records it should push *and* the `file_id`s it wants back. The initiator merges the pushed records and sends the wanted ones with `ReplicateRecord`, which already exists and already merges. One round-trip per pair, no new transfer path, and the pull direction reuses code that Phase 3 tests cover.

4. **The digest is `file_id → vector_clock`, per spec §11.** Comparison per entry: if their clock dominates mine, I want their record; if mine dominates, I push mine; if the clocks are *concurrent*, both happen — each side sends its version and the CRDT merge reconciles them. Equal clocks mean nothing to do, which is the common case and what keeps a steady-state round cheap.

5. **Membership piggybacks on the gossip RPC rather than getting its own.** A node's view of everyone's health travels with every digest exchange it already makes. A separate dissemination channel would double the message count to spread information that is a few dozen bytes.

6. **Suspicion is refutable, via incarnation numbers.** A node that hears itself suspected bumps its own incarnation and re-announces as alive; higher incarnation wins. Without refutation a single false suspicion — one dropped packet — would propagate to the whole cluster and stick until that node happened to be probed directly by every other. Verified on the prototype: node1 wrongly suspects node2, node2 refutes, node1's view returns to `ALIVE`.

7. **Blob catch-up is driven by placement, not by "fetch everything I lack".** After merging records, a node fetches a blob only if HRW says it is a replica for that content and it does not hold it. Fetching everything would make each node store the whole corpus and quietly undo Phase 2's sharding.

8. **A dead node is still gossiped to, occasionally.** Peer selection prefers live peers but is not restricted to them, or a node wrongly marked dead could never be discovered alive again — the failure detector would be a one-way door.

---

## File Structure

```
almacen/
  cluster/
    membership.py              # NEW: SWIM-style state machine, time injected
    reconciliation.py          # NEW: digest, diff, and the merge/fetch steps
    gossip.py                  # NEW: one gossip round against one peer + the driver loop
  rpc/
    cluster.proto              # MODIFY: Gossip RPC + membership piggyback
    cluster_servicer.py        # MODIFY: serve Gossip
  config.py                    # MODIFY: gossip interval, suspicion timeout, fanout
  main.py                      # MODIFY: start/stop the gossip loop in the lifespan
tests/
  unit/cluster/test_membership.py        # NEW
  unit/cluster/test_reconciliation.py    # NEW
  integration/cluster/test_gossip.py     # NEW: one round over real gRPC
  integration/cluster/test_partition_healing.py  # NEW: the headline tests
docker/
  docker-compose.yml           # MODIFY: shorter timers for a live demo
DAA/failure-detection-and-suspicion.md   # NEW (untracked)
DAA/anti-entropy-digests.md              # NEW (untracked)
```

`membership.py` and `reconciliation.py` contain no I/O and no clock reads — that is what makes the two hardest things in this phase (a failure detector and a convergence protocol) testable as pure functions.

---

### Task 1: `Membership` — SWIM-style state machine

**Files:** Create `almacen/cluster/membership.py`; test `tests/unit/cluster/test_membership.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/cluster/test_membership.py
"""Failure detection, with time as a parameter so nothing here sleeps."""
from datetime import datetime, timedelta, timezone

import pytest

from almacen.cluster.membership import Membership, NodeState

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

    assert membership.state_of("node2", T0 + TIMEOUT - timedelta(seconds=1)) is (
        NodeState.SUSPECT
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


def test_live_peers_excludes_the_dead_and_this_node():
    membership = make("node1")
    membership.record_unreachable("node3", T0)
    later = T0 + TIMEOUT

    assert membership.live_peers(later) == ("node2",)
```

- [ ] **Step 2: Run to verify they fail** — `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

```python
# almacen/cluster/membership.py
"""SWIM-style failure detection (spec §4), as a pure state machine.

Every method takes `now` rather than reading a clock, so the whole protocol is
testable without sleeping. The background loop in `gossip.py` is what supplies
real time.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import IntEnum

# Ordered worst-last, so `max` on (incarnation, state) prefers the more
# pessimistic report at equal incarnation.
class NodeState(IntEnum):
    ALIVE = 0
    SUSPECT = 1
    DEAD = 2


@dataclass(frozen=True)
class NodeStatus:
    state: NodeState
    # Bumped by a node about itself to override stale bad news (see `merge`).
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

    # ---- direct observation ----

    def record_reachable(self, peer: str, now: datetime) -> None:
        """A probe succeeded: the peer is alive, whatever we believed."""
        current = self._view.get(peer)
        if current is None or current.state is not NodeState.ALIVE:
            # A fresh incarnation, so this good news outranks the bad news that
            # may still be circulating about this peer.
            incarnation = 0 if current is None else current.incarnation + 1
            self._view[peer] = NodeStatus(NodeState.ALIVE, incarnation, now)

    def record_unreachable(self, peer: str, now: datetime) -> None:
        """A probe failed: suspect the peer, but do not declare it dead.

        One failed probe is one dropped packet as often as it is a dead node.
        The timeout in `state_of` is what separates the two.
        """
        current = self._view.get(peer)
        if current is None:
            self._view[peer] = NodeStatus(NodeState.SUSPECT, 0, now)
        elif current.state is NodeState.ALIVE:
            # Only the first failure starts the clock; later ones must not keep
            # pushing the deadline back, or a peer failing every probe would
            # never be declared dead.
            self._view[peer] = NodeStatus(NodeState.SUSPECT, current.incarnation, now)

    def state_of(self, peer: str, now: datetime) -> NodeState:
        """Current state, with suspicion ageing into death lazily."""
        if peer == self._node_id:
            return NodeState.ALIVE
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
        return tuple(
            peer
            for peer in sorted(self._view)
            if self.state_of(peer, now) is not NodeState.DEAD
        )

    # ---- gossiped state ----

    def snapshot(self, now: datetime) -> dict[str, NodeStatus]:
        """This node's view, as it travels on a gossip exchange."""
        view = {
            peer: NodeStatus(self.state_of(peer, now), status.incarnation, status.since)
            for peer, status in self._view.items()
        }
        # A node always reports itself alive: it is the only authority on that.
        view[self._node_id] = NodeStatus(NodeState.ALIVE, self._incarnation, now)
        return view

    def merge(self, remote_view: dict[str, NodeStatus], now: datetime) -> None:
        for peer, theirs in remote_view.items():
            if peer == self._node_id:
                self._refute(theirs)
                continue
            mine = self._view.get(peer)
            # Higher incarnation always wins; at equal incarnation the worse
            # state wins, so a real failure spreads instead of being argued away.
            if mine is None or (theirs.incarnation, theirs.state) > (
                mine.incarnation,
                mine.state,
            ):
                self._view[peer] = theirs

    def _refute(self, claim: NodeStatus) -> None:
        """Someone thinks we are suspect or dead. Outrank them.

        Without this, one false suspicion propagates to the whole cluster and
        persists until every other node happens to probe us directly.
        """
        if claim.state is not NodeState.ALIVE:
            self._incarnation = max(self._incarnation, claim.incarnation) + 1
```

- [ ] **Step 4: Run tests** — expect 13 passed.
- [ ] **Step 5: Commit** — `git commit -m "Add SWIM-style membership with refutable suspicion"`

---

### Task 2: DAA note — failure detection

**Files:** Create `../DAA/failure-detection-and-suspicion.md`

- [ ] **Step 1: Write the note.** Cover:

- **Problem:** decide whether an unreachable node is dead or merely slow, with no way to tell the two apart from the outside. This is the classic impossibility at the heart of asynchronous distributed systems: no timeout can be provably correct, so the design must choose which error it prefers.
- **Why not "one failed probe = dead":** turns every dropped packet into a cluster-wide membership change, and membership changes move data (HRW placement shifts). The cost of a false positive is re-replication.
- **Why not "wait much longer":** a genuinely dead node keeps receiving writes that go nowhere until it is noticed.
- **Suspicion as the middle state:** the failure detector becomes *eventually strong* rather than accurate — it may be wrong transiently, but a live node always eventually refutes. Quantify: a node is declared dead at `suspicion_timeout` after the first failed probe, and death is revocable.
- **Incarnation numbers and refutation**, with the prototype result: without them, a single false suspicion sticks until every node probes the victim directly. With them it survives exactly one gossip round-trip.
- **The merge rule's asymmetry** — higher incarnation wins, and at *equal* incarnation the worse state wins — and why both halves are needed: the first lets a node defend itself, the second lets bad news spread. Reversing either breaks one of the two.
- **Complexity:** O(N) state per node, O(N) per merge, O(1) probes per round. Contrast with all-to-all heartbeating, which is O(N²) messages per round — the reason SWIM probes a random subset.
- **What "simplified" leaves out, and the cost of each:** indirect probing (`ping-req` through k intermediaries, which distinguishes "the target is dead" from "my link to it is dead" and is the single biggest accuracy win in real SWIM), piggyback limits on dissemination, and a round-robin probe order with a shuffled member list. Note that without indirect probing, a one-sided network fault makes two live nodes each suspect the other.

---

### Task 3: `Reconciliation` — digests and diffing

**Files:** Create `almacen/cluster/reconciliation.py`; test `tests/unit/cluster/test_reconciliation.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/cluster/test_reconciliation.py
"""The anti-entropy diff: what to send, what to ask for, and nothing else."""
from almacen.cluster.reconciliation import build_digest, plan_exchange
from almacen.domain.file_record import FileRecord


def record(name: str = "a.txt", node_id: str = "node1") -> FileRecord:
    return FileRecord.new(name=name, content_hash="h", node_id=node_id)


def test_a_digest_maps_each_file_to_its_vector_clock():
    one, two = record("a.txt"), record("b.txt")

    digest = build_digest([one, two])

    assert digest == {
        one.file_id: one.vector_clock,
        two.file_id: two.vector_clock,
    }


def test_identical_state_exchanges_nothing():
    local = record()
    plan = plan_exchange([local], build_digest([local]))
    assert plan.push == [] and plan.wanted == []


def test_a_record_the_peer_lacks_is_pushed():
    local = record()
    plan = plan_exchange([local], {})
    assert [r.file_id for r in plan.push] == [local.file_id]
    assert plan.wanted == []


def test_a_record_only_the_peer_has_is_requested():
    theirs = record()
    plan = plan_exchange([], {theirs.file_id: theirs.vector_clock})
    assert plan.push == []
    assert plan.wanted == [theirs.file_id]


def test_a_newer_local_record_is_pushed():
    local = record()
    stale_clock = local.vector_clock.merged(local.vector_clock)
    local.rename("newer.txt", "node1")

    plan = plan_exchange([local], {local.file_id: stale_clock})

    assert [r.file_id for r in plan.push] == [local.file_id]
    assert plan.wanted == []


def test_a_newer_remote_record_is_requested():
    local = record()
    ahead = local.copy()
    ahead.rename("newer.txt", "node1")

    plan = plan_exchange([local], {local.file_id: ahead.vector_clock})

    assert plan.push == []
    assert plan.wanted == [local.file_id]


def test_concurrent_versions_are_exchanged_in_both_directions():
    # Neither side has seen the other's edit, so neither can be discarded:
    # both travel and the CRDT merge reconciles them.
    local = record()
    elsewhere = local.copy()
    local.rename("from-here.txt", "node1")
    elsewhere.rename("from-there.txt", "node2")

    plan = plan_exchange([local], {local.file_id: elsewhere.vector_clock})

    assert [r.file_id for r in plan.push] == [local.file_id]
    assert plan.wanted == [local.file_id]


def test_tombstoned_records_are_still_exchanged():
    # A tombstone is the update that most needs to propagate; skipping deleted
    # records would let a partitioned node keep serving a deleted file forever.
    local = record()
    local.mark_deleted("node1")
    plan = plan_exchange([local], {})
    assert [r.file_id for r in plan.push] == [local.file_id]


def test_the_plan_is_empty_between_two_empty_nodes():
    plan = plan_exchange([], {})
    assert plan.push == [] and plan.wanted == []
```

- [ ] **Step 2: Run to verify they fail.**

- [ ] **Step 3: Implement**

```python
# almacen/cluster/reconciliation.py
"""Push-pull anti-entropy: deciding what two nodes owe each other (spec §11)."""
from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field

from almacen.crdt.vector_clock import VectorClock
from almacen.domain.file_record import FileRecord

Digest = dict[uuid.UUID, VectorClock]


@dataclass
class ExchangePlan:
    """One side's half of an exchange."""

    push: list[FileRecord] = field(default_factory=list)
    wanted: list[uuid.UUID] = field(default_factory=list)


def build_digest(records: Iterable[FileRecord]) -> Digest:
    """What a node advertises: one vector clock per file, nothing else.

    A clock is a few integers, so a digest stays small even when the records
    themselves are not — which is the entire point of exchanging digests first
    rather than shipping state and letting the receiver sort it out.
    """
    return {record.file_id: record.vector_clock for record in records}


def plan_exchange(local_records: Iterable[FileRecord], remote: Digest) -> ExchangePlan:
    """Compare local state against a peer's digest.

    Tombstoned records are included deliberately: a tombstone is the update that
    most needs to travel, since a peer that never receives it keeps serving a
    file everyone else considers deleted.
    """
    plan = ExchangePlan()

    for record in local_records:
        their_clock = remote.get(record.file_id)
        if their_clock is None:
            plan.push.append(record)  # they have never seen this file
            continue
        if record.vector_clock == their_clock:
            continue  # identical histories; the common case in steady state
        if record.vector_clock.dominates(their_clock):
            plan.push.append(record)
        elif their_clock.dominates(record.vector_clock):
            plan.wanted.append(record.file_id)
        else:
            # Concurrent: neither version supersedes the other, so both must
            # travel and the CRDT merge decides.
            plan.push.append(record)
            plan.wanted.append(record.file_id)

    known = {record.file_id for record in local_records}
    plan.wanted.extend(file_id for file_id in remote if file_id not in known)
    return plan
```

Note `local_records` is consumed twice, so callers must pass a sequence, not a
generator; the signature says `Iterable` for convenience but a list is expected.
Materialize it at the top if that ever becomes a footgun.

- [ ] **Step 4: Run tests** — expect 9 passed.
- [ ] **Step 5: Commit** — `git commit -m "Add push-pull anti-entropy digest comparison"`

---

### Task 4: DAA note — anti-entropy digests

**Files:** Create `../DAA/anti-entropy-digests.md`

- [ ] **Step 1: Write the note.** Cover:

- **Problem:** two replicas hold overlapping sets of records and must find the differences while transferring as little as possible. Naively shipping every record every round costs `O(total state)` per round per pair, which is the thing anti-entropy exists to avoid.
- **Why a `file_id → vector_clock` digest works:** the clock is a complete summary of a record's causal history, so comparing clocks answers "who is ahead" without touching the records. `O(number of files)` small entries instead of `O(total bytes)`.
- **The four-way comparison** — missing / mine-dominates / theirs-dominates / concurrent — and why concurrent must send in *both* directions. Note that this is the only case that costs two transfers, and it is exactly the case a simple "newest wins" digest would get wrong.
- **Measured behaviour**, from the prototype: with peers chosen at random, 3 nodes converge in a median of 4 rounds (max 12) and 5 nodes in a median of 12 (max 23), over 200 trials each, 200/200 converging. This is the basis for decision 2 above — it is also why the tests use a deterministic schedule.
- **Complexity:** digest build `O(F)` for `F` local files; comparison `O(F + F')`; transfer proportional to the number of *differing* records, not to total state. Steady state (no differences) costs one `O(F)` message each way and zero record transfers.
- **The scaling limit, stated honestly:** the digest is `O(F)` per round regardless of how little has changed, so at large `F` a steady-state cluster spends bandwidth proving nothing changed. This is what Merkle-tree digests fix — hash the record set into a tree and compare `O(log F)` hashes, descending only into subtrees that differ. Spec §11 explicitly defers it; at demo scale `F` is small enough that the tree's maintenance cost would exceed what it saves.
- **Why tombstoned records stay in the digest** and what that implies for Phase 5: a purged tombstone is indistinguishable from a file never seen, so a node that purges too early can be re-taught the file by a peer that has not. This is precisely why Phase 5's purge needs a grace TTL longer than the worst-case partition, and it is worth recording here because the constraint originates in *this* phase's design.

---

### Task 5: Gossip over gRPC

**Files:** Modify `almacen/rpc/cluster.proto`, `almacen/rpc/cluster_servicer.py`, `almacen/storage/metadata_store.py`; test `tests/integration/rpc/test_gossip_servicer.py`

- [ ] **Step 1: Add `MetadataStore.list_all`**

Anti-entropy needs every record, including tombstoned ones — `list_live` would
hide exactly the updates that most need to propagate. Add alongside `list_live`:

```python
    def list_all(self) -> list[FileRecord]:
        """Every record, tombstoned or not.

        Anti-entropy uses this rather than `list_live`: a tombstone is an update
        like any other, and a peer that never receives it goes on serving a file
        the rest of the cluster considers deleted.
        """
        with self._lock:
            rows = self._conn.execute(f"SELECT {_COLUMNS} FROM files").fetchall()
            return [self._row_to_record(row) for row in rows]
```

Test it: a store with one live and one tombstoned record returns both from
`list_all` and one from `list_live`.

- [ ] **Step 2: Extend the proto**

```protobuf
service Cluster {
  rpc ReplicateRecord(FileRecordMsg) returns (ReplicateAck);
  rpc Ping(PingRequest) returns (PingResponse);
  // Push-pull anti-entropy: the caller advertises what it has, the responder
  // replies with what it should send and what it wants back.
  rpc Gossip(GossipDigest) returns (GossipDelta);
}

message DigestEntry {
  string file_id = 1;
  map<string, int64> vector_clock = 2;
}

message MemberStatus {
  string node_id = 1;
  // Mirrors NodeState: 0 alive, 1 suspect, 2 dead.
  int32 state = 2;
  int64 incarnation = 3;
  string since = 4;  // ISO-8601
}

message GossipDigest {
  string from_node_id = 1;
  repeated DigestEntry entries = 2;
  repeated MemberStatus membership = 3;
}

message GossipDelta {
  // Records the responder is ahead on, or that the caller has never seen.
  repeated FileRecordMsg records = 1;
  // file_ids the responder is behind on, or has never seen.
  repeated string wanted_file_ids = 2;
  repeated MemberStatus membership = 3;
}
```

- [ ] **Step 3: Regenerate** — `./scripts/gen_protos.sh`

- [ ] **Step 4: Write the failing servicer tests**, covering: a digest from an
empty peer gets every local record pushed; a digest listing a file the responder
lacks produces it in `wanted_file_ids`; a newer remote clock produces `wanted`
and no push; equal clocks produce an empty delta; and the membership snapshot
travels in both directions.

- [ ] **Step 5: Implement `Gossip` in `ClusterServicer`**, delegating the
decision to `plan_exchange` and the membership merge to `Membership.merge`. The
servicer holds a reference to the node's `Membership` — pass it in the
constructor alongside the store, and update `build_server` accordingly.

- [ ] **Step 6: Run tests, commit** — `git commit -m "Serve push-pull gossip over gRPC"`

---

### Task 6: The gossip client — one round against one peer

**Files:** Create `almacen/cluster/gossip.py`; test `tests/integration/cluster/test_gossip.py`

- [ ] **Step 1: Write the failing tests.** Two real nodes with divergent state;
one `gossip_once` call and both converge. Also: a round against an unreachable
peer records the failure in membership and does not raise.

- [ ] **Step 2: Implement `gossip_once`**

```python
def gossip_once(peer: Peer, *, settings, store, membership, channels, now) -> bool:
    """Run one push-pull exchange with `peer`. Returns whether it succeeded.

    Failure is reported, never raised: an unreachable peer is the normal case
    this phase exists to handle, and the caller (a background loop) must keep
    going.
    """
    local_records = store.list_all()
    request = _digest_message(settings.node_id, local_records, membership, now)
    try:
        stub = cluster_pb2_grpc.ClusterStub(channels.channel(peer.address))
        delta = stub.Gossip(request, timeout=settings.rpc_timeout_seconds)
    except grpc.RpcError as error:
        logger.info("gossip with %s failed: %s", peer.node_id, error)
        membership.record_unreachable(peer.node_id, now)
        return False

    membership.record_reachable(peer.node_id, now)
    membership.merge(_decode_membership(delta.membership), now)

    # Pull: merge what the peer is ahead on.
    for message in delta.records:
        store.merge_remote(message_to_record(message))

    # Push: send back what the peer said it is behind on. ReplicateRecord
    # already merges on the far side, so no new transfer path is needed.
    wanted = {uuid.UUID(raw) for raw in delta.wanted_file_ids}
    for record in local_records:
        if record.file_id in wanted:
            stub.ReplicateRecord(
                record_to_message(record), timeout=settings.rpc_timeout_seconds
            )
    return True
```

- [ ] **Step 3: Run tests, commit** — `git commit -m "Add a gossip round that reconciles metadata with one peer"`

---

### Task 7: Blob catch-up

Metadata converging is not enough: a node that gains a record pointing at content
it does not hold still cannot serve it.

**Files:** Modify `almacen/cluster/gossip.py`; test `tests/integration/cluster/test_blob_catchup.py`

- [ ] **Step 1: Write the failing tests.** A node that is an HRW replica for a
blob but lacks it fetches it after a gossip round. A node that is *not* a replica
does **not** fetch it — otherwise every node ends up holding everything and
Phase 2's sharding is quietly undone.

- [ ] **Step 2: Implement**

```python
def catch_up_blobs(*, settings, store, blob_store, replication_client) -> int:
    """Fetch blobs this node should hold but does not. Returns how many.

    Driven by placement, not by "fetch whatever I am missing": a node pulls only
    the content HRW makes it a replica for. Fetching everything would make each
    node store the whole corpus and undo the sharding.
    """
    fetched = 0
    for record in store.list_live():
        replicas = replica_set(
            record.content_hash, settings.node_ids, settings.replication_factor
        )
        if settings.node_id not in replicas:
            continue
        if blob_store.exists(record.content_hash):
            continue
        content = replication_client.get_blob(record.content_hash)
        if content is not None:
            blob_store.put(content)
            fetched += 1
    return fetched
```

- [ ] **Step 3: Run tests, commit** — `git commit -m "Fetch blobs a node should hold after reconciling metadata"`

---

### Task 8: Configuration and the background loop

**Files:** Modify `almacen/config.py`, `almacen/main.py`; create the driver in `almacen/cluster/gossip.py`; test `tests/unit/test_config.py`, `tests/integration/cluster/test_gossip_loop.py`

- [ ] **Step 1: Add settings** — `gossip_interval_seconds` (default 5.0),
`suspicion_timeout_seconds` (default 15.0), `gossip_fanout` (default 2), each
with an `ALMACEN_*` environment variable and validation that it is positive. Add
tests mirroring the existing config tests.

- [ ] **Step 2: Write the driver**

```python
class GossipLoop:
    """Runs gossip rounds on a timer until stopped.

    Deliberately thin: it owns the clock and the peer choice, and nothing else.
    All the protocol lives in `membership.py` and `reconciliation.py`, which take
    time and peers as parameters — which is why none of this phase's tests need
    to sleep.
    """

    def __init__(self, *, settings, store, blob_store, membership, channels,
                 replication_client, choose_peers=None, now=None) -> None:
        # Injected in tests: a deterministic chooser and a controllable clock.
        # Random peer selection needs up to 23 rounds to converge five nodes,
        # so a test using it would be flaky at any round count fast enough to
        # be worth running.
        self._choose_peers = choose_peers or self._random_peers
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        ...

    def run_once(self) -> None:
        """One tick: gossip with the chosen peers, then catch up on blobs."""

    def start(self) -> None: ...

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the thread and wait for it, so tests leave nothing running."""
```

`stop()` must set the event **and** join the thread. A loop that outlives its
test leaks a thread per test and produces failures attributed to whatever runs
next.

- [ ] **Step 3: Test the driver without sleeping** — inject a fake clock and a
deterministic chooser, call `run_once()` directly, and assert convergence.
Separately, assert that `start()` followed by `stop()` leaves no live thread.

- [ ] **Step 4: Wire it into the lifespan** in `main.py`, started after the gRPC
server and stopped before it, so the node is reachable for the whole time it is
gossiping.

- [ ] **Step 5: Run the whole suite**, then commit —
`git commit -m "Run gossip on a timer for as long as the node is serving"`

---

### Task 9: Partition and healing

The tests this phase exists for.

**Files:** Create `tests/integration/cluster/test_partition_healing.py`

- [ ] **Step 1: Write the tests.** A partition is simulated by stopping a node's
gRPC server and restarting it on the same port — cheaper and more deterministic
than manipulating the network, and it exercises the same code paths. Cover:

- A write that happens while a node is unreachable reaches it after gossip
  resumes. **This is the regression Phase 3 could not pass**, since its only
  delivery was a single best-effort push.
- Edits made on *both* sides of a partition survive the heal, merged.
- A delete issued during a partition reaches the partitioned node once healed,
  and the file is gone everywhere.
- A node that missed an upload entirely gains both the record and — if it is an
  HRW replica — the blob.
- A node unreachable for longer than the suspicion timeout is marked dead, and
  is marked alive again once it answers.
- Gossip is idempotent: running extra rounds after convergence changes nothing.

- [ ] **Step 2: Verify these tests have teeth.** Disable the gossip loop (or make
`plan_exchange` return an empty plan) and confirm the partition-healing tests
fail. A test that passes with reconciliation disabled is testing nothing.

- [ ] **Step 3: Commit** — `git commit -m "Add partition and healing tests"`

---

### Task 10: The live demo

**Files:** Modify `docker/docker-compose.yml`, `README.md`

- [ ] **Step 1: Shorten the timers in Compose** so reconciliation is observable
in a demo session rather than on a production schedule: `ALMACEN_GOSSIP_INTERVAL_SECONDS: "2"`,
`ALMACEN_SUSPICION_TIMEOUT_SECONDS: "6"`.

- [ ] **Step 2: Verify a real partition by hand**

```bash
docker compose -f docker/docker-compose.yml up --build -d
# Cut node5 off from the cluster network.
docker network disconnect almacen_default almacen-node5-1

# Write while it is isolated.
FILE_ID=$(curl -s -F "file=@README.md" -F "name=during-partition.md" \
  -F "tags=partitioned" http://127.0.0.1:8001/files \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['file_id'])")

curl -s -o /dev/null -w "node5 during partition: %{http_code}\n" \
  http://127.0.0.1:8005/files/$FILE_ID          # expect 404

# Heal, wait a couple of gossip rounds.
docker network connect almacen_default almacen-node5-1
sleep 8
curl -s -o /dev/null -w "node5 after healing:    %{http_code}\n" \
  http://127.0.0.1:8005/files/$FILE_ID          # expect 200
```

- [ ] **Step 3: Document it in the README** — a Phase 4 status section, the new
environment variables, and the partition demo above.

- [ ] **Step 4: Tear down and commit.**

---

### Task 11: Full verification

- [ ] **Step 1:** `pytest -q` — no failures, no skips.
- [ ] **Step 2:** Confirm no test sleeps: `grep -rn "time.sleep" tests/` should
return nothing. If a test needs to wait for the background loop, it should call
`run_once()` instead.
- [ ] **Step 3:** Confirm no gossip threads leak — the suite should not slow down
or produce cross-test interference as partition tests accumulate.
- [ ] **Step 4:** Run the repository hygiene check from the project rules file.
- [ ] **Step 5:** Commit any documentation left over.

---

## Definition of done

- [ ] `pytest -q` passes with no failures and no skips, and no test calls `time.sleep`.
- [ ] A write made while a node is unreachable reaches it after the partition heals.
- [ ] Edits on both sides of a partition survive, merged.
- [ ] A tombstone created during a partition propagates on healing.
- [ ] A node that missed an upload gains the record, and the blob if it is a replica.
- [ ] A node is suspected before being declared dead, and can return to alive.
- [ ] A false suspicion is refuted rather than sticking.
- [ ] The partition-healing tests fail when reconciliation is disabled.
- [ ] The Compose partition demo works end to end.
- [ ] Both DAA notes exist and are untracked.
- [ ] The hygiene rules from the project rules file are satisfied.
