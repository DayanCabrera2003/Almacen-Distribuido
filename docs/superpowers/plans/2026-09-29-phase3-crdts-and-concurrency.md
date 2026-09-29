# Phase 3 — CRDTs and Concurrency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Phase 2's "last write to arrive wins" metadata merge with real CRDTs, so that concurrent edits made on different nodes converge to the same state no matter what order updates arrive in — and prove it with tests that would fail against the current implementation.

**Architecture:** A new `almacen/crdt/` package holding three pure, I/O-free types — `VectorClock`, `LWWRegister`, `OrSet` — and a `FileRecord` rebuilt on top of them. The entity keeps its current *read* interface (`record.name`, `record.tags`, `record.tombstone`, …) so routers, schemas and `TagIndex` are untouched; what changes is that every mutation now needs the acting node's id, and that two copies of a record can be merged. `MetadataStore` persists the CRDT state, the wire format carries it, and `ClusterServicer` merges instead of overwriting.

**Tech Stack:** Unchanged. Pure stdlib for the CRDTs.

**Spec:** §6 (data model, vector-clock semantics, tombstone merge rule), §9 (concurrency and conflict resolution), §16 (this is phase 3 of 6).

---

## What this phase fixes

Phase 2 ends with `ClusterServicer.ReplicateRecord` calling `upsert`, which overwrites the local record wholesale. Two nodes editing the same file concurrently clobber each other, and *which* edit survives depends on network arrival order — so two nodes can end up permanently disagreeing. That is the gap this phase closes, and it is the assignment's "tratamiento de actualizaciones concurrentes" requirement.

**In scope:** the three CRDTs, `FileRecord` built on them, persistence of CRDT state, the wire format, merge-on-receive, conflict logging, and convergence tests.

**Out of scope, by phase:**

| Deferred to | What |
|---|---|
| Phase 4 | Gossip/SWIM membership and push-pull anti-entropy. Records still only move by the direct `ReplicateRecord` push from Phase 2; a peer that misses a push still diverges until something re-sends. Phase 3 makes the *merge* correct, not the *delivery*. |
| Phase 5 | Tombstone TTL and purge, blob GC, and pruning the OR-Set's remove set (which grows without bound — see Task 4). |
| Phase 6 | `/cluster/status`, structlog, Typer CLI. |

Do not add a gossip loop "to test convergence properly". Convergence is a property of the merge function and is tested directly on records (Task 10); delivery is Phase 4's problem.

---

## Design decisions made while planning

The spec fixes the CRDT choices; these mechanics are not pinned down by it.

1. **`FileRecord` keeps its read interface; only mutations change.** `record.name`, `.content_hash`, `.tombstone`, `.tags`, `.created_at`, `.updated_at` stay plain-valued **properties** over CRDT internals. A survey of the codebase found 8 distinct read attributes used across the API and storage layers and only 5 mutators. Changing the reads would ripple into `schemas.py`, both routers, `TagIndex` and `MetadataStore` for no benefit; changing the mutators is unavoidable, because a CRDT mutation needs to know which node is acting.

2. **Every mutator takes `node_id` explicitly.** Not stored on the record (records travel between nodes, so an "owning node" would be meaningless), and not read from a global (that would make the entity depend on configuration). The API layer already knows its node id from `Settings`; Task 6 plumbs it through a dependency.

3. **`updated_at` becomes a max-register, not an LWW-register.** The other three single-value fields are LWW-registers with a `(timestamp, node_id)` tie-break, but `updated_at` needs no tie-break: it is monotonic by construction, so merging is just `max`. This matters because tag changes must still bump `updated_at` — the OR-Set carries no timestamps, so without a dedicated register a tag edit would leave `updated_at` stale, which is a visible regression in the `FileMetadata` response.

4. **The OR-Set keeps an explicit remove set, rather than the tombstone-free ORSWOT variant.** It is what spec §9 describes ("`remove(tag)` removes every unique element observed up to that point"), and it is far easier to verify by hand. It grows without bound — measured: 100 add/remove cycles on one tag leave 100 adds and 100 removes stored for zero visible tags. That cost is accepted here and written up in Task 4; pruning belongs with the other GC work in Phase 5.

5. **The vector clock is stored as a JSON column; tag elements get real tables.** The clock is a small map read and written only as a whole, so a column avoids a join for no loss. The OR-Set's add/remove sets are queried per element and grow, so they get indexed tables.

6. **Merging happens in the storage layer's `upsert` path, not in the servicer.** `ClusterServicer` receives a record and asks the store to merge it in. Doing the read-merge-write inside `MetadataStore` under its existing lock makes it atomic, exactly as Phase 2's `mutate` did for local edits — otherwise two records arriving for the same file at once would race.

7. **No schema migration is written.** The database is local per node, holds only demo data, and the schema changes shape substantially. Nodes are expected to start from an empty `ALMACEN_DATA_DIR`. This is stated in the README rather than pretended otherwise.

---

## File Structure

```
almacen/
  crdt/
    __init__.py                    # NEW
    vector_clock.py                # NEW: causality tracking
    lww_register.py                # NEW: single-value fields
    or_set.py                      # NEW: tags
  domain/
    file_record.py                 # REWRITTEN on CRDTs; read interface preserved
  storage/
    metadata_store.py              # MODIFY: CRDT schema, merge_remote()
  rpc/
    cluster.proto                  # MODIFY: carry CRDT state
    record_codec.py                # MODIFY: encode/decode CRDT state
    cluster_servicer.py            # MODIFY: merge instead of overwrite
  api/
    deps.py                        # MODIFY: get_node_id, node_id into mutations
    routers/files.py, tags.py      # MODIFY: pass node_id to mutators
tests/
  unit/crdt/test_vector_clock.py   # NEW
  unit/crdt/test_lww_register.py   # NEW
  unit/crdt/test_or_set.py         # NEW
  unit/domain/test_file_record.py  # REWRITTEN
  unit/domain/test_merge_laws.py   # NEW: commutativity/associativity/idempotence
  integration/cluster/test_convergence.py  # NEW: the headline tests
DAA/crdt-selection.md              # NEW (untracked)
DAA/vector-clocks-and-conflict-detection.md  # NEW (untracked)
```

---

### Task 1: `VectorClock`

**Files:** Create `almacen/crdt/__init__.py` (empty), `almacen/crdt/vector_clock.py`; test `tests/unit/crdt/__init__.py` (empty), `tests/unit/crdt/test_vector_clock.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/crdt/test_vector_clock.py
from almacen.crdt.vector_clock import VectorClock


def test_a_new_clock_reads_zero_for_every_node():
    assert VectorClock().get("node1") == 0


def test_increment_returns_the_new_counter():
    clock = VectorClock()
    assert clock.increment("node1") == 1
    assert clock.increment("node1") == 2
    assert clock.get("node1") == 2


def test_increment_touches_only_its_own_component():
    clock = VectorClock({"node1": 5, "node2": 3})
    clock.increment("node1")
    assert clock.get("node1") == 6
    assert clock.get("node2") == 3


def test_a_clock_dominates_one_it_is_strictly_ahead_of():
    ahead = VectorClock({"node1": 2, "node2": 1})
    behind = VectorClock({"node1": 1, "node2": 1})
    assert ahead.dominates(behind)
    assert not behind.dominates(ahead)


def test_equal_clocks_do_not_dominate_each_other():
    clock = VectorClock({"node1": 1})
    assert not clock.dominates(VectorClock({"node1": 1}))


def test_a_missing_component_counts_as_zero():
    # node2 having no entry must mean "has seen nothing from node2", not
    # "incomparable" — otherwise a node that never wrote would make every
    # comparison concurrent.
    ahead = VectorClock({"node1": 1, "node2": 1})
    behind = VectorClock({"node1": 1})
    assert ahead.dominates(behind)


def test_clocks_that_each_saw_something_the_other_did_not_are_concurrent():
    left = VectorClock({"node1": 2, "node2": 1})
    right = VectorClock({"node1": 1, "node2": 2})
    assert left.concurrent_with(right)
    assert right.concurrent_with(left)
    assert not left.dominates(right)
    assert not right.dominates(left)


def test_equal_clocks_are_not_concurrent():
    assert not VectorClock({"node1": 1}).concurrent_with(VectorClock({"node1": 1}))


def test_a_dominating_clock_is_not_concurrent():
    ahead = VectorClock({"node1": 2})
    assert not ahead.concurrent_with(VectorClock({"node1": 1}))


def test_merge_takes_the_component_wise_maximum():
    left = VectorClock({"node1": 3, "node2": 1})
    right = VectorClock({"node1": 1, "node3": 7})
    merged = left.merged(right)
    assert merged.counters == {"node1": 3, "node2": 1, "node3": 7}


def test_merge_does_not_mutate_either_input():
    left = VectorClock({"node1": 1})
    right = VectorClock({"node2": 1})
    left.merged(right)
    assert left.counters == {"node1": 1}
    assert right.counters == {"node2": 1}


def test_merge_is_commutative_and_idempotent():
    left = VectorClock({"node1": 3, "node2": 1})
    right = VectorClock({"node1": 1, "node3": 7})
    assert left.merged(right).counters == right.merged(left).counters
    assert left.merged(left).counters == left.counters
```

- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/crdt/test_vector_clock.py -v`, expect `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

```python
# almacen/crdt/vector_clock.py
"""Causality tracking: which of two record versions happened first."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class VectorClock:
    """One counter per node, tracking what this replica has observed.

    Used to tell a *causally stale* update (safe to discard) from a *genuinely
    concurrent* one (a real conflict that needs a resolution rule). Absent
    components read as zero, so a node that has never written is simply behind,
    not incomparable.
    """

    counters: dict[str, int] = field(default_factory=dict)

    def get(self, node_id: str) -> int:
        return self.counters.get(node_id, 0)

    def increment(self, node_id: str) -> int:
        """Record one local mutation by `node_id`; returns the new counter."""
        self.counters[node_id] = self.get(node_id) + 1
        return self.counters[node_id]

    def dominates(self, other: "VectorClock") -> bool:
        """Whether this clock has seen strictly more than `other`."""
        nodes = set(self.counters) | set(other.counters)
        return all(self.get(n) >= other.get(n) for n in nodes) and self != other

    def concurrent_with(self, other: "VectorClock") -> bool:
        """Whether neither clock has seen everything the other has."""
        return (
            self != other
            and not self.dominates(other)
            and not other.dominates(self)
        )

    def merged(self, other: "VectorClock") -> "VectorClock":
        """Component-wise maximum. Neither input is modified."""
        nodes = set(self.counters) | set(other.counters)
        return VectorClock({n: max(self.get(n), other.get(n)) for n in nodes})

    def __eq__(self, other: object) -> bool:
        # Compare on observed values, so {"a": 1} equals {"a": 1, "b": 0}.
        if not isinstance(other, VectorClock):
            return NotImplemented
        nodes = set(self.counters) | set(other.counters)
        return all(self.get(n) == other.get(n) for n in nodes)
```

Note the custom `__eq__`: without it, `{"a": 1}` and `{"a": 1, "b": 0}` would
compare unequal and be reported as concurrent, even though they describe the
same history. `dominates` and `concurrent_with` both depend on it.

- [ ] **Step 4: Run tests** — expect 12 passed.
- [ ] **Step 5: Commit** — `git commit -m "Add vector clock for causality tracking"`

---

### Task 2: `LWWRegister`

**Files:** Create `almacen/crdt/lww_register.py`; test `tests/unit/crdt/test_lww_register.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/crdt/test_lww_register.py
from datetime import datetime, timedelta, timezone

from almacen.crdt.lww_register import LWWRegister

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T1 = T0 + timedelta(seconds=1)


def test_the_later_write_wins():
    older = LWWRegister("old", T0, "node1")
    newer = LWWRegister("new", T1, "node1")
    assert older.merged(newer).value == "new"
    assert newer.merged(older).value == "new"


def test_an_exact_timestamp_tie_is_broken_by_node_id():
    # Two nodes writing in the same clock tick must still agree, and agree the
    # same way regardless of which side merges.
    from_a = LWWRegister("from-a", T0, "nodeA")
    from_b = LWWRegister("from-b", T0, "nodeB")
    assert from_a.merged(from_b).value == "from-b"
    assert from_b.merged(from_a).value == "from-b"


def test_merging_a_register_with_itself_changes_nothing():
    register = LWWRegister("v", T0, "node1")
    assert register.merged(register) == register


def test_merge_is_associative():
    a = LWWRegister("a", T0, "node1")
    b = LWWRegister("b", T1, "node2")
    c = LWWRegister("c", T0, "node3")
    assert a.merged(b).merged(c) == a.merged(b.merged(c))


def test_merge_does_not_mutate_either_input():
    older = LWWRegister("old", T0, "node1")
    newer = LWWRegister("new", T1, "node2")
    older.merged(newer)
    assert older.value == "old"
    assert newer.value == "new"


def test_it_carries_any_value_type():
    assert LWWRegister(False, T0, "n").merged(LWWRegister(True, T1, "n")).value is True
```

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Implement**

```python
# almacen/crdt/lww_register.py
"""Last-writer-wins register for single-value fields (spec §9)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class LWWRegister(Generic[T]):
    """A value plus the (timestamp, node_id) that wrote it.

    Frozen: a write produces a new register rather than mutating one, which
    keeps merge free of aliasing surprises.
    """

    value: T
    timestamp: datetime
    node_id: str

    def merged(self, other: "LWWRegister[T]") -> "LWWRegister[T]":
        """Later timestamp wins; an exact tie is broken by the higher node_id.

        The tie-break is what makes merge deterministic. Without it, two writes
        in the same clock tick would resolve differently depending on which
        replica performed the merge, and the replicas would never converge.
        """
        return self if self._rank() >= other._rank() else other

    def _rank(self) -> tuple[datetime, str]:
        return (self.timestamp, self.node_id)
```

- [ ] **Step 4: Run tests** — expect 6 passed.
- [ ] **Step 5: Commit** — `git commit -m "Add last-writer-wins register for single-value fields"`

---

### Task 3: `OrSet`

**Files:** Create `almacen/crdt/or_set.py`; test `tests/unit/crdt/test_or_set.py`

- [ ] **Step 1: Write the failing tests**

The first test is the one that distinguishes an OR-Set from every simpler set
CRDT, and it is the reason this type was chosen.

```python
# tests/unit/crdt/test_or_set.py
from almacen.crdt.or_set import OrSet


def make(*elements: str) -> OrSet:
    s = OrSet()
    for index, element in enumerate(elements, start=1):
        s.add(element, "node1", index)
    return s


def test_a_concurrent_add_beats_a_concurrent_remove():
    # THE defining property. node1 removes the "draft" it has seen; node2, not
    # having seen that removal, adds "draft" again. The add must survive: a
    # remove can only cancel the adds it actually observed.
    original = make("draft")
    on_node1, on_node2 = original.copy(), original.copy()

    on_node1.remove("draft")
    on_node2.add("draft", "node2", 1)

    assert "draft" in on_node1.merged(on_node2).value()
    assert "draft" in on_node2.merged(on_node1).value()


def test_a_remove_cancels_the_adds_it_has_observed():
    s = make("draft")
    s.remove("draft")
    assert s.value() == set()


def test_an_element_can_be_re_added_after_removal():
    s = make("draft")
    s.remove("draft")
    s.add("draft", "node1", 2)
    assert s.value() == {"draft"}


def test_removing_an_unknown_element_is_a_no_op():
    s = make("a")
    s.remove("never-added")
    assert s.value() == {"a"}


def test_concurrent_adds_of_different_elements_both_survive():
    original = make("base")
    left, right = original.copy(), original.copy()
    left.add("x", "node1", 2)
    right.add("y", "node2", 1)
    assert left.merged(right).value() == {"base", "x", "y"}


def test_concurrent_removes_of_different_elements_both_apply():
    original = make("a", "b")
    left, right = original.copy(), original.copy()
    left.remove("a")
    right.remove("b")
    assert left.merged(right).value() == set()


def test_merge_is_commutative_associative_and_idempotent():
    a, b, c = make("a"), make("b"), make("c")
    assert a.merged(b).value() == b.merged(a).value()
    assert a.merged(b).merged(c).value() == a.merged(b.merged(c)).value()
    assert a.merged(a).value() == a.value()


def test_merge_does_not_mutate_either_input():
    left, right = make("a"), make("b")
    left.merged(right)
    assert left.value() == {"a"}
    assert right.value() == {"b"}


def test_copy_is_independent_of_the_original():
    original = make("a")
    duplicate = original.copy()
    duplicate.add("b", "node2", 1)
    assert original.value() == {"a"}
```

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Implement**

```python
# almacen/crdt/or_set.py
"""Observed-Remove Set: the CRDT behind a file's tags (spec §9)."""
from __future__ import annotations

from dataclasses import dataclass, field

# One occurrence of an element: (element, node_id, counter). The node_id and
# counter make otherwise-identical adds distinguishable, which is what lets a
# remove cancel exactly the adds it observed and no others.
Element = tuple[str, str, int]


@dataclass
class OrSet:
    """A set where a concurrent add beats a concurrent remove.

    Each `add` records a uniquely tagged occurrence. `remove` cancels every
    occurrence *currently observed*, so an add that this replica has not yet
    seen survives the merge — which is the behaviour that makes tag edits on
    partitioned nodes converge without losing work.
    """

    adds: set[Element] = field(default_factory=set)
    removes: set[Element] = field(default_factory=set)

    def add(self, element: str, node_id: str, counter: int) -> None:
        self.adds.add((element, node_id, counter))

    def remove(self, element: str) -> None:
        """Cancel every observed occurrence of `element`.

        Unobserved occurrences — including ones added concurrently elsewhere —
        are untouched, so they survive the merge.
        """
        self.removes |= {occ for occ in self.adds if occ[0] == element}

    def value(self) -> set[str]:
        """The elements currently in the set."""
        return {occ[0] for occ in self.adds - self.removes}

    def merged(self, other: "OrSet") -> "OrSet":
        """Union of both sides' adds and removes. Neither input is modified."""
        return OrSet(self.adds | other.adds, self.removes | other.removes)

    def copy(self) -> "OrSet":
        return OrSet(set(self.adds), set(self.removes))
```

- [ ] **Step 4: Run tests** — expect 9 passed.
- [ ] **Step 5: Commit** — `git commit -m "Add observed-remove set for conflict-free tag edits"`

---

### Task 4: DAA note — why these three CRDTs

**Files:** Create `../DAA/crdt-selection.md` (i.e. `Proyecto/DAA/crdt-selection.md`, outside the repository)

- [ ] **Step 1: Write the note.** State the problem and the reasoning, not just the conclusion. Cover at minimum:

- **Problem:** two nodes accept edits to the same file while unable to talk to each other; when they reconnect, both must reach the same state, with no coordinator, no locks, and no user asked to resolve anything. Formally: the merge function must be **commutative, associative and idempotent**, because the network provides no ordering guarantee and may deliver the same update twice.
- **Tags — why OR-Set, with the alternatives rejected on their merits:**
  - *G-Set* (grow-only): converges trivially, but `remove_tag` becomes impossible. Rejected on requirements.
  - *2P-Set*: allows one removal per element, then the element can never be re-added. "Remove the `draft` tag, later re-add it" is ordinary usage, so this is wrong.
  - *LWW-Set*: attaches timestamps to adds and removes and lets the later win. Converges, but resolves a concurrent add-vs-remove by clock skew, so a tag can vanish because a node's clock ran fast. Rejected because it makes correctness depend on clock synchrony.
  - *OR-Set* (chosen): a remove cancels only the occurrences it observed, so a concurrent add always survives. Converges without relying on clocks at all. The add-wins bias is the right default here — losing a tag someone just applied is worse than keeping one someone just removed, and the user can remove it again.
- **Complexity:** `add` O(1); `remove` O(|adds|) as written, since it scans for occurrences of the element — indexing adds by element would make it O(occurrences of that element), and is the first optimization if tag sets grow; `value()` O(|adds| + |removes|); `merged` O(n + m) set unions. State grows with the number of *operations*, not the number of live tags — **measured: 100 add/remove cycles on a single tag leave 100 adds and 100 removes stored for zero visible tags.**
- **The growth problem, honestly:** the remove set never shrinks. The standard answer is ORSWOT (OR-Set Without Tombstones), which replaces the explicit remove set with a per-node version vector and infers removals, giving state proportional to live elements plus nodes. Not implemented, for two reasons worth stating: it is materially harder to verify by hand, and spec §9 specifies the observed-remove formulation. Pruning belongs with the other reclamation work in Phase 5.
- **Single-value fields — why LWW-register:** name and content_hash have no meaningful merge (there is no "average" of two filenames), so *some* arbitrary winner must be picked. LWW with a `(timestamp, node_id)` tie-break is the simplest deterministic rule. State the weakness plainly: under clock skew the "later" write may not be the later *real-world* write, and one of two concurrent edits is discarded. The spec's mitigation is that genuine concurrency is *detected* via the vector clock and logged rather than silently dropped (Task 9). The alternative — exposing both siblings to the client, Dynamo-style — is explicitly out of scope per spec §17.
- **Why the tie-break cannot be omitted:** with equal timestamps and no tie-break, the merge result depends on which replica performed it, so replicas would never converge. Cheap to state, easy to get wrong.
- **Tombstone as a third LWW-register:** and why that is equivalent to "delete wins" in practice — a non-delete edit never writes the tombstone field, so it can never produce a competing timestamp for it (spec §6).

- [ ] **Step 2: Verify it is untracked** — `git status --short` must not mention `DAA/`.

---

### Task 5: `FileRecord` rebuilt on CRDTs

The heart of the phase. The read interface is preserved exactly; mutations gain a
`node_id`; the record gains `merged`.

**Files:** Rewrite `almacen/domain/file_record.py`; rewrite `tests/unit/domain/test_file_record.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/domain/test_file_record.py
import uuid
from datetime import datetime, timezone

from almacen.domain.file_record import FileRecord

NODE = "node1"


def test_new_exposes_plain_values_through_its_read_interface():
    record = FileRecord.new(
        name="a.txt", content_hash="h1", node_id=NODE, tags={"x", "y"}
    )

    assert isinstance(record.file_id, uuid.UUID)
    assert record.name == "a.txt"
    assert record.content_hash == "h1"
    assert record.tags == {"x", "y"}
    assert record.tombstone is False
    assert record.tombstone_at is None
    assert record.created_at.tzinfo is not None
    assert record.updated_at >= record.created_at


def test_new_records_get_distinct_ids():
    first = FileRecord.new(name="a", content_hash="h", node_id=NODE)
    second = FileRecord.new(name="a", content_hash="h", node_id=NODE)
    assert first.file_id != second.file_id


def test_new_without_tags_starts_empty():
    assert FileRecord.new(name="a", content_hash="h", node_id=NODE).tags == set()


def test_rename_changes_the_name_and_bumps_updated_at():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)
    before = record.updated_at

    record.rename("b.txt", NODE)

    assert record.name == "b.txt"
    assert record.updated_at >= before


def test_update_content_repoints_the_hash():
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id=NODE)
    record.update_content("h2", NODE)
    assert record.content_hash == "h2"


def test_add_and_remove_tag():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)

    record.add_tag("invoice", NODE)
    assert record.tags == {"invoice"}

    record.remove_tag("invoice", NODE)
    assert record.tags == set()


def test_removing_a_tag_the_file_does_not_have_is_a_no_op():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE, tags={"x"})
    record.remove_tag("absent", NODE)
    assert record.tags == {"x"}


def test_a_tag_change_bumps_updated_at():
    # The OR-Set carries no timestamps, so without a dedicated register a tag
    # edit would leave updated_at stale in the API response.
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)
    before = record.updated_at
    record.add_tag("x", NODE)
    assert record.updated_at >= before


def test_mark_deleted_sets_the_tombstone_and_its_timestamp():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)

    record.mark_deleted(NODE)

    assert record.tombstone is True
    assert record.tombstone_at is not None
    assert record.updated_at == record.tombstone_at


def test_every_mutation_advances_this_node_s_vector_clock():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)
    start = record.vector_clock.get(NODE)

    record.rename("b.txt", NODE)
    record.update_content("h2", NODE)
    record.add_tag("x", NODE)
    record.remove_tag("x", NODE)
    record.mark_deleted(NODE)

    assert record.vector_clock.get(NODE) == start + 5


def test_a_mutation_from_another_node_advances_only_that_component():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    record.rename("b.txt", "node2")
    assert record.vector_clock.get("node2") == 1
    assert record.vector_clock.get("node1") == 1  # only the creation


def test_tag_occurrences_use_the_vector_clock_as_their_counter():
    # Spec §6: one counter source, not two. Two adds of the same tag from the
    # same node must still be distinguishable occurrences.
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)
    record.add_tag("x", NODE)
    record.remove_tag("x", NODE)
    record.add_tag("x", NODE)
    assert record.tags == {"x"}
    assert len(record.tag_set.adds) == 2


def test_merge_of_concurrent_edits_keeps_both_tag_changes():
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1", tags={"draft"})
    on_node1, on_node2 = base.copy(), base.copy()

    on_node1.add_tag("urgent", "node1")
    on_node2.add_tag("reviewed", "node2")
    on_node2.remove_tag("draft", "node2")

    merged = on_node1.merged(on_node2)
    assert merged.tags == {"urgent", "reviewed"}


def test_merge_resolves_a_concurrent_rename_by_lww():
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    on_node1, on_node2 = base.copy(), base.copy()

    on_node1.rename("from-node1.txt", "node1")
    on_node2.rename("from-node2.txt", "node2")

    merged_left = on_node1.merged(on_node2)
    merged_right = on_node2.merged(on_node1)
    assert merged_left.name == merged_right.name, "both nodes must pick the same winner"


def test_merge_keeps_the_causally_newer_value():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    stale = record.copy()
    record.rename("newer.txt", "node1")

    assert record.merged(stale).name == "newer.txt"
    assert stale.merged(record).name == "newer.txt"


def test_a_delete_outlasts_a_concurrent_edit():
    # Spec §6: a non-delete edit never writes the tombstone field, so it cannot
    # produce a competing timestamp for it.
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    deleting, editing = base.copy(), base.copy()

    deleting.mark_deleted("node1")
    editing.add_tag("late", "node2")

    merged = deleting.merged(editing)
    assert merged.tombstone is True
    # The edit is not lost, just invisible through the API while tombstoned.
    assert "late" in merged.tags


def test_merge_combines_the_vector_clocks():
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    left, right = base.copy(), base.copy()
    left.rename("l", "node1")
    right.rename("r", "node2")

    merged = left.merged(right)
    assert merged.vector_clock.get("node1") == 2
    assert merged.vector_clock.get("node2") == 1


def test_merge_keeps_the_earlier_creation_time():
    left = FileRecord.new(name="a", content_hash="h", node_id="node1")
    right = left.copy()
    right.created_at = datetime(2030, 1, 1, tzinfo=timezone.utc)
    assert left.merged(right).created_at == left.created_at


def test_copy_is_independent_of_the_original():
    original = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE, tags={"x"})
    duplicate = original.copy()

    duplicate.add_tag("y", NODE)
    duplicate.rename("b.txt", NODE)

    assert original.tags == {"x"}
    assert original.name == "a.txt"


def test_merge_does_not_mutate_either_input():
    left = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    right = left.copy()
    right.rename("b.txt", "node2")

    left.merged(right)

    assert left.name == "a.txt"
    assert right.name == "b.txt"
```

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Rewrite `file_record.py`**

```python
# almacen/domain/file_record.py
"""Domain entity for a stored file, built on CRDTs so replicas converge."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from almacen.crdt.lww_register import LWWRegister
from almacen.crdt.or_set import OrSet
from almacen.crdt.vector_clock import VectorClock


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class FileRecord:
    """A file's identity and metadata, mergeable across replicas.

    The CRDT state lives in the `*_register` / `tag_set` / `vector_clock`
    fields; everything the rest of the system reads is exposed as a plain value
    through a property, so the API and storage layers never handle a CRDT
    directly.

    Every mutation takes the id of the node performing it: a CRDT operation is
    attributed, and the record itself has no home node — it travels.
    """

    file_id: uuid.UUID
    name_register: LWWRegister[str]
    content_hash_register: LWWRegister[str]
    tombstone_register: LWWRegister[bool]
    tag_set: OrSet
    vector_clock: VectorClock
    created_at: datetime
    # Monotonic, so merging is plain max — no tie-break needed. Kept separate
    # from the registers because tag edits must bump it and the OR-Set carries
    # no timestamps of its own.
    updated_at: datetime = field(default_factory=_utcnow)

    # ---- read interface: unchanged from Phase 2 ----

    @property
    def name(self) -> str:
        return self.name_register.value

    @property
    def content_hash(self) -> str:
        return self.content_hash_register.value

    @property
    def tombstone(self) -> bool:
        return self.tombstone_register.value

    @property
    def tombstone_at(self) -> datetime | None:
        return self.tombstone_register.timestamp if self.tombstone else None

    @property
    def tags(self) -> set[str]:
        return self.tag_set.value()

    # ---- construction ----

    @classmethod
    def new(
        cls,
        name: str,
        content_hash: str,
        node_id: str,
        tags: set[str] | None = None,
    ) -> "FileRecord":
        now = _utcnow()
        clock = VectorClock()
        counter = clock.increment(node_id)

        tag_set = OrSet()
        for tag in tags or set():
            tag_set.add(tag, node_id, counter)

        return cls(
            file_id=uuid.uuid4(),
            name_register=LWWRegister(name, now, node_id),
            content_hash_register=LWWRegister(content_hash, now, node_id),
            tombstone_register=LWWRegister(False, now, node_id),
            tag_set=tag_set,
            vector_clock=clock,
            created_at=now,
            updated_at=now,
        )

    # ---- mutations ----

    def rename(self, name: str, node_id: str) -> None:
        self._touch(node_id)
        self.name_register = LWWRegister(name, self.updated_at, node_id)

    def update_content(self, content_hash: str, node_id: str) -> None:
        self._touch(node_id)
        self.content_hash_register = LWWRegister(
            content_hash, self.updated_at, node_id
        )

    def add_tag(self, tag: str, node_id: str) -> None:
        counter = self._touch(node_id)
        # The vector clock is the single counter source (spec §6): the same
        # value identifies this mutation and tags this occurrence, so two adds
        # of the same tag from the same node stay distinguishable.
        self.tag_set.add(tag, node_id, counter)

    def remove_tag(self, tag: str, node_id: str) -> None:
        self._touch(node_id)
        self.tag_set.remove(tag)

    def mark_deleted(self, node_id: str) -> None:
        self._touch(node_id)
        self.tombstone_register = LWWRegister(True, self.updated_at, node_id)

    def _touch(self, node_id: str) -> int:
        """Record one mutation by `node_id`; returns its counter."""
        self.updated_at = _utcnow()
        return self.vector_clock.increment(node_id)

    # ---- replication ----

    def merged(self, other: "FileRecord") -> "FileRecord":
        """Combine two copies of the same file. Neither input is modified.

        Field-wise: LWW for the single-value fields, union for the tags,
        component-wise max for the clock. Commutative, associative and
        idempotent, which is what lets updates arrive in any order, more than
        once, and still converge.
        """
        if self.file_id != other.file_id:
            raise ValueError(
                f"cannot merge different files: {self.file_id} and {other.file_id}"
            )
        return FileRecord(
            file_id=self.file_id,
            name_register=self.name_register.merged(other.name_register),
            content_hash_register=self.content_hash_register.merged(
                other.content_hash_register
            ),
            tombstone_register=self.tombstone_register.merged(
                other.tombstone_register
            ),
            tag_set=self.tag_set.merged(other.tag_set),
            vector_clock=self.vector_clock.merged(other.vector_clock),
            # Creation is immutable; the earlier claim is the true one.
            created_at=min(self.created_at, other.created_at),
            updated_at=max(self.updated_at, other.updated_at),
        )

    def is_concurrent_with(self, other: "FileRecord") -> bool:
        """Whether neither copy has seen everything the other has (spec §6)."""
        return self.vector_clock.concurrent_with(other.vector_clock)

    def copy(self) -> "FileRecord":
        return FileRecord(
            file_id=self.file_id,
            name_register=self.name_register,
            content_hash_register=self.content_hash_register,
            tombstone_register=self.tombstone_register,
            tag_set=self.tag_set.copy(),
            vector_clock=VectorClock(dict(self.vector_clock.counters)),
            created_at=self.created_at,
            updated_at=self.updated_at,
        )
```

The registers are frozen dataclasses, so `copy` can share them safely; only the
mutable `OrSet` and `VectorClock` are duplicated.

- [ ] **Step 4: Run tests** — expect 19 passed.
- [ ] **Step 5: Commit** — `git commit -m "Rebuild FileRecord on CRDTs so replicas converge"`

---

### Task 6: Merge laws as executable properties

Task 5's tests cover specific scenarios. These assert the algebraic laws over
randomized histories, which is what actually guarantees convergence for histories
nobody thought to write a test for.

**Files:** Create `tests/unit/domain/test_merge_laws.py`

- [ ] **Step 1: Write the tests**

```python
# tests/unit/domain/test_merge_laws.py
"""Merge must be commutative, associative and idempotent.

These three laws are what make eventual consistency work: the network delivers
updates in arbitrary order, along multiple paths, and sometimes twice. If merge
obeys them, every replica that has seen the same set of updates holds the same
state regardless of the order it saw them in — no coordination needed.

Randomized histories rather than hand-picked ones, because the interesting
failures are the interleavings nobody thinks to write a test for.
"""
import random

import pytest

from almacen.domain.file_record import FileRecord

NODES = ("node1", "node2", "node3")
TAGS = ("a", "b", "c", "d")
TRIALS = 200


def _observable(record: FileRecord) -> tuple:
    """Everything a client could distinguish about a record."""
    return (
        record.name,
        record.content_hash,
        record.tombstone,
        tuple(sorted(record.tags)),
        tuple(sorted(record.vector_clock.counters.items())),
    )


def _random_history(record: FileRecord, node_id: str, rng: random.Random) -> None:
    for _ in range(rng.randint(1, 4)):
        action = rng.choice(["add", "remove", "rename", "content", "delete"])
        if action == "add":
            record.add_tag(rng.choice(TAGS), node_id)
        elif action == "remove":
            record.remove_tag(rng.choice(TAGS), node_id)
        elif action == "rename":
            record.rename(f"name-{rng.randint(0, 3)}", node_id)
        elif action == "content":
            record.update_content(f"hash-{rng.randint(0, 3)}", node_id)
        else:
            record.mark_deleted(node_id)


def _three_divergent_replicas(seed: int) -> tuple[FileRecord, FileRecord, FileRecord]:
    rng = random.Random(seed)
    base = FileRecord.new(
        name="base.txt", content_hash="h0", node_id="node1", tags={"a"}
    )
    replicas = []
    for node_id in NODES:
        replica = base.copy()
        _random_history(replica, node_id, rng)
        replicas.append(replica)
    return tuple(replicas)


@pytest.mark.parametrize("seed", range(TRIALS))
def test_merge_is_commutative(seed: int):
    left, right, _ = _three_divergent_replicas(seed)
    assert _observable(left.merged(right)) == _observable(right.merged(left))


@pytest.mark.parametrize("seed", range(TRIALS))
def test_merge_is_associative(seed: int):
    left, middle, right = _three_divergent_replicas(seed)
    assert _observable(left.merged(middle).merged(right)) == _observable(
        left.merged(middle.merged(right))
    )


@pytest.mark.parametrize("seed", range(TRIALS))
def test_merge_is_idempotent(seed: int):
    left, right, _ = _three_divergent_replicas(seed)
    once = left.merged(right)
    assert _observable(once.merged(right)) == _observable(once)
    assert _observable(left.merged(left)) == _observable(left)


@pytest.mark.parametrize("seed", range(TRIALS))
def test_replicas_converge_whatever_order_updates_arrive_in(seed: int):
    left, middle, right = _three_divergent_replicas(seed)
    rng = random.Random(seed)

    # Every node ends up with all three versions, but receives them in its own
    # order and via its own path.
    orders = [[left, middle, right] for _ in NODES]
    finals = []
    for order in orders:
        shuffled = order[:]
        rng.shuffle(shuffled)
        merged = shuffled[0]
        for other in shuffled[1:]:
            merged = merged.merged(other)
        finals.append(_observable(merged))

    assert len(set(finals)) == 1, "replicas that saw the same updates disagree"
```

- [ ] **Step 2: Run tests** — expect 800 passed (4 properties × 200 seeds), in well under a second.
- [ ] **Step 3: Commit** — `git commit -m "Assert the CRDT merge laws over randomized histories"`

---

### Task 7: Plumb node identity through the API

Every mutation now needs the acting node's id. The API layer is where it is known.

**Files:** Modify `almacen/api/deps.py`, `almacen/api/routers/files.py`, `almacen/api/routers/tags.py`; test `tests/integration/api/test_node_attribution.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/api/test_node_attribution.py
"""Edits must be attributed to the node that served them.

Attribution is what makes merge deterministic: it supplies the LWW tie-break
and the OR-Set occurrence tags. An edit recorded under the wrong node id would
still converge, but to the wrong value.
"""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from almacen.config import Settings
from almacen.main import create_app


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    settings = Settings(
        data_dir=tmp_path / "blobs",
        db_path=tmp_path / "metadata.db",
        node_id="node-alpha",
    )
    app = create_app(settings)
    client = TestClient(app)
    client.app_state = app.state  # for reaching the store directly
    return client


def test_an_upload_is_attributed_to_the_serving_node(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "x"},
    )
    file_id = upload.json()["file_id"]

    import uuid

    record = client.app_state.local_metadata_store.get(uuid.UUID(file_id))
    assert record.name_register.node_id == "node-alpha"
    assert record.vector_clock.get("node-alpha") >= 1
    assert all(occurrence[1] == "node-alpha" for occurrence in record.tag_set.adds)


def test_a_tag_edit_is_attributed_and_advances_the_clock(client: TestClient):
    import uuid

    upload = client.post(
        "/files", files={"file": ("a.txt", b"a", "text/plain")}, data={"name": "a.txt"}
    )
    file_id = uuid.UUID(upload.json()["file_id"])
    before = client.app_state.local_metadata_store.get(file_id).vector_clock.get(
        "node-alpha"
    )

    client.post(f"/files/{file_id}/tags", json={"tags": ["new"]})

    record = client.app_state.local_metadata_store.get(file_id)
    assert record.vector_clock.get("node-alpha") == before + 1
    assert record.tags == {"new"}


def test_a_rename_is_attributed_to_the_serving_node(client: TestClient):
    import uuid

    upload = client.post(
        "/files", files={"file": ("a.txt", b"a", "text/plain")}, data={"name": "a.txt"}
    )
    file_id = uuid.UUID(upload.json()["file_id"])

    client.patch(f"/files/{file_id}", data={"name": "b.txt"})

    record = client.app_state.local_metadata_store.get(file_id)
    assert record.name == "b.txt"
    assert record.name_register.node_id == "node-alpha"
```

- [ ] **Step 2: Run to verify it fails.**

- [ ] **Step 3: Add the dependency**

In `almacen/api/deps.py`:

```python
def get_node_id(request: Request) -> str:
    """This node's identity, which every CRDT mutation is attributed to."""
    return request.app.state.settings.node_id
```

- [ ] **Step 4: Thread it through the routers**

`files.py` — `upload_file` gains `node_id: str = Depends(get_node_id)` and calls
`FileRecord.new(..., node_id=node_id)`. `update_file` gains it and its `apply`
closure becomes:

```python
    def apply(record: FileRecord) -> None:
        if name is not None:
            record.rename(name, node_id)
        if content_hash is not None:
            record.update_content(content_hash, node_id)
```

`delete_file` gains it and becomes:

```python
    mutate_live_record(
        metadata_store, file_id, lambda record: record.mark_deleted(node_id)
    )
```

`tags.py` — `add_tags` and `remove_tag` gain it; their mutators pass `node_id` to
`add_tag` / `remove_tag`.

- [ ] **Step 5: Run the whole suite.** Every Phase 1/2 API test must still pass:
the responses are unchanged, only their attribution is new.

- [ ] **Step 6: Commit** — `git commit -m "Attribute every metadata edit to the node that served it"`

---

### Task 8: Persist the CRDT state

**Files:** Modify `almacen/storage/metadata_store.py`; test `tests/integration/storage/test_metadata_store_crdt.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/storage/test_metadata_store_crdt.py
"""The CRDT state must survive a round-trip through SQLite.

Persisting only the visible values would silently break convergence: a node
restarted mid-partition would forget which occurrences it had removed and which
writes it had seen, and would then merge to the wrong answer.
"""
from pathlib import Path

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore


def test_a_round_trip_preserves_the_lww_metadata(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node7")
    store.insert(record)

    restored = store.get(record.file_id)

    assert restored.name_register == record.name_register
    assert restored.content_hash_register == record.content_hash_register
    assert restored.tombstone_register == record.tombstone_register


def test_a_round_trip_preserves_the_vector_clock(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    record.rename("b.txt", "node2")
    record.add_tag("x", "node3")
    store.insert(record)

    restored = store.get(record.file_id)

    assert restored.vector_clock.counters == record.vector_clock.counters


def test_a_round_trip_preserves_removed_tag_occurrences(tmp_path: Path):
    # The removes are not derivable from the visible tag set, so losing them
    # would resurrect tags on the next merge.
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1",
                            tags={"draft"})
    record.remove_tag("draft", "node1")
    store.insert(record)

    restored = store.get(record.file_id)

    assert restored.tags == set()
    assert restored.tag_set.adds == record.tag_set.adds
    assert restored.tag_set.removes == record.tag_set.removes


def test_a_restored_record_still_merges_correctly(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1",
                          tags={"draft"})
    store.insert(base)

    # A peer concurrently re-adds the tag this node removed.
    elsewhere = base.copy()
    elsewhere.add_tag("draft", "node2")

    local = store.get(base.file_id)
    local.remove_tag("draft", "node1")
    store.update(local)

    merged = store.get(base.file_id).merged(elsewhere)
    assert "draft" in merged.tags, "a concurrent add must survive a reload"


def test_update_replaces_the_stored_occurrences(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    store.insert(record)

    record.add_tag("x", "node1")
    store.update(record)

    restored = store.get(record.file_id)
    assert restored.tags == {"x"}
    assert len(restored.tag_set.adds) == 1


def test_list_live_restores_full_crdt_state(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1",
                            tags={"x"})
    store.insert(record)

    (restored,) = store.list_live()

    assert restored.tags == {"x"}
    assert restored.vector_clock.counters == record.vector_clock.counters
```

- [ ] **Step 2: Run to verify they fail.**

- [ ] **Step 3: Replace the schema**

```sql
CREATE TABLE IF NOT EXISTS files (
    file_id TEXT PRIMARY KEY,
    -- Each single-value field stores its LWW value alongside the
    -- (timestamp, node_id) that wrote it; the value is kept in its own column
    -- so queries and manual inspection stay readable.
    name TEXT NOT NULL,
    name_ts TEXT NOT NULL,
    name_node TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    content_hash_ts TEXT NOT NULL,
    content_hash_node TEXT NOT NULL,
    tombstone INTEGER NOT NULL DEFAULT 0,
    tombstone_ts TEXT NOT NULL,
    tombstone_node TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    -- Small, read and written whole, so a column beats a join.
    vector_clock TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS file_tag_adds (
    file_id TEXT NOT NULL REFERENCES files(file_id),
    tag TEXT NOT NULL,
    node_id TEXT NOT NULL,
    counter INTEGER NOT NULL,
    PRIMARY KEY (file_id, tag, node_id, counter)
);

CREATE TABLE IF NOT EXISTS file_tag_removes (
    file_id TEXT NOT NULL REFERENCES files(file_id),
    tag TEXT NOT NULL,
    node_id TEXT NOT NULL,
    counter INTEGER NOT NULL,
    PRIMARY KEY (file_id, tag, node_id, counter)
);

CREATE INDEX IF NOT EXISTS idx_file_tag_adds_tag ON file_tag_adds(tag);
```

The old `file_tags` table is gone. `_insert_tags` becomes `_write_tag_state`,
writing both occurrence tables; `_row_to_record` reads them back and rebuilds the
`OrSet`, `LWWRegister`s and `VectorClock`. `vector_clock` is `json.dumps`ed.

Keep every existing locking rule: `_write` and `upsert` still run under
`with self._lock, self._conn:`, and no network call happens while the lock is
held.

- [ ] **Step 4: Run the storage tests**, then the whole suite. `TagIndex` needs no
change — it reads `record.tags`, which is now computed from the OR-Set.

- [ ] **Step 5: Commit** — `git commit -m "Persist CRDT state so a restarted node still merges correctly"`

---

### Task 9: Carry the CRDT state on the wire

**Files:** Modify `almacen/rpc/cluster.proto`, `almacen/rpc/record_codec.py`; test `tests/unit/rpc/test_record_codec.py`

- [ ] **Step 1: Extend the proto**

```protobuf
message TagOccurrence {
  string tag = 1;
  string node_id = 2;
  int64 counter = 3;
}

message FileRecordMsg {
  string file_id = 1;
  string name = 2;
  string name_ts = 3;
  string name_node = 4;
  string content_hash = 5;
  string content_hash_ts = 6;
  string content_hash_node = 7;
  bool tombstone = 8;
  string tombstone_ts = 9;
  string tombstone_node = 10;
  string created_at = 11;
  string updated_at = 12;
  map<string, int64> vector_clock = 13;
  repeated TagOccurrence tag_adds = 14;
  repeated TagOccurrence tag_removes = 15;
}
```

The Phase 2 `repeated string tags` field is gone: the visible tag set is derived
from the occurrences, and sending it too would let the two disagree.

- [ ] **Step 2: Regenerate** — `./scripts/gen_protos.sh`

- [ ] **Step 3: Extend the codec tests** — round-trip a record that has removed
occurrences and a multi-node vector clock, and assert the restored record merges
identically to the original. Keep the existing rejection test for missing
timestamps.

- [ ] **Step 4: Update `record_codec.py`** to encode and decode the new fields,
sorting the occurrence lists so the encoding stays deterministic.

- [ ] **Step 5: Run tests, commit** — `git commit -m "Carry CRDT state in the replication wire format"`

---

### Task 10: Merge on receive

The line this whole phase exists to change.

**Files:** Modify `almacen/storage/metadata_store.py` (add `merge_remote`), `almacen/rpc/cluster_servicer.py`; test `tests/integration/rpc/test_cluster_servicer.py`

- [ ] **Step 1: Write the failing tests** — append to the existing servicer tests:

```python
def test_a_replicated_record_is_merged_not_overwritten(stub, metadata_store):
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1",
                          tags={"shared"})
    metadata_store.insert(base)

    # This node adds a tag locally.
    local = metadata_store.get(base.file_id)
    local.add_tag("local-only", "node2")
    metadata_store.update(local)

    # A peer, which never saw that, adds a different tag and pushes.
    remote = base.copy()
    remote.add_tag("remote-only", "node3")
    stub.ReplicateRecord(record_to_message(remote))

    merged = metadata_store.get(base.file_id)
    assert merged.tags == {"shared", "local-only", "remote-only"}, (
        "an incoming record must merge with the local one, not replace it"
    )


def test_a_stale_replicated_record_does_not_undo_newer_local_work(
    stub, metadata_store
):
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    metadata_store.insert(base)
    stale = base.copy()

    local = metadata_store.get(base.file_id)
    local.rename("newer.txt", "node2")
    metadata_store.update(local)

    stub.ReplicateRecord(record_to_message(stale))

    assert metadata_store.get(base.file_id).name == "newer.txt"


def test_replicating_the_same_record_twice_changes_nothing(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1",
                            tags={"x"})
    message = record_to_message(record)

    stub.ReplicateRecord(message)
    first = metadata_store.get(record.file_id)
    stub.ReplicateRecord(message)
    second = metadata_store.get(record.file_id)

    assert first.tags == second.tags
    assert first.vector_clock.counters == second.vector_clock.counters


def test_a_record_for_an_unknown_file_is_stored_as_is(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node9",
                            tags={"x"})
    stub.ReplicateRecord(record_to_message(record))
    assert metadata_store.get(record.file_id).tags == {"x"}
```

- [ ] **Step 2: Add `MetadataStore.merge_remote`**

```python
    def merge_remote(self, incoming: FileRecord) -> FileRecord:
        """Merge a record received from a peer into the local copy.

        Read-merge-write happens under the store's lock, so two records arriving
        for the same file at once cannot interleave and lose one side's changes
        — the same reason local edits go through `mutate`.

        Returns the merged record, and whether the two sides were concurrent is
        reported by the caller (the servicer logs it).
        """
        with self._lock, self._conn:
            row = self._conn.execute(_SELECT_ONE, (str(incoming.file_id),)).fetchone()
            if row is None:
                self._insert_locked(incoming)
                return incoming

            merged = self._row_to_record(row).merged(incoming)
            self._write(merged)
            return merged
```

- [ ] **Step 3: Rewrite the servicer's `ReplicateRecord`** to call `merge_remote`
and log genuine conflicts. Replace the Phase 2 comment about last-write-wins:

```python
        local = self._metadata_store.get(record.file_id)
        if local is not None and local.is_concurrent_with(record):
            # Spec §9: a genuine conflict is resolved by LWW, never silently.
            # Logging it is what makes the resolution auditable in the demo.
            logger.warning(
                "concurrent update to %s from %s: local clock %s, incoming %s "
                "— resolving by last-writer-wins",
                record.file_id,
                context.peer() if context else "unknown",
                local.vector_clock.counters,
                record.vector_clock.counters,
            )
        self._metadata_store.merge_remote(record)
```

- [ ] **Step 4: Run tests, then the whole suite.**
- [ ] **Step 5: Commit** — `git commit -m "Merge replicated records instead of overwriting them"`

---

### Task 11: Convergence across a real cluster

**Files:** Create `tests/integration/cluster/test_convergence.py`

- [ ] **Step 1: Write the tests.** Build on the existing three-node fixture from
`test_metadata_replication.py` (extract it into `tests/integration/cluster/conftest.py`
so both modules share it). Cover:

- Two nodes each add a different tag to the same file while the third is
  unreachable; after both push, every reachable node shows both tags.
- A node renames while another adds a tag; both survive, and every node agrees on
  the name.
- A delete on one node and a tag edit on another: every node ends tombstoned.
- The same record pushed twice from two directions leaves state unchanged.
- **The regression that matters:** a scenario that produces *different* results
  under Phase 2's overwrite and the same result under Phase 3's merge. Assert the
  merged outcome explicitly, with a comment naming what Phase 2 would have done.

- [ ] **Step 2: Run, then the whole suite.**
- [ ] **Step 3: Commit** — `git commit -m "Add cross-node convergence tests"`

---

### Task 12: DAA note — vector clocks and conflict detection

**Files:** Create `../DAA/vector-clocks-and-conflict-detection.md`

- [ ] **Step 1: Write the note.** Cover:

- **Problem:** given two versions of a record, decide whether one descends from
  the other (take the newer, no conflict) or whether they are genuinely
  concurrent (a real conflict). Wall-clock timestamps cannot answer this: clock
  skew makes a causally *earlier* write look later.
- **Why a vector clock:** it captures the happens-before partial order directly.
  `A dominates B` iff A has seen everything B has and more. Neither dominating
  means concurrent.
- **Complexity:** O(N) per comparison and O(N) space per record, N = nodes that
  have ever written to that record. Fine at N=5; the well-known weakness is that
  the clock grows with *every* node that ever touches the record and never
  shrinks. Mention dotted version vectors / clock pruning by retired node id as
  the standard answers, deferred.
- **One clock per record, not per field** (spec §6): why this is the right
  granularity here — per-field clocks would multiply state by the number of
  fields to gain a distinction the resolution rules do not use, since the OR-Set
  merge is correct regardless of causality and the LWW rule only needs the
  detection for *logging*.
- **The subtle implementation detail worth recording:** absent components must
  read as zero, and equality must compare on observed values, so `{a:1}` and
  `{a:1, b:0}` are the same clock. Without that, every comparison against a node
  that never wrote would report "concurrent", and the conflict log would fill
  with false positives.
- **What detection is actually used for:** not resolution (LWW resolves), but
  auditability — the spec requires conflicts be visible rather than silently
  dropped. Note the honest limit: LWW still discards one of two concurrent
  writes; detection makes that visible, it does not make it lossless. Siblings
  are out of scope per spec §17.

- [ ] **Step 2: Verify it is untracked.**

---

### Task 13: Documentation and full verification

**Files:** Modify `README.md`

- [ ] **Step 1: Run the full suite.** Expect roughly 950 tests (the merge-law
properties are parametrized 200×4), 0 failures.

- [ ] **Step 2: Update the README** — status section describing Phase 3, and a
note that the metadata schema changed incompatibly, so nodes upgrading from a
Phase 2 deployment must start from an empty `ALMACEN_DATA_DIR`.

- [ ] **Step 3: Rebuild and re-verify the Compose cluster by hand.** Upload on
one node, edit tags on two others, confirm every node converges on the union.

- [ ] **Step 4: Commit.**

---

## Definition of done

- [ ] `pytest -q` passes with no failures and no skips.
- [ ] Merge is commutative, associative and idempotent over randomized histories.
- [ ] Concurrent tag edits on different nodes both survive; neither is lost.
- [ ] A causally stale record cannot undo newer local work.
- [ ] A delete outlasts a concurrent edit on another node.
- [ ] Genuine conflicts are logged with both vector clocks.
- [ ] The CRDT state survives a SQLite round-trip and a restart.
- [ ] Both DAA notes exist and are untracked.
- [ ] The repository hygiene rules from the project rules file are satisfied.
