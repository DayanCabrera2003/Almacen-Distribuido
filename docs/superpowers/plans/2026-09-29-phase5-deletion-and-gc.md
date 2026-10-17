# Phase 5 — Deletion and Garbage Collection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the store growing forever. Tombstoned records are eventually purged, and blobs nothing references any more are eventually deleted — both only after a grace period long enough that a node coming back from a partition cannot undo the deletion.

**Architecture:** One new module, `almacen/cluster/gc.py`, holding two pure decision functions (`records_to_purge`, `blobs_to_delete`) and the collector that applies them. The storage layer gains a small table tracking when each blob became unreferenced, and the existing gossip loop gains one more step per tick. Nothing about the CRDTs, the merge or the replication paths changes.

**Tech Stack:** Unchanged.

**Spec:** §10 (deletion lifecycle, grace TTL, reference counting, both orphan paths), §16 (this is phase 5 of 6).

---

## The hazard this phase must not create

Deleting things in a system that also *resurrects* things from peers is more dangerous than it looks. Measured, before this plan was written, on the real store and the real anti-entropy diff:

```
after delete  -> A live: 0 | B live: 1      (B is partitioned, never saw it)
after purge   -> A knows the file: False
partition heals, B gossips with A
RESURRECTED ON A: True | and it is LIVE: True
A live files now: ['secret.txt']
```

A purged tombstone is **indistinguishable from a file this node has never seen**, so the partitioned peer teaches it back — not as a tombstone, but as a live file. A user's deletion silently undone is the worst failure this system can produce, and it is one careless `DELETE FROM files` away.

Everything below follows from avoiding it. The grace TTL is not a tidiness knob; it is the safety margin, and it must exceed the longest partition the deployment expects to survive.

---

## Design decisions made while planning

1. **Reference counting includes tombstoned records, not just live ones.** Spec §10 says a blob's total retention after a delete is "roughly two grace TTLs", and that only works if a tombstoned-but-not-yet-purged record still counts as a referrer: the blob becomes unreferenced when the *record* is purged, then waits its own TTL. Verified on a real node — after `DELETE`, the tombstoned record still points at its content hash, so counting over `list_all()` gives 1 and counting over `list_live()` gives 0. Using the live-only count would collapse the two TTLs into one parallel window and halve the safety margin.

2. **The `PATCH`-repoint orphan and the delete orphan share one mechanism.** After a `PATCH`, the record still exists and still counts — but it points at the *new* hash, so the old one drops to zero references immediately and waits one TTL. Verified: a `PATCH` leaves the old blob on disk with no referrer. No special case is needed; "count the referrers, start the clock at zero" covers both.

3. **The tombstone TTL runs from when the delete *originated*, not from when this node learned of it.** Spec §10's wording, and the right one: the TTL exists to bound how long the tombstone has had to propagate, which is a property of the delete, not of the observer. The cost is a dependence on the deleting node's clock — a slow clock there makes everyone purge sooner. Bounded by Phase 3's monotonic floor, which guarantees `tombstone_at` is at least the record's own creation time, so it cannot be arbitrarily far in the past.

4. **A blob's "became unreferenced at" is persisted, not recomputed.** It cannot be derived from anything else: once the last referrer is gone, nothing in the store remembers when that happened. A restart that forgot it would restart every candidate's clock, so blobs would be retained longer but never wrongly deleted — safe, but it would also mean a node that restarts often never collects anything. A two-column table is cheaper than that failure mode.

5. **A blob that regains a referrer stops being a candidate.** Content is addressed by hash, so a re-upload of identical bytes, or anti-entropy re-teaching a record that points at it, makes an existing candidate referenced again. The collector clears the candidacy rather than deleting on the old timer.

6. **GC runs on the existing gossip loop, not its own thread.** It is periodic work on local state with the same "must not monopolise the tick" constraint as blob catch-up, and it gets the same treatment: a per-round budget and a stop check. A second thread would double the shutdown surface for no benefit.

7. **OR-Set remove-set pruning is NOT in this phase** — a correction to what `DAA/crdt-selection.md` and `DAA/anti-entropy-digests.md` currently say. Pruning a remove is exactly the resurrection hazard one level down: drop `(tag, node, counter)` from the remove set while any peer still holds the matching add, and the next merge brings the tag back. Doing it safely needs to know that *every* node has observed the removal — a causal-stability barrier, which is a distinct mechanism this design does not have and which is a larger piece of work than the rest of this phase combined. Task 6 amends both notes rather than leaving them promising something that is not delivered.

---

## File Structure

```
almacen/
  cluster/gc.py                 # NEW: purge decisions + the collector
  storage/metadata_store.py     # MODIFY: purge(), reference counts
  storage/blob_store.py         # MODIFY: list_hashes(), delete()
  storage/gc_state.py           # NEW: the blob-candidate table
  config.py                     # MODIFY: grace TTL, gc budget
  cluster/gossip.py             # MODIFY: one more step per tick
tests/
  unit/cluster/test_gc_decisions.py        # NEW: the pure decisions
  integration/storage/test_gc_state.py     # NEW
  integration/cluster/test_gc.py           # NEW: the collector
  integration/cluster/test_no_resurrection.py  # NEW: the headline guard
docs/deployment.md              # MODIFY: short TTL for the demo
DAA/grace-ttl-and-resurrection.md   # NEW (untracked)
DAA/blob-reference-counting.md      # NEW (untracked)
```

---

### Task 1: Settings and storage primitives

**Files:** Modify `almacen/config.py`, `almacen/storage/blob_store.py`, `almacen/storage/metadata_store.py`; create `almacen/storage/gc_state.py`

- [x] **Step 1: Settings** — `grace_ttl_seconds` (default `86400.0`, i.e. 24h) and
`gc_budget` (default `32`), both validated positive, both with `ALMACEN_*`
variables. Tests mirroring the existing config tests, including that the default
really is 24 hours — the demo overrides it, and a default of "a few minutes"
silently shipped to a real deployment would be the bug this whole phase is
about.

- [x] **Step 2: `BlobStore.list_hashes()` and `BlobStore.delete()`**

```python
    def list_hashes(self) -> set[str]:
        """Every blob held locally.

        Skips the `.tmp-*` files `put` uses for its atomic write; treating one
        as a blob would mean trying to reference-count a name that is not a
        content hash.
        """
        return {
            path.name
            for path in self._root.iterdir()
            if path.is_file() and not path.name.startswith(".tmp-")
        }

    def delete(self, content_hash: str) -> bool:
        """Remove a blob. Returns whether it was there. Idempotent."""
        path = self._path_for(content_hash)
        if not path.exists():
            return False
        path.unlink()
        return True
```

- [x] **Step 3: `MetadataStore.purge()` and `MetadataStore.reference_counts()`**

```python
    def purge(self, file_id: uuid.UUID) -> bool:
        """Delete a record and its tag occurrences outright. Returns whether it existed.

        This is the one irreversible operation in the store. A purged record is
        indistinguishable from one this node never saw, which is why the caller
        must have checked the grace TTL first — see gc.records_to_purge.
        """
        with self._lock, self._conn:
            for table in ("file_tag_adds", "file_tag_removes"):
                self._conn.execute(
                    f"DELETE FROM {table} WHERE file_id = ?", (str(file_id),)
                )
            cursor = self._conn.execute(
                "DELETE FROM files WHERE file_id = ?", (str(file_id),)
            )
            return cursor.rowcount > 0

    def reference_counts(self) -> dict[str, int]:
        """How many records point at each content hash.

        Counts tombstoned records too: they still reference their content until
        they are purged, which is what makes a blob's retention two grace TTLs
        rather than one (spec §10).
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT content_hash, COUNT(*) FROM files GROUP BY content_hash"
            ).fetchall()
            return {content_hash: count for content_hash, count in rows}
```

- [x] **Step 4: `gc_state.py` — when each blob became unreferenced**

A tiny store of its own rather than more columns on `files`, because the row is
keyed by content hash and outlives every record that pointed at it.

```python
class BlobGCState:
    """Tracks when each orphaned blob became a candidate for deletion.

    Persisted because it cannot be recomputed: once the last referrer is gone,
    nothing else remembers when that happened.
    """

    def mark_candidates(self, hashes: set[str], now: datetime) -> None:
        """Record newly unreferenced blobs, leaving existing timers alone."""

    def clear(self, hashes: set[str]) -> None:
        """A blob regained a referrer; it is no longer a candidate."""

    def candidates_since(self, deadline: datetime) -> set[str]:
        """Blobs that became candidates at or before `deadline`."""
```

Reuse the same locking discipline as `MetadataStore`; a separate SQLite file
keeps it independent of the metadata schema.

- [x] **Step 5: Tests, then commit** — `git commit -m "Add storage primitives for purging records and collecting blobs"`

---

### Task 2: The purge decisions, as pure functions

**Files:** Create `almacen/cluster/gc.py`; test `tests/unit/cluster/test_gc_decisions.py`

Both decisions take the data and the time and return what to remove. No I/O, so
every rule below is testable in microseconds and none of these tests sleep.

- [x] **Step 1: Write the failing tests.** Cover at minimum:

- A live record is never purged, however old.
- A tombstoned record younger than the TTL is not purged.
- A tombstoned record older than the TTL is purged.
- A record tombstoned exactly at the deadline is purged (boundary is `>=`).
- A tombstone with no `tombstone_at` is never purged — it should be impossible,
  and guessing a time for it is how a file gets purged early.
- `blobs_to_delete` returns nothing while a blob has referrers, even if it was
  once a candidate.
- A blob unreferenced for longer than the TTL is returned.
- A blob unreferenced for less than the TTL is not.
- A blob held locally that no record ever referenced is still a candidate — it
  is orphaned by definition.

- [x] **Step 2: Implement**

```python
def records_to_purge(
    records: Sequence[FileRecord], now: datetime, grace: timedelta
) -> list[uuid.UUID]:
    """Tombstoned records whose grace period has expired.

    The TTL runs from when the delete *originated*, not from when this node
    heard about it: its purpose is to bound how long the tombstone has had to
    reach every peer, which is a property of the delete rather than of the
    observer.
    """
    deadline = now - grace
    return [
        record.file_id
        for record in records
        if record.tombstone
        and record.tombstone_at is not None
        and record.tombstone_at <= deadline
    ]
```

and the blob half, which takes the local hashes, the reference counts and the
candidate timestamps, and returns `(newly_unreferenced, newly_referenced_again,
deletable)` so the collector can update state and delete in one pass.

- [x] **Step 3: Run tests, commit**

---

### Task 3: The collector

**Files:** Modify `almacen/cluster/gc.py`; test `tests/integration/cluster/test_gc.py`

- [x] **Step 1: Write the failing tests.** A real store, blob store and GC state.
Cover: a tombstone past its TTL is purged and the record is gone; the blob it
pointed at becomes a candidate at that moment and is *not* deleted yet; after a
second TTL it is deleted; a `PATCH`-orphaned blob becomes a candidate
immediately and is deleted after one TTL; a blob that regains a referrer is
cleared and survives; the budget caps one round; a stop signal interrupts it.

- [x] **Step 2: Implement `collect()`**, applying the decisions in this order —
purge records first, *then* recount references, so a blob orphaned by this
round's purge becomes a candidate in the same round rather than the next one.

- [x] **Step 3: Run tests, commit**

---

### Task 4: No resurrection — the guard this phase needs most

**Files:** Create `tests/integration/cluster/test_no_resurrection.py`

- [x] **Step 1: Write the tests**, using the real anti-entropy path rather than
calling merge directly, because the hazard lives in the interaction:

- A tombstone purged **after** the TTL, with the peer having already seen it,
  stays deleted on both sides after gossip.
- A tombstone purged **before** the peer has seen it — forced by calling
  `purge()` directly, bypassing the TTL — resurrects the file. **Assert the
  resurrection.** This is the one test in the suite that asserts a bug exists:
  it documents precisely what the grace TTL is buying, and it fails the day
  someone "optimises" the TTL away, which is exactly when the reminder is
  needed.
- With the TTL respected, gossip after the purge leaves both sides with nothing.

- [x] **Step 2: Run tests, commit**

---

### Task 5: Wire GC into the loop

**Files:** Modify `almacen/cluster/gossip.py`, `almacen/main.py`

- [x] **Step 1: Add a `collect` step to `GossipLoop.run_once`**, after blob
catch-up, with the same budget-and-stop-check treatment and the same
`except Exception` backstop — nothing may escape the thread.

Order matters: catch-up first, then collect. Running collect first would let a
node delete a blob in the same tick it was about to fetch a record that
references it.

- [x] **Step 2: Construct the collector in `create_app`** and give the loop the
`BlobGCState`. GC must be disabled along with gossip when
`gossip_interval_seconds <= 0`, so the existing fixtures are unaffected.

- [x] **Step 3: Run the whole suite** — nothing should change, since the default
TTL is 24 hours and no test runs that long.

- [x] **Step 4: Commit**

---

### Task 6: DAA notes, and two corrections

**Files:** Create `../DAA/grace-ttl-and-resurrection.md` and
`../DAA/blob-reference-counting.md`; **amend** `../DAA/crdt-selection.md` and
`../DAA/anti-entropy-digests.md`

- [ ] **Step 1: `grace-ttl-and-resurrection.md`.** The problem (a purged
tombstone is indistinguishable from an unseen file), the measured resurrection,
why the TTL must exceed the worst expected partition, why it runs from origin
time, and the cost of getting it wrong in each direction — too short resurrects
deleted data, too long means unbounded growth. Include the measured transcript.

- [ ] **Step 2: `blob-reference-counting.md`.** The two orphan paths and why one
mechanism covers both; why tombstoned records count as referrers and what
halving that would cost; the two-TTL retention arithmetic; complexity
(`O(records + blobs)` per round, and why a maintained counter would be worse
here than a recount); and why a distributed reference count is *not* attempted —
each node counts only its own records, which is safe because placement is
deterministic.

- [ ] **Step 3: Correct the two existing notes.** Both currently say OR-Set
remove-set pruning "belongs in Phase 5". It does not, and decision 7 above
explains why. Amend them to say so rather than leaving a promise the code does
not keep.

---

### Task 7: Demo and documentation

**Files:** Modify `docs/deployment.md`, `README.md`

- [ ] **Step 1: Short TTL in the demo deployment** — add
`-e ALMACEN_GRACE_TTL_SECONDS=20` to the `docker run` commands in
`docs/deployment.md` and the README, so a delete, a purge and a blob collection
are all observable in a demo session. State there that this is a demo value and
that 24h is the default for a reason.

- [ ] **Step 2: Verify by hand** — upload, delete, confirm the blob is still on
disk, wait past two TTLs, confirm the record and then the blob are gone.

- [ ] **Step 3: README** — Phase 5 status, the new variables, the demo, and an
explicit warning that lowering the TTL below the worst expected partition can
resurrect deleted files.

- [ ] **Step 4: Commit**

---

### Task 8: Full verification

- [ ] `pytest -q` — no failures, no skips.
- [ ] No test sleeps (the one `time.sleep(0)` in the membership test is a GIL
      yield, not a wait).
- [ ] The resurrection test fails if the grace check in `records_to_purge` is
      removed.
- [ ] The repository hygiene check from the project rules file.

---

## Definition of done

- [ ] A tombstoned record older than the grace TTL is purged; a younger one is not.
- [ ] A blob orphaned by a purge is deleted one TTL after the purge, not before.
- [ ] A blob orphaned by a `PATCH` repoint is deleted one TTL after the repoint.
- [ ] A blob that regains a referrer is not deleted.
- [ ] Purging within the TTL is shown to resurrect a file, and purging after it is shown not to.
- [ ] GC runs on the gossip loop, is budgeted, and leaks no threads.
- [ ] Both new DAA notes exist, and the two stale claims in the existing notes are corrected.
- [ ] The hygiene rules from the project rules file are satisfied.
