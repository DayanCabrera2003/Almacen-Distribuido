"""Deletion and garbage collection: tombstone purge and orphan blobs (spec §10).

The decisions are pure functions of the data and the time, so every rule is
testable without a store or a clock.
"""
from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from almacen.domain.file_record import FileRecord
from almacen.storage.blob_store import BlobStore
from almacen.storage.gc_state import BlobGCState
from almacen.storage.metadata_store import MetadataStore

logger = logging.getLogger(__name__)


def records_to_purge(
    records: Sequence[FileRecord], now: datetime, grace: timedelta
) -> list[uuid.UUID]:
    """Tombstoned records whose grace period has expired.

    The TTL runs from when the delete *originated*, not from when this node
    heard about it: its purpose is to bound how long the tombstone has had to
    reach every peer, which is a property of the delete rather than of the
    observer.

    A tombstone without a time is never purged. It should not exist, and
    guessing a time for it is exactly how a file gets purged before its
    tombstone has propagated — which a peer then undoes by resurrecting it.
    """
    deadline = now - grace
    return [
        record.file_id
        for record in records
        if record.tombstone
        and record.tombstone_at is not None
        and record.tombstone_at <= deadline
    ]


@dataclass(frozen=True)
class BlobDecision:
    """What one GC round should do to the local blobs and their candidacy."""

    # Orphans seen for the first time; their clock starts now.
    newly_unreferenced: set[str]
    # Candidates to forget: they regained a referrer, or the blob is gone.
    no_longer_candidates: set[str]
    # Orphaned for at least one grace TTL; safe to delete from disk.
    deletable: set[str]


def blobs_to_delete(
    *,
    local_hashes: set[str],
    reference_counts: Mapping[str, int],
    candidates: Mapping[str, datetime],
    now: datetime,
    grace: timedelta,
) -> BlobDecision:
    """Decide which local blobs to start, stop and finish collecting.

    One mechanism covers both ways a blob is orphaned (spec §10): the record
    pointing at it is purged, or a `PATCH` repoints that record elsewhere.
    Either way its reference count reaches zero, and from then it waits one TTL.

    A blob that regains a referrer — identical bytes uploaded again, or
    anti-entropy re-teaching a record that points at it — leaves candidacy
    rather than being deleted on the old timer.
    """
    unreferenced = {h for h in local_hashes if reference_counts.get(h, 0) == 0}
    deadline = now - grace
    return BlobDecision(
        newly_unreferenced=unreferenced - candidates.keys(),
        no_longer_candidates={h for h in candidates if h not in unreferenced},
        deletable={
            h
            for h, since in candidates.items()
            if h in unreferenced and since <= deadline
        },
    )


@dataclass(frozen=True)
class GCResult:
    purged: int
    blobs_deleted: int


def collect(
    *,
    store: MetadataStore,
    blob_store: BlobStore,
    gc_state: BlobGCState,
    now: datetime,
    grace: timedelta,
    budget: int,
    should_stop: Callable[[], bool] = lambda: False,
) -> GCResult:
    """Run one GC round on this node's local state.

    Records are purged first and references recounted *afterwards*, so a blob
    orphaned by this round's purge becomes a candidate in this same round
    rather than the next one — otherwise its TTL would start a tick late.

    `budget` caps the irreversible operations (purges plus blob deletions) in
    one round, and `should_stop` is checked before each: this runs on the
    gossip thread, which must stay responsive to shutdown. Bookkeeping on the
    candidate table is not budgeted; it is one cheap transaction per round.

    Each node counts only its own records; no distributed reference count is
    attempted. That is enough because metadata is replicated to every node, so
    the local store eventually holds every referrer, and the grace TTL covers
    the window before a referrer created elsewhere arrives here. The remaining
    race — identical bytes re-uploaded between the recount and the delete —
    loses this node's copy only; the record survives, and blob catch-up
    re-fetches the content from another replica.
    """
    done = 0
    purged = 0
    for file_id in records_to_purge(store.list_all(), now, grace):
        if done >= budget or should_stop():
            return GCResult(purged=purged, blobs_deleted=0)
        if store.purge(file_id):
            purged += 1
            logger.info("purged tombstone %s", file_id)
        done += 1

    if should_stop():
        return GCResult(purged=purged, blobs_deleted=0)

    decision = blobs_to_delete(
        local_hashes=blob_store.list_hashes(),
        reference_counts=store.reference_counts(),
        candidates=gc_state.candidates(),
        now=now,
        grace=grace,
    )
    gc_state.clear(decision.no_longer_candidates)
    gc_state.mark_candidates(decision.newly_unreferenced, now)

    deleted = 0
    for content_hash in sorted(decision.deletable):
        if done >= budget or should_stop():
            break
        blob_store.delete(content_hash)
        # Forgotten one at a time, so a stop between two deletions leaves no
        # candidate pointing at a blob that is already gone.
        gc_state.clear({content_hash})
        deleted += 1
        done += 1
        logger.info("collected blob %s", content_hash[:12])

    return GCResult(purged=purged, blobs_deleted=deleted)
