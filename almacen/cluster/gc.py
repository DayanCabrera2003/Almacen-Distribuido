"""Deletion and garbage collection: purging tombstones, collecting orphan blobs (spec §10).

The decisions are pure functions of the data and the time, so every rule is
testable without a store or a clock.
"""
from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from almacen.domain.file_record import FileRecord


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
