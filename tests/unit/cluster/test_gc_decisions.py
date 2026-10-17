"""The two GC decisions, as pure functions of the data and the time."""
from datetime import datetime, timedelta, timezone

from almacen.cluster.gc import blobs_to_delete, records_to_purge
from almacen.crdt.lww_register import LWWRegister
from almacen.domain.file_record import FileRecord

NOW = datetime(2026, 1, 2, tzinfo=timezone.utc)
GRACE = timedelta(hours=24)
DEADLINE = NOW - GRACE


def live() -> FileRecord:
    return FileRecord.new(name="a.txt", content_hash="h", node_id="node1")


def tombstoned_at(when: datetime) -> FileRecord:
    record = live()
    record.tombstone_register = LWWRegister(True, when, "node1")
    return record


# ---- records_to_purge ----


def test_a_live_record_is_never_purged_however_old():
    record = live()
    # Created long before the deadline; liveness alone must protect it.
    record.created_at = NOW - timedelta(days=365)

    assert records_to_purge([record], NOW, GRACE) == []


def test_a_tombstone_younger_than_the_ttl_is_kept():
    record = tombstoned_at(DEADLINE + timedelta(seconds=1))
    assert records_to_purge([record], NOW, GRACE) == []


def test_a_tombstone_older_than_the_ttl_is_purged():
    record = tombstoned_at(DEADLINE - timedelta(seconds=1))
    assert records_to_purge([record], NOW, GRACE) == [record.file_id]


def test_a_tombstone_exactly_at_the_deadline_is_purged():
    record = tombstoned_at(DEADLINE)
    assert records_to_purge([record], NOW, GRACE) == [record.file_id]


def test_a_tombstone_without_a_time_is_never_purged():
    # Should be impossible. Guessing a time for it is how a file gets purged
    # before its tombstone has had a chance to propagate.
    record = tombstoned_at(DEADLINE - timedelta(days=30))
    record.tombstone_register = LWWRegister(True, None, "node1")  # type: ignore[arg-type]

    assert records_to_purge([record], NOW, GRACE) == []


def test_only_expired_tombstones_are_selected_from_a_mix():
    expired = tombstoned_at(DEADLINE - timedelta(hours=1))
    fresh = tombstoned_at(NOW)
    alive = live()

    assert records_to_purge([alive, expired, fresh], NOW, GRACE) == [expired.file_id]


# ---- blobs_to_delete ----


def test_a_referenced_blob_is_never_deleted_even_if_it_was_a_candidate():
    decision = blobs_to_delete(
        local_hashes={"h"},
        reference_counts={"h": 1},
        candidates={"h": DEADLINE - timedelta(days=1)},
        now=NOW,
        grace=GRACE,
    )

    assert decision.deletable == set()
    # And its candidacy is withdrawn, so a later orphaning starts a fresh clock.
    assert decision.no_longer_candidates == {"h"}


def test_a_blob_unreferenced_for_longer_than_the_ttl_is_deletable():
    decision = blobs_to_delete(
        local_hashes={"h"},
        reference_counts={},
        candidates={"h": DEADLINE - timedelta(seconds=1)},
        now=NOW,
        grace=GRACE,
    )
    assert decision.deletable == {"h"}


def test_a_blob_unreferenced_exactly_at_the_deadline_is_deletable():
    decision = blobs_to_delete(
        local_hashes={"h"}, reference_counts={}, candidates={"h": DEADLINE},
        now=NOW, grace=GRACE,
    )
    assert decision.deletable == {"h"}


def test_a_blob_unreferenced_for_less_than_the_ttl_is_kept():
    decision = blobs_to_delete(
        local_hashes={"h"},
        reference_counts={},
        candidates={"h": DEADLINE + timedelta(seconds=1)},
        now=NOW,
        grace=GRACE,
    )
    assert decision.deletable == set()
    assert decision.newly_unreferenced == set()


def test_an_orphan_seen_for_the_first_time_becomes_a_candidate_not_a_deletion():
    decision = blobs_to_delete(
        local_hashes={"h"}, reference_counts={"other": 2}, candidates={},
        now=NOW, grace=GRACE,
    )
    # Nothing tells us how long it has been orphaned, so the clock starts now.
    assert decision.newly_unreferenced == {"h"}
    assert decision.deletable == set()


def test_a_blob_no_record_ever_referenced_is_still_a_candidate():
    # Orphaned by definition: e.g. a write whose metadata never landed.
    decision = blobs_to_delete(
        local_hashes={"stray"}, reference_counts={}, candidates={},
        now=NOW, grace=GRACE,
    )
    assert decision.newly_unreferenced == {"stray"}


def test_a_candidate_whose_blob_is_already_gone_is_forgotten():
    decision = blobs_to_delete(
        local_hashes=set(), reference_counts={}, candidates={"gone": DEADLINE},
        now=NOW, grace=GRACE,
    )
    assert decision.deletable == set()
    assert decision.no_longer_candidates == {"gone"}


def test_a_reference_to_a_blob_not_held_locally_is_ignored():
    # This node is not a replica for that content; nothing to collect or track.
    decision = blobs_to_delete(
        local_hashes=set(), reference_counts={"elsewhere": 1}, candidates={},
        now=NOW, grace=GRACE,
    )
    assert decision.newly_unreferenced == set()
    assert decision.no_longer_candidates == set()
    assert decision.deletable == set()
