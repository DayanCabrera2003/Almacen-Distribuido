from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from almacen.storage.gc_state import BlobGCState

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def state(tmp_path: Path):
    gc_state = BlobGCState(tmp_path / "gc_state.db")
    yield gc_state
    gc_state.close()


def test_a_new_state_has_no_candidates(state: BlobGCState):
    assert state.candidates() == {}
    assert state.candidates_since(T0) == set()


def test_marked_candidates_are_reported_with_their_time(state: BlobGCState):
    state.mark_candidates({"a", "b"}, T0)

    assert state.candidates() == {"a": T0, "b": T0}


def test_re_marking_does_not_restart_the_timer(state: BlobGCState):
    # The collector re-marks every orphan every round. If that reset the clock,
    # no blob would ever reach its deadline.
    state.mark_candidates({"a"}, T0)
    state.mark_candidates({"a"}, T0 + timedelta(hours=1))

    assert state.candidates() == {"a": T0}


def test_candidates_since_is_inclusive_of_the_deadline(state: BlobGCState):
    state.mark_candidates({"old"}, T0)
    state.mark_candidates({"exact"}, T0 + timedelta(seconds=10))
    state.mark_candidates({"new"}, T0 + timedelta(seconds=11))

    assert state.candidates_since(T0 + timedelta(seconds=10)) == {"old", "exact"}


def test_clear_forgets_candidates(state: BlobGCState):
    state.mark_candidates({"a", "b"}, T0)

    state.clear({"a", "never-marked"})

    assert set(state.candidates()) == {"b"}


def test_candidacy_survives_a_restart(tmp_path: Path):
    # The whole reason this is persisted: nothing else remembers when a blob
    # lost its last referrer.
    path = tmp_path / "gc_state.db"
    first = BlobGCState(path)
    first.mark_candidates({"a"}, T0)
    first.close()

    reopened = BlobGCState(path)
    try:
        assert reopened.candidates() == {"a": T0}
    finally:
        reopened.close()
