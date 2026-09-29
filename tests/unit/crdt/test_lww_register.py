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
