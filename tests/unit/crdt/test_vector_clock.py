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
