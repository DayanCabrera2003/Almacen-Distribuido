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
