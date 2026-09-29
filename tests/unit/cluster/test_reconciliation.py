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
    stale = local.copy()
    local.rename("newer.txt", "node1")

    plan = plan_exchange([local], {local.file_id: stale.vector_clock})

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


def test_wanted_never_contains_duplicates():
    local = record()
    elsewhere = local.copy()
    local.rename("here", "node1")
    elsewhere.rename("there", "node2")

    plan = plan_exchange([local], {local.file_id: elsewhere.vector_clock})

    assert len(plan.wanted) == len(set(plan.wanted))


def test_the_plan_is_empty_between_two_empty_nodes():
    plan = plan_exchange([], {})
    assert plan.push == [] and plan.wanted == []
