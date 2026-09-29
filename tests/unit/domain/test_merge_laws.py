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


def _three_divergent_replicas(seed: int):
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
    versions = list(_three_divergent_replicas(seed))
    rng = random.Random(seed)

    # Every node ends up with all three versions, but receives them in its own
    # order and via its own path.
    finals = []
    for _ in NODES:
        shuffled = versions[:]
        rng.shuffle(shuffled)
        merged = shuffled[0]
        for other in shuffled[1:]:
            merged = merged.merged(other)
        finals.append(_observable(merged))

    assert len(set(finals)) == 1, "replicas that saw the same updates disagree"


# The randomized histories above never produce two writes with the *same*
# timestamp, because the clock has microsecond resolution — so they do not
# exercise the LWW tie-break at all. Verified by deleting the tie-break: all 800
# cases above still passed. Two nodes writing inside the same clock tick is a
# real scenario in a fast cluster, so it gets its own property.
@pytest.mark.parametrize("seed", range(TRIALS))
def test_writes_in_the_same_clock_tick_still_converge(seed: int):
    from almacen.crdt.lww_register import LWWRegister

    rng = random.Random(seed)
    base = FileRecord.new(name="base.txt", content_hash="h0", node_id="node1")

    # Force every replica's write onto one shared instant.
    instant = base.created_at
    replicas = []
    for node_id in NODES:
        replica = base.copy()
        replica.name_register = LWWRegister(f"from-{node_id}", instant, node_id)
        replica.content_hash_register = LWWRegister(
            f"hash-from-{node_id}", instant, node_id
        )
        replica.vector_clock.increment(node_id)
        replicas.append(replica)

    finals = []
    for _ in NODES:
        shuffled = replicas[:]
        rng.shuffle(shuffled)
        merged = shuffled[0]
        for other in shuffled[1:]:
            merged = merged.merged(other)
        finals.append((merged.name, merged.content_hash))

    assert len(set(finals)) == 1, (
        "replicas writing in the same tick disagree: the last-writer-wins "
        "tie-break is missing or not deterministic"
    )
