import pytest

from almacen.cluster.placement import replica_set

NODES = ["node1", "node2", "node3", "node4", "node5"]


def test_returns_exactly_r_nodes():
    result = replica_set("abc123", NODES, r=3)
    assert len(result) == 3
    assert len(set(result)) == 3, "replica set must not contain duplicates"
    assert set(result) <= set(NODES)


def test_is_deterministic():
    first = replica_set("abc123", NODES, r=3)
    second = replica_set("abc123", NODES, r=3)
    assert first == second


def test_is_independent_of_node_list_order():
    # Two nodes handed the same membership in a different order must agree on
    # the replica set, or a blob would be written to one set and read from
    # another.
    forward = replica_set("abc123", NODES, r=3)
    backward = replica_set("abc123", list(reversed(NODES)), r=3)
    assert forward == backward


def test_returns_all_nodes_when_cluster_smaller_than_r():
    assert replica_set("abc123", ["node1"], r=3) == ["node1"]
    assert sorted(replica_set("abc123", ["node1", "node2"], r=3)) == ["node1", "node2"]


def test_rejects_empty_cluster():
    with pytest.raises(ValueError):
        replica_set("abc123", [], r=3)


def test_rejects_non_positive_r():
    with pytest.raises(ValueError):
        replica_set("abc123", NODES, r=0)


def test_removing_a_node_only_moves_keys_it_was_hosting():
    # This is the whole point of HRW over `hash(key) % N`: losing one node must
    # not reshuffle the placement of keys that node was not part of.
    hashes = [f"hash-{i}" for i in range(500)]
    before = {h: replica_set(h, NODES, r=3) for h in hashes}

    survivors = [n for n in NODES if n != "node3"]
    after = {h: replica_set(h, survivors, r=3) for h in hashes}

    for h in hashes:
        if "node3" not in before[h]:
            assert after[h] == before[h], (
                f"placement of {h} changed although node3 never hosted it"
            )
        else:
            # It lost one replica and gained exactly one replacement; the two
            # replicas it kept stay put.
            kept = [n for n in before[h] if n != "node3"]
            assert set(kept) <= set(after[h])


def test_distributes_keys_roughly_evenly():
    hashes = [f"hash-{i}" for i in range(3000)]
    counts = dict.fromkeys(NODES, 0)
    for h in hashes:
        for node in replica_set(h, NODES, r=3):
            counts[node] += 1

    expected = 3000 * 3 / len(NODES)  # 1800 placements per node
    for node, count in counts.items():
        assert abs(count - expected) < expected * 0.15, (
            f"{node} got {count} placements, expected ~{expected}"
        )
