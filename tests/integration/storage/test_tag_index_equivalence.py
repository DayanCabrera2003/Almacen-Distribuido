"""Property: the tag query agrees with a plain filter over decoded records.

The oracle is the original algorithm: decode every live record and evaluate the
AND/OR predicate on its OR-Set value in Python. Whatever `TagIndex.query` does
internally, it must return exactly that set of files, after any history of
concurrent tag edits, merges, deletions and purges.
"""
import random
from pathlib import Path

import pytest

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex

TAGS = ["invoice", "draft", "urgent", "2024", "x"]
PEERS = ["n1", "n2", "n3"]
LOCAL = "local"


def _oracle(store: MetadataStore, tags: list[str], mode: str) -> set:
    wanted = set(tags)
    live = store.list_live()
    if mode == "and":
        return {r.file_id for r in live if wanted <= r.tags}
    return {r.file_id for r in live if wanted & r.tags}


class _History:
    """A local store plus in-memory replicas of the same files on peer nodes."""

    def __init__(self, store: MetadataStore, rng: random.Random) -> None:
        self.store = store
        self.rng = rng
        # peer -> file_id -> that peer's copy
        self.peers: dict[str, dict] = {peer: {} for peer in PEERS}

    def step(self) -> None:
        operation = self.rng.choice(
            [
                self.create,
                self.peer_edit,
                self.peer_edit,
                self.local_edit,
                self.local_edit,
                self.push,
                self.push,
                self.pull,
                self.purge,
            ]
        )
        operation()

    def _known(self) -> list:
        ids = {r.file_id for r in self.store.list_all()}
        for replica in self.peers.values():
            ids |= replica.keys()
        return sorted(ids, key=str)

    def create(self) -> None:
        node = self.rng.choice(PEERS + [LOCAL])
        tags = set(self.rng.sample(TAGS, self.rng.randint(0, 3)))
        record = FileRecord.new(name="f", content_hash="h", node_id=node, tags=tags)
        if node == LOCAL:
            self.store.insert(record)
        else:
            self.peers[node][record.file_id] = record

    def _edit(self, record: FileRecord, node: str) -> None:
        roll = self.rng.random()
        tag = self.rng.choice(TAGS)
        if roll < 0.45:
            record.add_tag(tag, node)
        elif roll < 0.9:
            record.remove_tag(tag, node)
        else:
            record.mark_deleted(node)

    def peer_edit(self) -> None:
        node = self.rng.choice(PEERS)
        if self.peers[node]:
            file_id = self.rng.choice(sorted(self.peers[node], key=str))
            self._edit(self.peers[node][file_id], node)

    def local_edit(self) -> None:
        records = self.store.list_all()
        if records:
            file_id = self.rng.choice(records).file_id
            self.store.mutate(file_id, lambda r: self._edit(r, LOCAL))

    def push(self) -> None:
        """A peer's copy reaches the local store (replication or anti-entropy)."""
        node = self.rng.choice(PEERS)
        if self.peers[node]:
            file_id = self.rng.choice(sorted(self.peers[node], key=str))
            self.store.merge_remote(self.peers[node][file_id].copy())

    def pull(self) -> None:
        """A peer learns the local copy, then keeps editing from there."""
        node = self.rng.choice(PEERS)
        records = self.store.list_all()
        if records:
            record = self.rng.choice(records)
            current = self.peers[node].get(record.file_id)
            self.peers[node][record.file_id] = (
                current.merged(record) if current else record.copy()
            )

    def purge(self) -> None:
        tombstoned = [r for r in self.store.list_all() if r.tombstone]
        if tombstoned:
            self.store.purge(self.rng.choice(tombstoned).file_id)


def _queries(rng: random.Random) -> list[list[str]]:
    queries = [[tag] for tag in TAGS] + [TAGS[:2], TAGS[1:4], ["absent"]]
    queries += [rng.sample(TAGS, rng.randint(1, 4)) for _ in range(4)]
    # Duplicates must not change the answer (AND counts distinct tags).
    queries.append(["draft", "draft", "urgent"])
    return queries


@pytest.mark.parametrize("seed", range(40))
def test_query_matches_linear_filter_over_random_histories(tmp_path: Path, seed: int):
    rng = random.Random(seed)
    store = MetadataStore(tmp_path / "metadata.db")
    index = TagIndex(store)
    history = _History(store, rng)

    for step in range(120):
        history.step()
        if step % 10 == 9:
            for tags in _queries(rng):
                for mode in ("and", "or"):
                    got = [r.file_id for r in index.query(tags, mode=mode)]
                    assert len(got) == len(set(got)), "a file was returned twice"
                    assert set(got) == _oracle(store, tags, mode), (seed, step, tags, mode)


def test_query_returns_fully_decoded_records(tmp_path: Path):
    store = MetadataStore(tmp_path / "metadata.db")
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="n1", tags={"x", "y"})
    record.remove_tag("y", "n1")
    store.insert(record)

    [found] = TagIndex(store).query(["x"], mode="and")

    assert found.tag_set == record.tag_set
    assert found.vector_clock.counters == record.vector_clock.counters
    assert found.name == "a.txt"
