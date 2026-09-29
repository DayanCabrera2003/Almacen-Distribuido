"""Domain entity for a stored file, built on CRDTs so replicas converge."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from almacen.crdt.lww_register import LWWRegister
from almacen.crdt.or_set import OrSet
from almacen.crdt.vector_clock import VectorClock


# Smallest step that keeps two consecutive writes distinguishable. datetime
# resolves to microseconds, so this is the finest gap that survives a round-trip
# through ISO-8601 and SQLite.
_TIMESTAMP_STEP = timedelta(microseconds=1)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class FileRecord:
    """A file's identity and metadata, mergeable across replicas.

    The CRDT state lives in the `*_register` / `tag_set` / `vector_clock`
    fields; everything the rest of the system reads is exposed as a plain value
    through a property, so the API and storage layers never handle a CRDT
    directly.

    Every mutation takes the id of the node performing it: a CRDT operation is
    attributed, and the record itself has no home node — it travels.
    """

    file_id: uuid.UUID
    name_register: LWWRegister[str]
    content_hash_register: LWWRegister[str]
    tombstone_register: LWWRegister[bool]
    tag_set: OrSet
    vector_clock: VectorClock
    created_at: datetime
    # Monotonic per record (see `_touch`), so merging is plain max — no
    # tie-break needed. Kept separate from the registers because tag edits must
    # bump it and the OR-Set carries no timestamps of its own.
    updated_at: datetime = field(default_factory=_utcnow)

    # ---- read interface: unchanged from Phase 2 ----

    @property
    def name(self) -> str:
        return self.name_register.value

    @property
    def content_hash(self) -> str:
        return self.content_hash_register.value

    @property
    def tombstone(self) -> bool:
        return self.tombstone_register.value

    @property
    def tombstone_at(self) -> datetime | None:
        return self.tombstone_register.timestamp if self.tombstone else None

    @property
    def tags(self) -> set[str]:
        return self.tag_set.value()

    # ---- construction ----

    @classmethod
    def new(
        cls,
        name: str,
        content_hash: str,
        node_id: str,
        tags: set[str] | None = None,
    ) -> "FileRecord":
        now = _utcnow()
        clock = VectorClock()
        counter = clock.increment(node_id)

        tag_set = OrSet()
        for tag in tags or set():
            tag_set.add(tag, node_id, counter)

        return cls(
            file_id=uuid.uuid4(),
            name_register=LWWRegister(name, now, node_id),
            content_hash_register=LWWRegister(content_hash, now, node_id),
            tombstone_register=LWWRegister(False, now, node_id),
            tag_set=tag_set,
            vector_clock=clock,
            created_at=now,
            updated_at=now,
        )

    # ---- mutations ----

    def rename(self, name: str, node_id: str) -> None:
        self._touch(node_id)
        self.name_register = LWWRegister(name, self.updated_at, node_id)

    def update_content(self, content_hash: str, node_id: str) -> None:
        self._touch(node_id)
        self.content_hash_register = LWWRegister(content_hash, self.updated_at, node_id)

    def add_tag(self, tag: str, node_id: str) -> None:
        counter = self._touch(node_id)
        # The vector clock is the single counter source (spec §6): the same
        # value identifies this mutation and tags this occurrence, so two adds
        # of the same tag from the same node stay distinguishable.
        self.tag_set.add(tag, node_id, counter)

    def remove_tag(self, tag: str, node_id: str) -> None:
        self._touch(node_id)
        self.tag_set.remove(tag)

    def mark_deleted(self, node_id: str) -> None:
        self._touch(node_id)
        self.tombstone_register = LWWRegister(True, self.updated_at, node_id)

    def _touch(self, node_id: str) -> int:
        """Stamp one mutation by `node_id`; returns its counter.

        The timestamp is a *hybrid logical* one: wall-clock time, but never at
        or before the newest timestamp this record already carries. Taking the
        bare wall clock breaks three things, silently, because the
        last-writer-wins rank would no longer be strictly increasing per record:

        - two writes by the same node inside one clock tick get identical
          ranks, and `merged` stops being commutative — the two nodes that see
          them in opposite orders disagree forever;
        - a delete issued on a node whose clock lags loses to the record's own
          earlier state, and the file is resurrected (spec §6 claims delete
          always wins, which only holds if clocks are monotone across nodes —
          this is what makes that claim true here);
        - a write made *after* observing a future-dated remote write loses to
          it, so the user's change vanishes with no error.

        Deriving the floor from the record rather than from the machine makes
        the rank monotonic along each record's own causal history, which is the
        property last-writer-wins actually needs. The cost is that one peer with
        a badly fast clock drags a record's timestamps forward and they stay
        there; bounding that drift is what a full hybrid logical clock adds, and
        is not needed at this scale.
        """
        self.updated_at = max(_utcnow(), self._newest_timestamp() + _TIMESTAMP_STEP)
        return self.vector_clock.increment(node_id)

    def _newest_timestamp(self) -> datetime:
        """The latest instant this record already carries anywhere."""
        return max(
            self.updated_at,
            self.name_register.timestamp,
            self.content_hash_register.timestamp,
            self.tombstone_register.timestamp,
        )

    # ---- replication ----

    def merged(self, other: "FileRecord") -> "FileRecord":
        """Combine two copies of the same file. Neither input is modified.

        Field-wise: LWW for the single-value fields, union for the tags,
        component-wise max for the clock. Commutative, associative and
        idempotent, which is what lets updates arrive in any order, more than
        once, and still converge.
        """
        if self.file_id != other.file_id:
            raise ValueError(
                f"cannot merge different files: {self.file_id} and {other.file_id}"
            )
        return FileRecord(
            file_id=self.file_id,
            name_register=self.name_register.merged(other.name_register),
            content_hash_register=self.content_hash_register.merged(
                other.content_hash_register
            ),
            tombstone_register=self.tombstone_register.merged(other.tombstone_register),
            tag_set=self.tag_set.merged(other.tag_set),
            vector_clock=self.vector_clock.merged(other.vector_clock),
            # Creation is immutable; the earlier claim is the true one.
            created_at=min(self.created_at, other.created_at),
            updated_at=max(self.updated_at, other.updated_at),
        )

    def is_concurrent_with(self, other: "FileRecord") -> bool:
        """Whether neither copy has seen everything the other has (spec §6)."""
        return self.vector_clock.concurrent_with(other.vector_clock)

    def copy(self) -> "FileRecord":
        # The registers are frozen, so sharing them is safe; only the mutable
        # OrSet and VectorClock need duplicating.
        return FileRecord(
            file_id=self.file_id,
            name_register=self.name_register,
            content_hash_register=self.content_hash_register,
            tombstone_register=self.tombstone_register,
            tag_set=self.tag_set.copy(),
            vector_clock=VectorClock(dict(self.vector_clock.counters)),
            created_at=self.created_at,
            updated_at=self.updated_at,
        )
