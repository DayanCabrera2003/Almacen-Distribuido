# almacen/crdt/vector_clock.py
"""Causality tracking: which of two record versions happened first."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class VectorClock:
    """One counter per node, tracking what this replica has observed.

    Used to tell a *causally stale* update (safe to discard) from a *genuinely
    concurrent* one (a real conflict that needs a resolution rule). Absent
    components read as zero, so a node that has never written is simply behind,
    not incomparable.
    """

    counters: dict[str, int] = field(default_factory=dict)

    def get(self, node_id: str) -> int:
        return self.counters.get(node_id, 0)

    def increment(self, node_id: str) -> int:
        """Record one local mutation by `node_id`; returns the new counter."""
        self.counters[node_id] = self.get(node_id) + 1
        return self.counters[node_id]

    def dominates(self, other: "VectorClock") -> bool:
        """Whether this clock has seen strictly more than `other`."""
        nodes = set(self.counters) | set(other.counters)
        return all(self.get(n) >= other.get(n) for n in nodes) and self != other

    def concurrent_with(self, other: "VectorClock") -> bool:
        """Whether neither clock has seen everything the other has."""
        return (
            self != other and not self.dominates(other) and not other.dominates(self)
        )

    def merged(self, other: "VectorClock") -> "VectorClock":
        """Component-wise maximum. Neither input is modified."""
        nodes = set(self.counters) | set(other.counters)
        return VectorClock({n: max(self.get(n), other.get(n)) for n in nodes})

    def __eq__(self, other: object) -> bool:
        # Compare on observed values, so {"a": 1} equals {"a": 1, "b": 0}.
        # Without this, any comparison against a node that never wrote would
        # report "concurrent" and flood the conflict log with false positives.
        if not isinstance(other, VectorClock):
            return NotImplemented
        nodes = set(self.counters) | set(other.counters)
        return all(self.get(n) == other.get(n) for n in nodes)
