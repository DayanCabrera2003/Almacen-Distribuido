# almacen/crdt/or_set.py
"""Observed-Remove Set: the CRDT behind a file's tags (spec §9)."""
from __future__ import annotations

from dataclasses import dataclass, field

# One occurrence of an element: (element, node_id, counter). The node_id and
# counter make otherwise-identical adds distinguishable, which is what lets a
# remove cancel exactly the adds it observed and no others.
Element = tuple[str, str, int]


@dataclass
class OrSet:
    """A set where a concurrent add beats a concurrent remove.

    Each `add` records a uniquely tagged occurrence. `remove` cancels every
    occurrence *currently observed*, so an add that this replica has not yet
    seen survives the merge — which is the behaviour that makes tag edits on
    partitioned nodes converge without losing work.
    """

    adds: set[Element] = field(default_factory=set)
    removes: set[Element] = field(default_factory=set)

    def add(self, element: str, node_id: str, counter: int) -> None:
        self.adds.add((element, node_id, counter))

    def remove(self, element: str) -> None:
        """Cancel every observed occurrence of `element`.

        Unobserved occurrences — including ones added concurrently elsewhere —
        are untouched, so they survive the merge.
        """
        self.removes |= {occ for occ in self.adds if occ[0] == element}

    def value(self) -> set[str]:
        """The elements currently in the set."""
        return {occ[0] for occ in self.adds - self.removes}

    def merged(self, other: "OrSet") -> "OrSet":
        """Union of both sides' adds and removes. Neither input is modified."""
        return OrSet(self.adds | other.adds, self.removes | other.removes)

    def copy(self) -> "OrSet":
        return OrSet(set(self.adds), set(self.removes))
