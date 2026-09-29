# almacen/crdt/lww_register.py
"""Last-writer-wins register for single-value fields (spec §9)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class LWWRegister(Generic[T]):
    """A value plus the (timestamp, node_id) that wrote it.

    Frozen: a write produces a new register rather than mutating one, which
    keeps merge free of aliasing surprises.
    """

    value: T
    timestamp: datetime
    node_id: str

    def merged(self, other: "LWWRegister[T]") -> "LWWRegister[T]":
        """Later timestamp wins; an exact tie is broken by the higher node_id.

        The tie-break is what makes merge deterministic. Without it, two writes
        in the same clock tick would resolve differently depending on which
        replica performed the merge, and the replicas would never converge.
        """
        return self if self._rank() >= other._rank() else other

    def _rank(self) -> tuple[datetime, str]:
        return (self.timestamp, self.node_id)
