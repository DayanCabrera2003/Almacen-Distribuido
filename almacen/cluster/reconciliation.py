# almacen/cluster/reconciliation.py
"""Push-pull anti-entropy: deciding what two nodes owe each other (spec §11)."""
from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field

from almacen.crdt.vector_clock import VectorClock
from almacen.domain.file_record import FileRecord

Digest = dict[uuid.UUID, VectorClock]


@dataclass
class ExchangePlan:
    """One side's half of an exchange."""

    push: list[FileRecord] = field(default_factory=list)
    wanted: list[uuid.UUID] = field(default_factory=list)


def build_digest(records: Sequence[FileRecord]) -> Digest:
    """What a node advertises: one vector clock per file, nothing else.

    A clock is a few integers, so a digest stays small even when the records
    themselves are not — which is the entire point of exchanging digests first
    rather than shipping state and letting the receiver sort it out.
    """
    return {record.file_id: record.vector_clock for record in records}


def plan_exchange(local_records: Sequence[FileRecord], remote: Digest) -> ExchangePlan:
    """Compare local state against a peer's digest.

    `local_records` is walked twice, so it must be a sequence rather than a
    generator.

    Tombstoned records are included deliberately: a tombstone is the update that
    most needs to travel, since a peer that never receives it keeps serving a
    file everyone else considers deleted.
    """
    plan = ExchangePlan()

    for record in local_records:
        their_clock = remote.get(record.file_id)
        if their_clock is None:
            plan.push.append(record)  # they have never seen this file
            continue
        if record.vector_clock == their_clock:
            continue  # identical histories; the common case in steady state
        if record.vector_clock.dominates(their_clock):
            plan.push.append(record)
        elif their_clock.dominates(record.vector_clock):
            plan.wanted.append(record.file_id)
        else:
            # Concurrent: neither version supersedes the other, so both must
            # travel and the CRDT merge decides.
            plan.push.append(record)
            plan.wanted.append(record.file_id)

    known = {record.file_id for record in local_records}
    plan.wanted.extend(file_id for file_id in remote if file_id not in known)
    return plan
