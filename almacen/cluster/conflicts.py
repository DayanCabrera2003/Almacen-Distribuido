# almacen/cluster/conflicts.py
"""Surfacing concurrent updates before they are merged (spec §9)."""
from __future__ import annotations

from almacen.domain.file_record import FileRecord


def log_if_concurrent(
    log, local: FileRecord | None, incoming: FileRecord, *, sender: str
) -> None:
    """Warn when `incoming` is concurrent with the local copy.

    A genuine conflict is resolved by last-writer-wins, but never silently.
    Logging both clocks is what makes the resolution auditable — LWW discards
    one of two concurrent writes, and that loss should be visible rather than
    inferred. Called on every path a remote record is merged through, so the
    log does not depend on which half of a gossip round carried the record.
    """
    if local is None or not local.is_concurrent_with(incoming):
        return
    log.warning(
        "concurrent_update_resolved",
        file_id=str(incoming.file_id),
        sender=sender,
        local_clock=dict(local.vector_clock.counters),
        incoming_clock=dict(incoming.vector_clock.counters),
        resolution="last_writer_wins",
    )
