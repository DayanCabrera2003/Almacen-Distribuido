"""Rendezvous hashing (HRW) — choosing which nodes store a given blob."""
from __future__ import annotations

import hashlib
from collections.abc import Sequence


def replica_set(content_hash: str, node_ids: Sequence[str], r: int) -> list[str]:
    """Return the R nodes responsible for `content_hash`, highest score first.

    Highest Random Weight (HRW): score every node against the key and take the
    top R. Every node computes this independently from the same membership list
    and reaches the same answer, so no placement metadata has to be stored or
    agreed on.

    If the cluster is smaller than R, the whole cluster is the replica set.
    """
    if r <= 0:
        raise ValueError(f"replication factor must be positive, got {r}")
    if not node_ids:
        raise ValueError("cannot place a blob in an empty cluster")

    # Sorting on (score, node_id) makes the result independent of the order
    # node_ids arrives in, and breaks score ties deterministically, so two
    # nodes with the same membership always agree.
    ranked = sorted(
        ((_score(content_hash, node_id), node_id) for node_id in set(node_ids)),
        reverse=True,
    )
    return [node_id for _, node_id in ranked[:r]]


def _score(content_hash: str, node_id: str) -> int:
    """Weight of one (key, node) pair: a hash of the two combined.

    Hashing the pair — rather than combining separate hashes arithmetically —
    keeps the scores of a single key across nodes independent of each other,
    which is what gives HRW its even distribution.
    """
    digest = hashlib.sha256(f"{content_hash}:{node_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big")
