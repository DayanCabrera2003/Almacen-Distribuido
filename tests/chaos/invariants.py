"""The properties the cluster must keep, whatever the fault schedule.

`InvariantChecker` holds the ones that must hold at *every* step; the module
functions are the ones that only make sense once the network has healed.
"""
from __future__ import annotations

import hashlib
import uuid

from almacen.cluster.membership import NodeState
from almacen.cluster.placement import replica_set
from almacen.domain.file_record import FileRecord
from tests.chaos.cluster import NODE_IDS, ChaosCluster
from tests.chaos.history import History
from tests.chaos.operations import REPLICATION_FACTOR


class InvariantChecker:
    def __init__(self, cluster: ChaosCluster, history: History) -> None:
        self._cluster = cluster
        self._history = history
        # Every file each node has ever held as a tombstone.
        self._seen_tombstones: dict[str, set[uuid.UUID]] = {n: set() for n in NODE_IDS}

    def after_step(self, deleted_blobs: dict[str, set[str]] | None = None) -> None:
        records = {n: self._cluster.records(n) for n in NODE_IDS}
        self._check_no_resurrection(records)
        if deleted_blobs:
            self._check_gc_kept_referenced_blobs(records, deleted_blobs)

    def after_heal(self) -> None:
        """Right after a healing tick, every acknowledged delete is everywhere.

        This is the precondition the grace TTL relies on, observed rather than
        assumed: if it fails, a later purge could resurrect a file and the
        resurrection check below would report the symptom instead of the cause.
        """
        deleted = self._history.deleted_ids()
        for node_id in NODE_IDS:
            live = {r.file_id for r in self._cluster.records(node_id) if not r.tombstone}
            assert not live & deleted, (
                f"{node_id} still serves deleted files after healing: "
                f"{sorted(map(str, live & deleted))}"
            )

    def _check_no_resurrection(self, records: dict[str, list[FileRecord]]) -> None:
        """A node that has held a file as deleted never serves it again.

        Purging is allowed — the file is then unknown — but learning it back as
        live is the resurrection the grace TTL exists to prevent.
        """
        for node_id, node_records in records.items():
            seen = self._seen_tombstones[node_id]
            live = {r.file_id for r in node_records if not r.tombstone}
            resurrected = live & seen
            assert not resurrected, (
                f"{node_id} resurrected deleted files {sorted(map(str, resurrected))}"
            )
            seen |= {r.file_id for r in node_records if r.tombstone}

    def _check_gc_kept_referenced_blobs(
        self,
        records: dict[str, list[FileRecord]],
        deleted_blobs: dict[str, set[str]],
    ) -> None:
        """No blob a live record points at — on *any* node — was collected.

        Each node decides from its own replica only, so this is the global
        consequence of the local rule plus the grace TTL, not a restatement of
        it.
        """
        referenced = {
            r.content_hash
            for node_records in records.values()
            for r in node_records
            if not r.tombstone
        }
        for node_id, hashes in deleted_blobs.items():
            wrongly = hashes & referenced
            assert not wrongly, (
                f"{node_id} collected blobs still referenced by live records: "
                f"{sorted(h[:12] for h in wrongly)}"
            )


# ---- checks once the network is healthy ----


def canonical_state(records: list[FileRecord]) -> dict[uuid.UUID, tuple]:
    """Every replicated field of every record, in a comparable form."""
    return {
        r.file_id: (
            r.name_register,
            r.content_hash_register,
            r.tombstone_register,
            frozenset(r.tag_set.adds),
            frozenset(r.tag_set.removes),
            tuple(sorted(r.vector_clock.counters.items())),
            r.created_at,
            r.updated_at,
        )
        for r in records
    }


def metadata_converged(cluster: ChaosCluster) -> bool:
    states = [canonical_state(cluster.records(n)) for n in NODE_IDS]
    return all(state == states[0] for state in states[1:])


def assert_metadata_converged(cluster: ChaosCluster) -> None:
    reference = canonical_state(cluster.records(NODE_IDS[0]))
    for node_id in NODE_IDS[1:]:
        state = canonical_state(cluster.records(node_id))
        differing = {
            str(f)
            for f in reference.keys() | state.keys()
            if reference.get(f) != state.get(f)
        }
        assert not differing, (
            f"{node_id} disagrees with {NODE_IDS[0]} on {sorted(differing)}"
        )


def assert_matches_history(cluster: ChaosCluster, history: History) -> None:
    """The converged state is one the acknowledged operations allow."""
    records = {r.file_id: r for r in cluster.records(NODE_IDS[0])}
    live = {f for f, r in records.items() if not r.tombstone}
    expected_live = {f.file_id for f in history.live_files()}
    assert live == expected_live, (
        f"live files differ from the acknowledged history: "
        f"missing {sorted(map(str, expected_live - live))}, "
        f"unexpected {sorted(map(str, live - expected_live))}"
    )
    for truth in history.live_files():
        record = records[truth.file_id]
        label = f"f{truth.index} ({truth.file_id})"
        assert record.name in truth.names, f"{label}: name {record.name!r} was never set"
        assert record.content_hash in truth.hashes, (
            f"{label}: content {record.content_hash[:12]} was never acknowledged"
        )
        tags = record.tags
        present, absent = truth.expected_tags()
        missing = present - tags
        assert not missing, f"{label}: lost tags {sorted(missing)}"
        revived = absent & tags
        assert not revived, f"{label}: removed tags came back {sorted(revived)}"
        invented = tags - truth.ever_added_tags()
        assert not invented, f"{label}: tags never added {sorted(invented)}"


def blobs_fully_placed(cluster: ChaosCluster) -> bool:
    """Every live file's blob is on each of its R replicas."""
    for record in cluster.records(NODE_IDS[0]):
        if record.tombstone:
            continue
        for replica in replica_set(record.content_hash, list(NODE_IDS), REPLICATION_FACTOR):
            if not cluster.nodes[replica].blob_store.exists(record.content_hash):
                return False
    return True


def assert_live_content_readable_everywhere(
    cluster: ChaosCluster, history: History
) -> None:
    for truth in history.live_files():
        for node in cluster.up_nodes():
            response = node.client.get(f"/files/{truth.file_id}")
            assert response.status_code == 200, (
                f"{node.node_id} cannot serve f{truth.index}: {response.status_code}"
            )
            content_hash = hashlib.sha256(response.content).hexdigest()
            record = node.store.get(truth.file_id)
            assert content_hash == record.content_hash, (
                f"{node.node_id} served f{truth.index} with the wrong hash"
            )
            assert response.content == history.contents[content_hash]


def assert_everyone_alive(cluster: ChaosCluster) -> None:
    """Once healed, no node still believes a peer is suspect or dead.

    The failure detector may be wrong, but only transiently: any successful
    contact must correct it (architecture §10).
    """
    now = cluster.clock.now()
    for node in cluster.up_nodes():
        membership = node.app.state.membership
        wrong = {
            peer: membership.state_of(peer, now).name
            for peer in NODE_IDS
            if membership.state_of(peer, now) is not NodeState.ALIVE
        }
        assert not wrong, f"{node.node_id} still believes {wrong}"


def assert_fully_collected(cluster: ChaosCluster) -> None:
    """No tombstone left, and each node holds exactly its share of live blobs."""
    for node in cluster.up_nodes():
        records = node.store.list_all()
        tombstones = [str(r.file_id) for r in records if r.tombstone]
        assert not tombstones, f"{node.node_id} never purged {tombstones}"
        expected = {
            r.content_hash
            for r in records
            if node.node_id
            in replica_set(r.content_hash, list(NODE_IDS), REPLICATION_FACTOR)
        }
        held = node.blob_store.list_hashes()
        assert held == expected, (
            f"{node.node_id}: orphans never collected "
            f"{sorted(h[:12] for h in held - expected)}, "
            f"missing {sorted(h[:12] for h in expected - held)}"
        )
