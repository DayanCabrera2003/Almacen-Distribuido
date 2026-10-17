"""Garbage collection logs what it irreversibly removed."""
from datetime import datetime, timedelta, timezone

from structlog.testing import capture_logs

from almacen.domain.file_record import FileRecord
from tests.integration.cluster.test_gc import GRACE, node  # noqa: F401
from tests.integration.cluster.test_gossip_loop import cluster, make_loop  # noqa: F401


def _events(logs, name):
    return [entry for entry in logs if entry["event"] == name]


def test_each_purge_and_blob_deletion_is_logged(node):  # noqa: F811
    record = node.upload(b"deleted content")
    deleted_at = node.delete(record)

    with capture_logs() as logs:
        node.collect(deleted_at + GRACE)
        node.collect(deleted_at + 2 * GRACE)

    [purged] = _events(logs, "tombstone_purged")
    assert purged["file_id"] == str(record.file_id)
    [collected] = _events(logs, "blob_collected")
    assert collected["content_hash"] == record.content_hash[:12]


def test_a_tick_that_collected_something_logs_a_summary(cluster):  # noqa: F811
    nodes, peers, channels = cluster
    node1 = nodes["node1"]
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    record.mark_deleted("node1")
    node1["store"].insert(record)
    later = datetime.now(timezone.utc) + 2 * timedelta(
        seconds=node1["settings"].grace_ttl_seconds
    )

    with capture_logs() as logs:
        make_loop(node1, peers, channels, targets=set(), now=later).run_once()
        # Nothing left to collect: no summary for an idle round.
        make_loop(node1, peers, channels, targets=set(), now=later).run_once()

    [summary] = _events(logs, "gc_round_completed")
    assert summary["node_id"] == "node1"
    assert summary["purged"] == 1
    assert summary["blobs_deleted"] == 0
