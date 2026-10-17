"""Deletes, content replacements and GC under faults.

A delete- and PATCH-heavy mix creates many tombstones and orphaned blobs while
partitions come and go, each shorter than the grace TTL. Throughout, no node
may serve a file again once it has held it as deleted, and no blob a live
record points at may be collected. After healing and three grace TTLs every
tombstone must be purged and every orphan collected, while live files stay
readable.
"""
from tests.chaos.operations import OperationMix
from tests.chaos.scenario import ChaosRun, Profile

DELETION_HEAVY = Profile(
    mix=OperationMix(
        upload=6,
        patch_content=4,
        rename=1,
        add_tags=1,
        remove_tag=1,
        delete=5,
        download=2,
        dedup_probability=0.3,
    )
)


def test_deletes_never_resurrect_and_orphans_are_collected(
    chaos_cluster, seed, request
):
    run = ChaosRun(chaos_cluster, seed, DELETION_HEAVY)
    with run.reporting(request.node.nodeid):
        run.run_chaos()
        run.heal_and_converge()
        run.collect_everything()
