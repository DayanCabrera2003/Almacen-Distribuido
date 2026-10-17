"""GET /cluster/status: this node's view of the cluster (spec §12)."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from almacen.api.deps import (
    get_blob_store,
    get_local_metadata_store,
    get_membership,
    get_settings,
)
from almacen.api.schemas import (
    ClusterStatus,
    GCSettings,
    GossipSettings,
    MemberStatus,
    ReplicationSettings,
    StorageCounts,
)
from almacen.cluster.membership import Membership, NodeStatus
from almacen.config import Settings
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore

router = APIRouter(prefix="/cluster", tags=["cluster"])

# The `since` of a peer that has been in the view from the start but was never
# observed changing state.
_NEVER = datetime.min.replace(tzinfo=timezone.utc)


# A plain `def`, not `async def`: the store and the blob directory are blocking
# I/O, so FastAPI runs this on its thread pool instead of the event loop.
@router.get("/status", response_model=ClusterStatus)
def cluster_status(
    settings: Settings = Depends(get_settings),
    membership: Membership = Depends(get_membership),
    metadata_store: MetadataStore = Depends(get_local_metadata_store),
    blob_store: BlobStore = Depends(get_blob_store),
) -> ClusterStatus:
    # One call, one lock acquisition: the membership part of the answer is a
    # consistent cut even while the gossip thread is merging into it. The
    # counts below are read separately, so they are each exact but not taken
    # at the same instant as the membership — fine for a status page.
    view = membership.snapshot(datetime.now(timezone.utc))
    members = [
        _member(node_id, status, settings) for node_id, status in sorted(view.items())
    ]

    live, tombstones = metadata_store.record_counts()
    return ClusterStatus(
        node_id=settings.node_id,
        members=members,
        replication=ReplicationSettings(
            replication_factor=settings.replication_factor,
            write_quorum=settings.write_quorum,
            rpc_timeout_seconds=settings.rpc_timeout_seconds,
        ),
        gossip=GossipSettings(
            enabled=settings.gossip_enabled,
            interval_seconds=settings.gossip_interval_seconds,
            fanout=settings.gossip_fanout,
            suspicion_timeout_seconds=settings.suspicion_timeout_seconds,
            blob_catchup_budget=settings.blob_catchup_budget,
        ),
        gc=GCSettings(
            grace_ttl_seconds=settings.grace_ttl_seconds,
            budget=settings.gc_budget,
        ),
        storage=StorageCounts(
            live_records=live,
            tombstones=tombstones,
            blobs=len(blob_store.list_hashes()),
        ),
    )


def _member(node_id: str, status: NodeStatus, settings: Settings) -> MemberStatus:
    is_self = node_id == settings.node_id
    # A node's own `since` is stamped "now" by every snapshot, so it carries no
    # information; neither does the placeholder of a peer never seen to change.
    since = None if is_self or status.since == _NEVER else status.since
    return MemberStatus(
        node_id=node_id,
        address=settings.peer_address(node_id),
        state=status.state.name.lower(),
        incarnation=status.incarnation,
        since=since,
        is_self=is_self,
    )
