# almacen/cluster/gossip.py
"""One push-pull gossip round, and the loop that drives it (spec §11)."""
from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from datetime import datetime

import grpc

from almacen.cluster.channels import ChannelPool
from almacen.cluster.membership import Membership
from almacen.cluster.placement import replica_set
from almacen.cluster.reconciliation import build_digest
from almacen.cluster.replication_client import ReplicationClient
from almacen.config import Peer, Settings
from almacen.rpc import cluster_pb2 as pb
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.gossip_codec import (
    decode_membership,
    encode_digest,
    encode_membership,
)
from almacen.rpc.record_codec import message_to_record, record_to_message
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore

logger = logging.getLogger(__name__)


def gossip_once(
    peer: Peer,
    *,
    settings: Settings,
    store: MetadataStore,
    membership: Membership,
    channels: ChannelPool,
    now: datetime,
) -> bool:
    """Run one push-pull exchange with `peer`. Returns whether it succeeded.

    Failure is reported, never raised. An unreachable peer is the normal case
    this phase exists to handle, and the caller is a background thread: an
    exception escaping here would stop that node gossiping for good, silently.
    """
    local_records = store.list_all()
    request = pb.GossipDigest(
        from_node_id=settings.node_id,
        entries=encode_digest(build_digest(local_records)),
        membership=encode_membership(membership.snapshot(now)),
    )

    try:
        stub = pb_grpc.ClusterStub(channels.channel(peer.address))
        delta = stub.Gossip(request, timeout=settings.rpc_timeout_seconds)
    except grpc.RpcError as error:
        logger.info("gossip with %s failed: %s", peer.node_id, error)
        membership.record_unreachable(peer.node_id, now)
        return False

    membership.record_reachable(peer.node_id, now)

    # Everything below can fail too, and all of it runs on the gossip thread.
    # A decode error, or a peer that dies between answering the digest and
    # receiving the pushes — the exact scenario this phase exists for — would
    # otherwise propagate out of the thread target.
    try:
        membership.merge(decode_membership(delta.membership), now)

        # Pull: merge what the peer is ahead on.
        for message in delta.records:
            store.merge_remote(message_to_record(message))

        # Push: send back what the peer said it is behind on. ReplicateRecord
        # already merges on the far side, so no new transfer path is needed.
        wanted = {uuid.UUID(raw) for raw in delta.wanted_file_ids}
        for record in local_records:
            if record.file_id in wanted:
                stub.ReplicateRecord(
                    record_to_message(record), timeout=settings.rpc_timeout_seconds
                )
    except (grpc.RpcError, ValueError) as error:
        logger.warning("gossip with %s failed mid-exchange: %s", peer.node_id, error)
        return False

    return True


def catch_up_blobs(
    *,
    settings: Settings,
    store: MetadataStore,
    blob_store: BlobStore,
    replication_client: ReplicationClient,
    budget: int,
    should_stop: Callable[[], bool] = lambda: False,
) -> int:
    """Fetch up to `budget` blobs this node should hold but does not.

    Driven by placement, not by "fetch whatever I am missing": a node pulls only
    the content rendezvous hashing makes it a replica for. Fetching everything
    would make every node store the whole corpus and quietly undo the sharding.

    Bounded on purpose. This runs on the gossip thread and each fetch is a
    blocking transfer, so a node healing from a long partition could otherwise
    spend minutes inside a single tick — which is what would actually defeat
    the loop's shutdown join. Whatever is left over is picked up next tick:
    anti-entropy is a loop, not a one-shot.
    """
    fetched = 0
    for record in store.list_live():
        if fetched >= budget or should_stop():
            break
        replicas = replica_set(
            record.content_hash, settings.node_ids, settings.replication_factor
        )
        if settings.node_id not in replicas:
            continue
        if blob_store.exists(record.content_hash):
            continue
        content = replication_client.get_blob(record.content_hash)
        if content is not None:
            blob_store.put(content)
            fetched += 1
            logger.info("caught up blob %s", record.content_hash[:12])
    return fetched
