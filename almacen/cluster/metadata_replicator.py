# almacen/cluster/metadata_replicator.py
"""Pushes metadata records to every peer (spec §4: full metadata replication)."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import grpc

from almacen.cluster.channels import ChannelPool
from almacen.config import Settings
from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.record_codec import record_to_message

logger = logging.getLogger(__name__)


class MetadataReplicator:
    """Best-effort push of a record to all peers.

    Deliberately best-effort: unlike content (where W acks are a durability
    guarantee from spec §3), metadata replication failing must not turn a
    successful local write into a client-visible error. A peer that misses a push
    diverges, and nothing here repairs it — that is exactly the gap Phase 4's
    push-pull anti-entropy closes.
    """

    def __init__(self, settings: Settings, channels: ChannelPool | None = None) -> None:
        self._settings = settings
        self._channels = channels or ChannelPool()

    def replicate(self, record: FileRecord) -> None:
        peers = self._settings.remote_peers
        if not peers:
            return

        message = record_to_message(record)
        with ThreadPoolExecutor(max_workers=len(peers)) as pool:
            # list() forces every push to complete; exceptions are handled inside
            # _push, so nothing escapes to the caller.
            list(pool.map(lambda peer: self._push(peer, message), peers))

    def _push(self, peer, message) -> None:
        try:
            stub = pb_grpc.ClusterStub(self._channels.channel(peer.address))
            stub.ReplicateRecord(message, timeout=self._settings.rpc_timeout_seconds)
        except grpc.RpcError as error:
            logger.warning(
                "could not replicate record %s to %s: %s",
                message.file_id,
                peer.node_id,
                error,
            )

    def close(self) -> None:
        """Close the channel pool.

        Only safe when this replicator owns its pool. When a pool is shared with
        other clients — as it is in `create_app` — the owner closes it instead,
        or this would disconnect the others too.
        """
        self._channels.close()
