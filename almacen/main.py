# almacen/main.py
"""FastAPI application bootstrap for a single node of the cluster."""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI

from almacen.api.routers import files as files_router
from almacen.api.routers import tags as tags_router
from almacen.cluster.channels import ChannelPool
from almacen.cluster.gossip import GossipLoop
from almacen.cluster.membership import Membership
from almacen.cluster.metadata_replicator import MetadataReplicator
from almacen.cluster.replicated_metadata_store import ReplicatedMetadataStore
from almacen.cluster.replication_client import ReplicationClient
from almacen.config import Settings
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex

logger = logging.getLogger(__name__)

GRPC_SHUTDOWN_GRACE_SECONDS = 2.0


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Run the node's gRPC server for as long as the HTTP app is serving.

    The server is built here, not in `create_app`, because `add_insecure_port`
    binds the socket immediately and `create_app` runs at import time.
    """
    settings: Settings = app.state.settings
    server = build_server(
        blob_store=app.state.blob_store,
        # The local store, never the replicating wrapper: records arriving from
        # peers must not be pushed straight back out.
        metadata_store=app.state.local_metadata_store,
        node_id=settings.node_id,
        membership=app.state.membership,
        port=settings.grpc_port,
    )
    server.start()
    app.state.grpc_server = server

    # Started after the gRPC server so the node is reachable for the whole time
    # it is gossiping, and stopped before it for the same reason.
    if app.state.gossip_loop is not None:
        app.state.gossip_loop.start()

    logger.info(
        "node %s serving gRPC on port %s, peers: %s",
        settings.node_id,
        settings.grpc_port,
        [p.node_id for p in settings.remote_peers],
    )
    try:
        yield
    finally:
        # Gossip first: a round in flight would otherwise keep using the server
        # and the channels that the next two lines tear down.
        if app.state.gossip_loop is not None:
            app.state.gossip_loop.stop()
        server.stop(GRPC_SHUTDOWN_GRACE_SECONDS).wait()
        # The pool is shared by both cluster clients, so it is closed here by its
        # owner rather than by either of them.
        app.state.channels.close()
        app.state.local_metadata_store.close()
        logger.info("node %s stopped", settings.node_id)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(title="Almacen Distribuido", lifespan=_lifespan)

    blob_store = BlobStore(settings.data_dir)
    local_metadata_store = MetadataStore(settings.db_path)
    # One pool shared by both cluster clients, so peers get one connection each.
    channels = ChannelPool()

    app.state.settings = settings
    app.state.channels = channels
    app.state.blob_store = blob_store
    app.state.local_metadata_store = local_metadata_store
    app.state.replication_client = ReplicationClient(settings, blob_store, channels)
    app.state.metadata_store = ReplicatedMetadataStore(
        local_metadata_store, MetadataReplicator(settings, channels)
    )
    # Queries read the local replica directly; there is nothing to replicate.
    app.state.tag_index = TagIndex(local_metadata_store)

    # One Membership instance, shared by the servicer and the loop. Two would
    # each hold half the picture and neither would see the other's observations.
    membership = Membership(
        node_id=settings.node_id,
        peers=tuple(settings.node_ids),
        suspicion_timeout=timedelta(seconds=settings.suspicion_timeout_seconds),
    )
    app.state.membership = membership
    # Disabled by a non-positive interval. That is what keeps the test suite's
    # multi-node fixtures from starting real gossip threads on a real clock.
    app.state.gossip_loop = (
        GossipLoop(
            settings=settings,
            store=local_metadata_store,
            blob_store=blob_store,
            membership=membership,
            channels=channels,
            replication_client=app.state.replication_client,
        )
        if settings.gossip_enabled
        else None
    )

    app.include_router(files_router.router)
    app.include_router(tags_router.router)

    return app


app = create_app()
