# almacen/main.py
"""FastAPI application bootstrap for a single node of the cluster."""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

import structlog
from fastapi import FastAPI

from almacen.api.routers import cluster as cluster_router
from almacen.api.routers import files as files_router
from almacen.api.routers import tags as tags_router
from almacen.cluster.channels import ChannelPool
from almacen.cluster.gossip import GossipLoop
from almacen.cluster.membership import Membership
from almacen.cluster.metadata_replicator import MetadataReplicator
from almacen.cluster.replicated_metadata_store import ReplicatedMetadataStore
from almacen.cluster.replication_client import ReplicationClient
from almacen.config import Settings
from almacen.observability import configure_logging_from_env
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.gc_state import BlobGCState
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex

logger = structlog.get_logger(__name__)

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
        grace_ttl=timedelta(seconds=settings.grace_ttl_seconds),
    )
    server.start()
    app.state.grpc_server = server

    # Started after the gRPC server so the node is reachable for the whole time
    # it is gossiping, and stopped before it for the same reason.
    if app.state.gossip_loop is not None:
        app.state.gossip_loop.start()

    logger.info(
        "node_started",
        node_id=settings.node_id,
        grpc_port=settings.grpc_port,
        peers=[p.node_id for p in settings.remote_peers],
        gossip_enabled=settings.gossip_enabled,
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
        if app.state.gc_state is not None:
            app.state.gc_state.close()
        logger.info("node_stopped", node_id=settings.node_id)


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
    # One Membership instance, shared by the servicer, the loop and both
    # cluster clients. Two would each hold half the picture and neither would
    # see the other's observations.
    membership = Membership(
        node_id=settings.node_id,
        peers=tuple(settings.node_ids),
        suspicion_timeout=timedelta(seconds=settings.suspicion_timeout_seconds),
    )
    app.state.membership = membership
    app.state.replication_client = ReplicationClient(
        settings, blob_store, channels, membership
    )
    app.state.metadata_store = ReplicatedMetadataStore(
        local_metadata_store, MetadataReplicator(settings, channels, membership)
    )
    # Queries read the local replica directly; there is nothing to replicate.
    app.state.tag_index = TagIndex(local_metadata_store)

    # Disabled by a non-positive interval. That is what keeps the test suite's
    # multi-node fixtures from starting real gossip threads on a real clock.
    # Garbage collection runs on the same loop, so it is disabled with it.
    app.state.gc_state = None
    app.state.gossip_loop = None
    if settings.gossip_enabled:
        # A file of its own beside the metadata, independent of its schema.
        app.state.gc_state = BlobGCState(settings.db_path.with_name("gc_state.db"))
        app.state.gossip_loop = GossipLoop(
            settings=settings,
            store=local_metadata_store,
            blob_store=blob_store,
            gc_state=app.state.gc_state,
            membership=membership,
            channels=channels,
            replication_client=app.state.replication_client,
        )

    app.include_router(files_router.router)
    app.include_router(tags_router.router)
    app.include_router(cluster_router.router)

    return app


# Before the app is built, so every component logs through the configured
# renderer from its first line.
configure_logging_from_env()
app = create_app()
