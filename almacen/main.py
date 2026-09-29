# almacen/main.py
"""FastAPI application bootstrap for a single node."""
from __future__ import annotations

from fastapi import FastAPI

from almacen.api.routers import files as files_router
from almacen.api.routers import tags as tags_router
from almacen.config import Settings
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(title="Almacen Distribuido")

    app.state.blob_store = BlobStore(settings.data_dir)
    app.state.metadata_store = MetadataStore(settings.db_path)
    app.state.tag_index = TagIndex(app.state.metadata_store)

    app.include_router(files_router.router)
    app.include_router(tags_router.router)

    return app


app = create_app()
