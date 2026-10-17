"""GC rides on the gossip loop, so it is wired, and disabled, together with it."""
from pathlib import Path

from almacen.config import Settings
from almacen.main import create_app
from almacen.storage.gc_state import BlobGCState


def test_gc_state_is_created_next_to_the_metadata_when_gossip_runs(tmp_path: Path):
    settings = Settings(data_dir=tmp_path / "blobs", db_path=tmp_path / "metadata.db")
    app = create_app(settings)
    try:
        assert app.state.gossip_loop is not None
        assert isinstance(app.state.gc_state, BlobGCState)
        assert (tmp_path / "gc_state.db").exists()
    finally:
        app.state.gc_state.close()
        app.state.local_metadata_store.close()


def test_gc_is_disabled_along_with_gossip(tmp_path: Path):
    # The multi-node fixtures disable gossip to avoid real timers; GC must not
    # sneak a background job back in through another door.
    settings = Settings(
        data_dir=tmp_path / "blobs",
        db_path=tmp_path / "metadata.db",
        gossip_interval_seconds=0,
    )
    app = create_app(settings)
    try:
        assert app.state.gossip_loop is None
        assert app.state.gc_state is None
        assert not (tmp_path / "gc_state.db").exists()
    finally:
        app.state.local_metadata_store.close()
