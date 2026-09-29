from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from almacen.config import Settings
from almacen.main import create_app


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    settings = Settings(data_dir=tmp_path / "blobs", db_path=tmp_path / "metadata.db")
    app = create_app(settings)
    return TestClient(app)


def test_upload_then_download_roundtrips_content(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("report.txt", b"hello world", "text/plain")},
        data={"name": "report.txt", "tags": "invoice,draft"},
    )
    assert upload.status_code == 201
    body = upload.json()
    assert sorted(body["tags"]) == ["draft", "invoice"]
    file_id = body["file_id"]

    download = client.get(f"/files/{file_id}")
    assert download.status_code == 200
    assert download.content == b"hello world"


def test_download_unknown_file_returns_404(client: TestClient):
    response = client.get("/files/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404


def test_patch_renames_and_replaces_content(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"v1", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]

    rename = client.patch(f"/files/{file_id}", data={"name": "b.txt"})
    assert rename.status_code == 200
    assert rename.json()["name"] == "b.txt"

    update_content = client.patch(
        f"/files/{file_id}",
        files={"file": ("b.txt", b"v2", "text/plain")},
    )
    assert update_content.status_code == 200
    assert client.get(f"/files/{file_id}").content == b"v2"


def test_delete_tombstones_and_all_endpoints_then_404(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"v1", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]

    delete = client.delete(f"/files/{file_id}")
    assert delete.status_code == 204

    assert client.get(f"/files/{file_id}").status_code == 404
    assert client.patch(f"/files/{file_id}", data={"name": "x"}).status_code == 404
    assert client.delete(f"/files/{file_id}").status_code == 404
