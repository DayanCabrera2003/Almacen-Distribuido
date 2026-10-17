"""GET /files/{file_id}/metadata: one file's metadata without its content."""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from almacen.config import Settings
from almacen.main import create_app


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    settings = Settings(data_dir=tmp_path / "blobs", db_path=tmp_path / "metadata.db")
    return TestClient(create_app(settings))


def _upload(client: TestClient) -> dict:
    response = client.post(
        "/files",
        files={"file": ("report.txt", b"hello", "text/plain")},
        data={"name": "report.txt", "tags": "invoice,draft"},
    )
    assert response.status_code == 201
    return response.json()


def test_metadata_matches_the_list_item_for_the_same_file(client):
    uploaded = _upload(client)
    file_id = uploaded["file_id"]

    response = client.get(f"/files/{file_id}/metadata")

    assert response.status_code == 200
    body = response.json()
    assert body == uploaded
    assert body == client.get("/files").json()[0]
    assert set(body) == {
        "file_id",
        "name",
        "content_hash",
        "tags",
        "created_at",
        "updated_at",
    }
    assert body["tags"] == ["draft", "invoice"]


def test_metadata_reflects_later_changes(client):
    file_id = _upload(client)["file_id"]
    client.patch(f"/files/{file_id}", data={"name": "renamed.txt"})
    client.post(f"/files/{file_id}/tags", json={"tags": ["final"]})

    body = client.get(f"/files/{file_id}/metadata").json()

    assert body["name"] == "renamed.txt"
    assert body["tags"] == ["draft", "final", "invoice"]
    assert body["updated_at"] > body["created_at"]


def test_metadata_of_an_unknown_file_is_404(client):
    response = client.get("/files/00000000-0000-0000-0000-000000000000/metadata")

    assert response.status_code == 404


def test_metadata_of_a_deleted_file_is_404(client):
    file_id = _upload(client)["file_id"]
    client.delete(f"/files/{file_id}")

    response = client.get(f"/files/{file_id}/metadata")

    assert response.status_code == 404


def test_metadata_with_a_malformed_id_is_422(client):
    assert client.get("/files/not-a-uuid/metadata").status_code == 422
