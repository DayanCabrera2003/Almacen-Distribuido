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


def test_list_with_and_mode_returns_intersection(client: TestClient):
    client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "invoice,draft"},
    )
    client.post(
        "/files",
        files={"file": ("b.txt", b"b", "text/plain")},
        data={"name": "b.txt", "tags": "invoice"},
    )

    response = client.get("/files", params={"tags": "invoice,draft", "mode": "and"})

    assert response.status_code == 200
    assert [f["name"] for f in response.json()] == ["a.txt"]


def test_list_with_or_mode_returns_union(client: TestClient):
    client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "x"},
    )
    client.post(
        "/files",
        files={"file": ("b.txt", b"b", "text/plain")},
        data={"name": "b.txt", "tags": "y"},
    )

    response = client.get("/files", params={"tags": "x,y", "mode": "or"})

    assert {f["name"] for f in response.json()} == {"a.txt", "b.txt"}


def test_list_with_no_tags_returns_all_live_files(client: TestClient):
    client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt"},
    )

    response = client.get("/files")

    assert response.status_code == 200
    assert [f["name"] for f in response.json()] == ["a.txt"]


def test_list_excludes_deleted_files(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "x"},
    )
    file_id = upload.json()["file_id"]
    client.delete(f"/files/{file_id}")

    response = client.get("/files", params={"tags": "x"})

    assert response.json() == []


def test_list_rejects_invalid_mode(client: TestClient):
    response = client.get("/files", params={"tags": "x", "mode": "xor"})
    assert response.status_code == 400


def test_list_ignores_invalid_mode_when_no_tags_given(client: TestClient):
    client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt"},
    )

    response = client.get("/files", params={"mode": "xor"})

    assert response.status_code == 200
    assert [f["name"] for f in response.json()] == ["a.txt"]


def test_add_list_and_remove_tags(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "invoice"},
    )
    file_id = upload.json()["file_id"]

    list_tags = client.get(f"/files/{file_id}/tags")
    assert list_tags.status_code == 200
    assert list_tags.json()["tags"] == ["invoice"]

    add_tags = client.post(f"/files/{file_id}/tags", json={"tags": ["draft", "final"]})
    assert add_tags.status_code == 200
    assert sorted(add_tags.json()["tags"]) == ["draft", "final", "invoice"]

    remove_tag = client.delete(f"/files/{file_id}/tags/draft")
    assert remove_tag.status_code == 200
    assert sorted(remove_tag.json()["tags"]) == ["final", "invoice"]


def test_remove_absent_tag_is_a_noop_not_an_error(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]

    response = client.delete(f"/files/{file_id}/tags/nonexistent")

    assert response.status_code == 200
    assert response.json()["tags"] == []


def test_tag_endpoints_404_on_deleted_file(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]
    client.delete(f"/files/{file_id}")

    assert client.get(f"/files/{file_id}/tags").status_code == 404
    assert client.post(f"/files/{file_id}/tags", json={"tags": ["x"]}).status_code == 404
    assert client.delete(f"/files/{file_id}/tags/x").status_code == 404
