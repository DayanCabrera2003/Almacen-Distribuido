"""Every API input path applies the same tag rule (almacen.domain.tags).

The property that matters: a tag the API accepts can then be found by a query.
Before the rule, `POST /files/{id}/tags` stored "", " sp " and "a,b" verbatim,
none of which `GET /files?tags=...` could ever name.
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

from almacen.config import Settings
from almacen.domain.tags import MAX_TAG_LENGTH
from almacen.main import create_app

# Raw inputs the API accepts, and the tag each one is stored as.
ACCEPTED = [
    ("invoice", "invoice"),
    ("  padded  ", "padded"),
    ("two words", "two words"),
    ("2026/q1", "2026/q1"),
    ("café", "café"),
    ("x" * MAX_TAG_LENGTH, "x" * MAX_TAG_LENGTH),
]

REJECTED = ["", "   ", "a,b", "bad\ntag", "nul\x00", "x" * (MAX_TAG_LENGTH + 1)]


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    settings = Settings(data_dir=tmp_path / "blobs", db_path=tmp_path / "metadata.db")
    return TestClient(create_app(settings))


def _upload(client: TestClient, tags: str | None = None):
    data = {"name": "a.txt"}
    if tags is not None:
        data["tags"] = tags
    return client.post(
        "/files", files={"file": ("a.txt", b"a", "text/plain")}, data=data
    )


@pytest.fixture
def file_id(client: TestClient) -> str:
    return _upload(client).json()["file_id"]


def _found_by_query(client: TestClient, tag: str, file_id: str) -> bool:
    response = client.get("/files", params={"tags": tag})
    assert response.status_code == 200, response.text
    return file_id in {f["file_id"] for f in response.json()}


# --- POST /files/{id}/tags ---------------------------------------------------


@pytest.mark.parametrize(("raw", "stored"), ACCEPTED)
def test_added_tag_is_stored_normalized_and_findable(client, file_id, raw, stored):
    response = client.post(f"/files/{file_id}/tags", json={"tags": [raw]})

    assert response.status_code == 200, response.text
    assert response.json()["tags"] == [stored]
    assert _found_by_query(client, stored, file_id)
    # The raw form is findable too: the query normalizes the same way.
    assert _found_by_query(client, raw, file_id)


@pytest.mark.parametrize("raw", REJECTED)
def test_add_rejects_a_tag_that_could_never_be_queried(client, file_id, raw):
    response = client.post(f"/files/{file_id}/tags", json={"tags": ["ok", raw]})

    assert response.status_code == 422
    assert "tag" in response.text
    # All or nothing: the valid tag in the same request was not applied either.
    assert client.get(f"/files/{file_id}/tags").json()["tags"] == []


# --- POST /files (upload form) ----------------------------------------------


def test_upload_tags_are_normalized_and_findable(client):
    response = _upload(client, " invoice , two words ")

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["tags"] == ["invoice", "two words"]
    assert _found_by_query(client, "two words", body["file_id"])


@pytest.mark.parametrize("raw", ["", "   "])
def test_upload_with_blank_tags_means_no_tags(client, raw):
    response = _upload(client, raw)

    assert response.status_code == 201
    assert response.json()["tags"] == []


@pytest.mark.parametrize("raw", ["a,,b", "a,", "bad\ntag", "x" * (MAX_TAG_LENGTH + 1)])
def test_upload_rejects_invalid_tags_and_stores_nothing(client, raw):
    response = _upload(client, raw)

    assert response.status_code == 422
    assert "tag" in response.json()["detail"]
    assert client.get("/files").json() == []


# --- GET /files?tags= --------------------------------------------------------


@pytest.mark.parametrize("raw", ["a,,b", "a,", "bad\x07tag", "x" * (MAX_TAG_LENGTH + 1)])
def test_query_rejects_invalid_tags(client, raw):
    response = client.get("/files", params={"tags": raw})

    assert response.status_code == 422
    assert "tag" in response.json()["detail"]


def test_query_with_blank_tags_lists_every_file(client, file_id):
    response = client.get("/files", params={"tags": "  "})

    assert response.status_code == 200
    assert [f["file_id"] for f in response.json()] == [file_id]


# --- DELETE /files/{id}/tags/{tag} -------------------------------------------


@pytest.mark.parametrize(("raw", "stored"), ACCEPTED)
def test_any_accepted_tag_can_be_removed_again(client, file_id, raw, stored):
    client.post(f"/files/{file_id}/tags", json={"tags": [raw]})

    response = client.delete(f"/files/{file_id}/tags/{quote(raw, safe='')}")

    assert response.status_code == 200, response.text
    assert response.json()["tags"] == []
    assert not _found_by_query(client, stored, file_id)


@pytest.mark.parametrize("raw", ["   ", "a,b", "bad\ntag", "x" * (MAX_TAG_LENGTH + 1)])
def test_remove_rejects_an_invalid_tag(client, file_id, raw):
    response = client.delete(f"/files/{file_id}/tags/{quote(raw, safe='')}")

    assert response.status_code == 422
    assert "tag" in response.json()["detail"]
