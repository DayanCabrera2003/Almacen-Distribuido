# Almacén Distribuido de Archivos Basado en Etiquetas

Tag-based distributed file store. See
`docs/superpowers/specs/2026-09-28-tag-based-distributed-store-design.md` for the
full architecture, and `docs/project-context.md` for the original assignment
brief.

## Status

Phase 1 (single-node core) — no networking/clustering yet. A single node exposes
a REST API to upload, download, tag, query, and delete files, backed by local
filesystem blob storage and a local SQLite metadata store.

## Setup

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e ".[dev]"

## Run

    uvicorn almacen.main:app --reload

By default, data is stored under `./data` (override with the `ALMACEN_DATA_DIR`
environment variable). Interactive API docs: http://127.0.0.1:8000/docs

## Test

    pytest -v

## API

| Method   | Path                          | Description                                              |
| -------- | ----------------------------- | -------------------------------------------------------- |
| `POST`   | `/files`                      | Upload a file (multipart: `file`, optional `name`, `tags` as a comma-separated list). Returns metadata, `201`. |
| `GET`    | `/files`                      | List live files. Optional `tags` (comma-separated) and `mode` (`and` \| `or`, default `and`). |
| `GET`    | `/files/{file_id}`            | Download the file's content.                             |
| `PATCH`  | `/files/{file_id}`            | Rename (`name`) and/or replace content (`file`). Both optional. |
| `DELETE` | `/files/{file_id}`            | Delete the file (tombstone), `204`.                      |
| `GET`    | `/files/{file_id}/tags`       | List the file's tags.                                    |
| `POST`   | `/files/{file_id}/tags`       | Add tags (JSON body `{"tags": ["a", "b"]}`).             |
| `DELETE` | `/files/{file_id}/tags/{tag}` | Remove one tag. Removing an absent tag is a no-op.       |

Notes:

- A deleted (tombstoned) file is indistinguishable from an unknown one: every
  endpoint above returns `404` for it.
- `mode` only applies when `tags` is given; an invalid `mode` alongside `tags`
  returns `400`.

## Layout

    almacen/
      config.py          # node Settings (data_dir, db_path)
      main.py            # FastAPI app factory + module-level `app`
      domain/            # FileRecord entity, no I/O
      storage/           # BlobStore (filesystem), MetadataStore (SQLite), TagIndex
      api/               # schemas, dependency providers, routers
    tests/
      unit/              # domain tests
      integration/       # storage and API tests
