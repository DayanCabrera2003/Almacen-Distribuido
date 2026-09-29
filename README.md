# Almacén Distribuido de Archivos Basado en Etiquetas

Tag-based distributed file store. See
`docs/superpowers/specs/2026-09-28-tag-based-distributed-store-design.md` for the
full architecture, and `docs/project-context.md` for the original assignment
brief.

## Status

Phase 2 (static cluster) — a fixed set of nodes replicate content and metadata.
Content is stored on R=3 of N=5 nodes chosen by rendezvous hashing, acknowledged
after W=2 replicas confirm. Metadata is replicated in full to every node, so any
node answers any query. No failure handling yet: membership is static (no
gossip), and concurrent conflicting writes are resolved by arrival order rather
than CRDT merge. Those are Phases 3 and 4.

## Setup

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e ".[dev]"

## Run a single node

    uvicorn almacen.main:app --reload

By default, data is stored under `./data` (override with the `ALMACEN_DATA_DIR`
environment variable). Interactive API docs: http://127.0.0.1:8000/docs

## Configuration

Each node reads its configuration from the environment:

| Variable | Default | Meaning |
| --- | --- | --- |
| `ALMACEN_DATA_DIR` | `./data` | Holds `blobs/` and `metadata.db` |
| `ALMACEN_NODE_ID` | `node1` | This node's identity; must appear in `ALMACEN_PEERS` |
| `ALMACEN_GRPC_PORT` | `50051` | Port for the internal node-to-node plane |
| `ALMACEN_PEERS` | *(empty)* | `node_id@host:port,...` for every node **including this one**; empty means single-node |
| `ALMACEN_REPLICATION_FACTOR` | `3` | R — replicas per blob |
| `ALMACEN_WRITE_QUORUM` | `2` | W — replicas that must confirm a write |

With `ALMACEN_PEERS` unset the node is a one-node cluster: the replica set for
any blob is that single node, and the acknowledgements required shrink to match
(`min(W, replicas)` = 1). `ALMACEN_REPLICATION_FACTOR` itself keeps its
configured value. This is how local development and most of the test suite run.

## Run a five-node cluster

    docker compose -f docker/docker-compose.yml up --build -d

Nodes are exposed on ports 8001-8005. Any node accepts any request, and a node
serves content it does not itself hold by fetching it from a replica:

    curl -F "file=@README.md" -F "name=README.md" -F "tags=demo" http://127.0.0.1:8001/files
    curl "http://127.0.0.1:8005/files?tags=demo"

Tear down with `docker compose -f docker/docker-compose.yml down -v`.

## Regenerating the gRPC stubs

`almacen/rpc/*_pb2*.py` are generated from the `.proto` files and committed, so a
plain `pip install -e .` needs no build step. After editing any `.proto`:

    ./scripts/gen_protos.sh

This needs `grpcio-tools` (a dev dependency). The generated code pins a minimum
`protobuf` runtime version, so if you regenerate with a newer `grpcio-tools`,
raise the `protobuf` floor in `pyproject.toml` to match.

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
      config.py          # node Settings: storage paths + static cluster membership
      main.py            # FastAPI app factory; lifespan runs the gRPC server
      domain/            # FileRecord entity, no I/O
      storage/           # BlobStore (filesystem), MetadataStore (SQLite), TagIndex
      rpc/               # node-to-node gRPC: protos, generated stubs, servicers
      cluster/           # placement (HRW), replication client, metadata replication
      api/               # schemas, dependency providers, routers
    docker/              # Dockerfile + five-node Compose cluster
    scripts/             # gen_protos.sh
    tests/
      unit/              # domain, config, placement, codec — no I/O
      integration/       # storage, rpc, cluster, API
      cluster/           # five-node end-to-end
