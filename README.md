# Almacén Distribuido de Archivos Basado en Etiquetas

Tag-based distributed file store. See
`docs/superpowers/specs/2026-09-28-tag-based-distributed-store-design.md` for the
full architecture, and `docs/project-context.md` for the original assignment
brief.

## Status

Phase 5 (deletion and garbage collection) — deleted files and orphaned content
are now reclaimed, without letting a node returning from a partition undo a
delete.

Content is stored on R=3 of N=5 nodes chosen by rendezvous hashing, acknowledged
after W=2 replicas confirm. Metadata is replicated in full to every node, so any
node answers any query, and metadata is merged rather than overwritten:

- **tags** are an observed-remove set, so a tag added on one node survives a
  concurrent removal on another;
- **name**, **content pointer** and **tombstone** are last-writer-wins registers
  with a `(timestamp, node_id)` tie-break, so every node picks the same winner.
  Write timestamps are monotonic *per record* — each mutation is stamped above
  the newest timestamp the record already carries — so a write always outranks
  what it observed. Without that floor, a delete issued on a node with a lagging
  clock could resurrect a file, and two writes in one clock tick could leave
  replicas permanently disagreeing. Clock skew between nodes therefore affects
  *which* of two genuinely concurrent writes wins, but cannot undo a write that
  causally followed another;
- a **vector clock** per record separates a causally stale update (discarded
  quietly) from a genuinely concurrent one (resolved by last-writer-wins and
  logged with both clocks).

Nodes track each other's health with a SWIM-style failure detector — a failed
probe means *suspicion*, not death, and a node wrongly suspected refutes the
claim rather than being evicted. Every few seconds each node runs a push-pull
anti-entropy round with a couple of peers: they exchange a `file_id → vector
clock` digest, transfer only the records that actually differ, and merge them
with the Phase 3 CRDT merge. A node also fetches any blob it is an HRW replica
for but does not hold, so content catches up along with metadata.

The practical effect: a write made while a node is unreachable reaches it once
the partition heals, rather than being lost forever.

Deletion is two-staged. A `DELETE` tombstones the record, which gossip spreads
like any other update; once the tombstone is older than the **grace TTL**
(counted from when the delete was issued) each node purges the record. A blob
whose last referring record is purged — or repointed elsewhere by a `PATCH` —
becomes a GC candidate, and is deleted from disk only after it has stayed
unreferenced for another grace TTL. A deleted file's content therefore outlives
the delete by roughly two TTLs, on purpose. A node never re-adopts a tombstone
that is already past the TTL, so a purge sticks instead of being handed back by
a peer that has not collected yet. GC runs on the gossip loop with a per-round
budget.

> **Warning:** the grace TTL is a safety margin, not a tidiness knob. A purged
> tombstone is indistinguishable from a file a node has never seen, so if a node
> was partitioned for longer than the TTL it still holds the file live and, once
> the partition heals, teaches it back to everyone — the delete is silently
> undone. Keep `ALMACEN_GRACE_TTL_SECONDS` above the longest partition the
> deployment must survive; the 24h default exists for that reason.

Still missing: OR-Set remove entries for tags are never pruned (safe pruning
needs every node to have observed the removal, which this design does not
track). There is no `/cluster/status` endpoint yet — the membership view is
in-process only — and no structured logging or CLI, which are Phase 6.

### Upgrading from Phase 2

The metadata schema changed incompatibly to store the CRDT state, and no
migration is provided: a node's `ALMACEN_DATA_DIR` must be empty before it
starts. Removing the node's volume (`docker volume rm almacen-nodeN-data`)
empties it; see [docs/deployment.md](docs/deployment.md).

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
| `ALMACEN_DATA_DIR` | `./data` | Holds `blobs/`, `metadata.db` and `gc_state.db` |
| `ALMACEN_NODE_ID` | `node1` | This node's identity; must appear in `ALMACEN_PEERS` |
| `ALMACEN_GRPC_PORT` | `50051` | Port for the internal node-to-node plane |
| `ALMACEN_PEERS` | *(empty)* | `node_id@host:port,...` for every node **including this one**; empty means single-node |
| `ALMACEN_REPLICATION_FACTOR` | `3` | R — replicas per blob |
| `ALMACEN_WRITE_QUORUM` | `2` | W — replicas that must confirm a write |
| `ALMACEN_RPC_TIMEOUT_SECONDS` | `5.0` | Ceiling on one replicated write against an unresponsive peer |
| `ALMACEN_GOSSIP_INTERVAL_SECONDS` | `5.0` | Seconds between anti-entropy rounds. **Zero or less disables gossip entirely** |
| `ALMACEN_SUSPICION_TIMEOUT_SECONDS` | `15.0` | How long a peer stays *suspect* before being declared dead |
| `ALMACEN_GOSSIP_FANOUT` | `2` | Peers contacted per round |
| `ALMACEN_BLOB_CATCHUP_BUDGET` | `8` | Most blobs fetched in one round, so a long catch-up cannot monopolise the loop |
| `ALMACEN_GRACE_TTL_SECONDS` | `86400` | How long a tombstone is kept before its record is purged, and how long an unreferenced blob is kept before it is deleted. **Must exceed the longest expected partition**, or deleted files can come back |
| `ALMACEN_GC_BUDGET` | `32` | Most purges plus blob deletions in one round. GC runs on the gossip loop, so it is disabled along with gossip |

With `ALMACEN_PEERS` unset the node is a one-node cluster: the replica set for
any blob is that single node, and the acknowledgements required shrink to match
(`min(W, replicas)` = 1). `ALMACEN_REPLICATION_FACTOR` itself keeps its
configured value. This is how local development and most of the test suite run.

## Run a five-node cluster

The cluster is deployed by hand with `docker build`, `docker network create` and
one `docker run` per node — no orchestration file. The full step-by-step, with
the reason for every flag, is in [docs/deployment.md](docs/deployment.md). In
short:

    docker build -f docker/Dockerfile -t almacen:latest .
    docker network create almacen-net
    PEERS="node1@node1:50051,node2@node2:50051,node3@node3:50051,node4@node4:50051,node5@node5:50051"
    for i in 1 2 3 4 5; do
      docker run -d --name node$i --hostname node$i --network almacen-net \
        -p 800$i:8000 -v almacen-node$i-data:/data \
        -e ALMACEN_NODE_ID=node$i -e ALMACEN_PEERS="$PEERS" \
        -e ALMACEN_GOSSIP_INTERVAL_SECONDS=2 -e ALMACEN_SUSPICION_TIMEOUT_SECONDS=6 \
        -e ALMACEN_GRACE_TTL_SECONDS=20 \
        almacen:latest
    done

Nodes are exposed on ports 8001-8005. Any node accepts any request, and a node
serves content it does not itself hold by fetching it from a replica:

    curl -F "file=@README.md" -F "name=README.md" -F "tags=demo" http://127.0.0.1:8001/files
    curl "http://127.0.0.1:8005/files?tags=demo"

Nodes have no startup ordering, so an upload issued in the first seconds after
starting them may return 503 while peers are still binding their gRPC ports.
Give the cluster a moment before the first request.

The commands above shorten the gossip interval to 2s, the suspicion timeout to
6s and the grace TTL to 20s, so reconciliation and garbage collection are
observable within a demo session rather than on the production schedule. The
20s TTL is a **demo value only** — see the warning under Status.

### Demonstrating a partition

With the cluster running:

    # Cut node5 off from the cluster network.
    docker network disconnect almacen-net node5

    # Write while it is isolated.
    FILE_ID=$(curl -s -F "file=@README.md" -F "name=during-partition.md" \
      -F "tags=partitioned" http://127.0.0.1:8001/files \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['file_id'])")

    curl -s -o /dev/null -w "node2: %{http_code}\n" http://127.0.0.1:8002/files/$FILE_ID
    curl -s -m 3 -o /dev/null -w "node5: %{http_code}\n" http://127.0.0.1:8005/files/$FILE_ID

    # Heal, and give gossip a few rounds.
    docker network connect almacen-net node5
    sleep 10
    curl -s -o /dev/null -w "node5: %{http_code}\n" http://127.0.0.1:8005/files/$FILE_ID

node2 answers `200` throughout. node5 answers `000` while partitioned — not
`404`: `docker network disconnect` removes the container from the network
entirely, so its published port stops answering too, and curl cannot connect at
all. After healing it answers `200`, having learned both the record and the
content through gossip.

### Demonstrating deletion and garbage collection

With the cluster running (and its 20s grace TTL):

    echo "doomed $(date +%s%N)" > /tmp/doomed.txt
    FILE_ID=$(curl -s -F "file=@/tmp/doomed.txt" -F "name=doomed.txt" \
      http://127.0.0.1:8001/files \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['file_id'])")
    HASH=$(sha256sum /tmp/doomed.txt | cut -d' ' -f1)
    blobs() { for i in 1 2 3 4 5; do printf "node$i:%s " \
      "$(docker exec node$i ls /data/blobs | grep -c $HASH)"; done; echo; }

    curl -s -o /dev/null -w "%{http_code}\n" -X DELETE http://127.0.0.1:8001/files/$FILE_ID
    blobs              # still on its 3 replicas: only the record is tombstoned
    sleep 25; blobs    # record purged everywhere; blobs are now GC candidates
    sleep 25; blobs    # blobs deleted: 0 on every node

The step-by-step in
[docs/deployment.md](docs/deployment.md#7-watch-a-delete-being-garbage-collected)
also shows the record itself disappearing from every node's metadata.

Tear down with:

    docker rm -f node1 node2 node3 node4 node5
    docker network rm almacen-net
    docker volume rm almacen-node1-data almacen-node2-data almacen-node3-data \
      almacen-node4-data almacen-node5-data

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
      crdt/              # OrSet, LWWRegister, VectorClock — pure, no I/O
      storage/           # BlobStore (filesystem), MetadataStore (SQLite), TagIndex,
                         # BlobGCState (when each orphaned blob lost its last referrer)
      rpc/               # node-to-node gRPC: protos, generated stubs, servicers
      cluster/           # placement (HRW), replication, membership, gossip, anti-entropy, GC
      api/               # schemas, dependency providers, routers
    docker/              # Dockerfile (one image: every node is the same kind)
    scripts/             # gen_protos.sh
    tests/
      unit/              # crdt, domain, config, placement, codec — no I/O
      integration/       # storage, rpc, cluster, API
      cluster/           # five-node end-to-end
