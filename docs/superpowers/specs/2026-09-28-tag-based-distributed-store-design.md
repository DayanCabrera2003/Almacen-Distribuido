# Tag-Based Distributed File Store — Design Spec

**Date:** 2026-09-28
**Status:** Approved by user, pending spec review

## 1. Objective

Build a distributed file store where every file has a stable identity, content,
name, and a set of tags, supporting add/remove/list of tags and queries over tag
combinations. The system must define consistency and durability for both content
and metadata, handle concurrent updates, define clear deletion semantics, and
reconcile state after network partitions heal.

Full original assignment brief: see `docs/project-context.md`.

## 2. Quality bar and constraints

- Solo developer, portfolio-grade quality (not a minimal course demo).
- Language: Python.
- Demo/deployment environment: Docker Compose, each node as a container, with
  network partitions injectable between containers (`docker network disconnect`
  or a proxy such as toxiproxy).
- Target demo scale: N=5 nodes, replication factor R=3 for content, write quorum
  W=2.

## 3. Consistency model

**AP with reconciliation** (Dynamo-style eventual consistency), not CP/consensus
(e.g. Raft). The assignment explicitly requires "reconciliación después de
particiones" and "tratamiento de actualizaciones concurrentes" — both are AP
concerns. A CP design would make the minority partition reject writes rather than
reconcile divergence afterward, which contradicts the requirement.

- **Metadata** (file records: name, content pointer, tags): eventually
  consistent, fully replicated to every node, merged via CRDTs.
- **Content**: durable via synchronous replication to a subset (R) of nodes at
  write time, with asynchronous anti-entropy catching up any replica that missed
  the write (e.g. because it was partitioned).

## 4. Topology

Symmetric peer nodes — no dedicated coordinator, no separate metadata-server role
(unlike CephFS's MDS/OSD split). Every node runs the same binary and plays two
roles simultaneously:

- **Data node**: stores a shard of content blobs, content-addressed by SHA-256,
  placed across a replica set of R of N nodes via rendezvous hashing (HRW).
- **Metadata node**: stores a full replica of all file metadata (records + tag
  index), kept eventually consistent via gossip + CRDT merge.

**Why not split metadata/data across node types like CephFS:** that separation
earns its complexity at datacenter scale, where metadata needs different
hardware/latency characteristics than bulk data. At this project's scale (a
handful of demo nodes), splitting roles across physical nodes adds operational
complexity without benefit. Responsibilities are instead separated **in
software** — independent packages (`storage/`, `cluster/`, `crdt/`, etc.) — not
across node types. This trade-off is documented explicitly (see §12) since the
assignment asks for architectural comparison against CephFS.

Full metadata replication (rather than sharding metadata like content) is a
deliberate choice: it makes tag-combination queries (AND/OR across tags) O(1)
scatter-gather — answerable on any single node — directly serving the "tag
index" and "queries over tag combinations" requirements. Content, being much
larger, is sharded across only R of N nodes.

**Communication planes:**
- Client-facing: REST over HTTP (FastAPI).
- Internal (node-to-node): gRPC — blob replication, gossip membership exchange,
  metadata anti-entropy.

**Membership & failure detection:** a simplified SWIM-style gossip protocol,
implemented on top of gRPC (no mature off-the-shelf SWIM library exists in
Python, and it is a core learning/portfolio component in its own right):
periodic heartbeats, a suspicion state before declaring a node dead, and
propagation of membership changes via gossip.

## 5. File identity model

**Stable ID ≠ content hash** (inode + blob style, not pure content addressing
like IPFS). Each file has a UUID (`file_id`) that never changes for its
lifetime. The file record points to a `content_hash` (SHA-256 of the current
blob). Updating a file's content changes which blob the record points to, but
identity, name, and tags persist across content updates. A pure content-addressed
identity (where identity IS the hash) would not support this without manual
linking between versions.

## 6. Data model

Replicated in full to every node:

```
FileRecord {
  file_id: UUID          # stable identity
  name: str               # LWW-register (timestamp, node_id)
  content_hash: str       # SHA-256, LWW-register
  tags: OR-Set<str>       # CRDT — conflict-free add/remove
  vector_clock: {node_id: counter}
  tombstone: bool + timestamp
}
```

## 7. Storage layer (per node, local)

- **Blobs**: flat filesystem, `blobs/<sha256>` (same pattern as Git objects).
  Writes are atomic: write to a temp file, then rename.
- **Metadata**: SQLite in WAL mode — durable locally, transactional, no external
  dependency.

## 8. Placement and replication of content

- **Placement:** rendezvous hashing (HRW) over `content_hash`, selecting R of N
  nodes as the replica set for a given blob. HRW avoids the mass-resharding that
  naive modulo hashing causes when nodes join/leave, and is simpler to implement
  correctly than a consistent-hash ring with virtual nodes.
- **Write path:** client → coordinator node (any node accepting the request)
  computes the replica set via HRW → writes the blob in parallel to the R
  replicas → acknowledges success once W of R replicas confirm (Dynamo-style
  write quorum). Replicas that lag are caught up by anti-entropy.
- **Read path:** the coordinator requests the blob from any replica in the set,
  starting with the fastest/closest, falling back to the next on failure.

## 9. Concurrency and conflict resolution

- **Tags (`OR-Set` CRDT):** each `add(tag)` creates a uniquely tagged element
  `(tag, node_id, counter)`; `remove(tag)` removes every unique element observed
  up to that point. Concurrent adds/removes from different nodes during a
  partition converge without conflict on reconciliation (union of adds minus
  observed removes) — no manual conflict resolution needed.
- **Name and content_hash (single-value fields):** `LWW-register` with tie-break
  key `(timestamp, node_id)`. If two nodes concurrently rename or update content
  during a partition, the higher timestamp wins on reconciliation (ties broken
  by higher node_id). The vector clock distinguishes a genuinely concurrent
  conflict from a causally stale update; genuine conflicts are logged as
  LWW-resolved conflicts (visible for debugging/demo), never silently dropped.

## 10. Deletion semantics

1. `DELETE` sets `tombstone=true` with a timestamp on the `FileRecord`; nothing
   is physically deleted yet. The file disappears from listings/queries
   immediately on the node that processed the delete.
2. The tombstone propagates via gossip/anti-entropy like any other metadata
   change.
3. After a configurable **grace TTL** (default: 24 simulated hours) from when
   the tombstone originated, a local GC process purges the `FileRecord`
   permanently and, via **reference counting** over `content_hash` (multiple
   `file_id`s can point at the same deduplicated blob), deletes the physical
   blob only if no other file still references it.
4. The grace TTL exists to give the tombstone time to reach nodes that were
   partitioned when the delete happened — preventing an isolated node from
   "resurrecting" a deleted file via anti-entropy with stale data after it
   reconnects.

## 11. Reconciliation after partitions (anti-entropy)

- Each node periodically (e.g. every 5s) runs **push-pull gossip** with 1-2
  random peers: they exchange a digest of metadata state (`file_id →
  vector_clock` map) and compute the diff (records the other side is missing or
  has an outdated version of).
- Only the diffing `FileRecord`s are transferred; merge is per-field (OR-Set
  unions, LWW-register compares timestamps, vector clocks combine by taking the
  max per node).
- For content: once a partition heals, a node that is behind on a blob requests
  it from a peer that has it (detected because its `FileRecord` points at a
  `content_hash` the node lacks locally).
- At this project's scale, comparing the full `file_id → vector_clock` map per
  gossip round is sufficient; a Merkle-tree diff is noted as a future
  optimization for larger scale, not implemented now.

## 12. API surface

**REST (client-facing, FastAPI):**
```
POST   /files                      # upload: multipart content + name + tags[]
GET    /files/{file_id}            # download content
PATCH  /files/{file_id}            # update name and/or content (new version)
DELETE /files/{file_id}            # tombstone
GET    /files/{file_id}/tags       # list tags for a file
POST   /files/{file_id}/tags       # add tag(s)
DELETE /files/{file_id}/tags/{tag} # remove tag
GET    /files?tags=a,b&mode=and    # query by tag combination (AND/OR)
GET    /cluster/status             # membership/health, for debugging/demo
```

**gRPC (internal, node-to-node):**
```
service Cluster {
  rpc Gossip(GossipDigest) returns (GossipDelta);   // metadata anti-entropy
  rpc Ping(PingRequest) returns (PingResponse);      // SWIM heartbeat
}
service Replication {
  rpc PutBlob(stream BlobChunk) returns (PutBlobAck);
  rpc GetBlob(BlobRequest) returns (stream BlobChunk);
}
```

## 13. Tech stack

| Layer | Choice | Rationale |
|---|---|---|
| REST | FastAPI + uvicorn | native async, Pydantic validation, free OpenAPI docs |
| Internal RPC | grpcio + grpcio-tools | strongly typed inter-node calls, streaming for large blobs |
| Local metadata | SQLite (stdlib, WAL) | durable, transactional, zero extra infra |
| Blobs | flat filesystem | simple, sufficient for this scope |
| CLI | Typer | operate the cluster/demo without curl |
| Tests | pytest + pytest-asyncio | standard |
| Packaging | Docker + docker-compose | reproducible local cluster |
| Chaos/partitions | `docker network disconnect` / toxiproxy | inject partitions to demonstrate reconciliation |
| Observability | structlog + `/cluster/status` | trace what each node is doing during the demo |

## 14. Repository structure

Atomic files, single responsibility per module:

```
almacen/
  api/
    routers/files.py         # file endpoints
    routers/tags.py          # tag endpoints
    routers/cluster.py       # status/debug endpoint
    schemas.py                # Pydantic request/response models
  rpc/
    cluster.proto
    replication.proto
    cluster_servicer.py       # implements Cluster (gossip/ping)
    replication_servicer.py   # implements Replication (put/get blob)
  domain/
    file_record.py            # FileRecord entity (no I/O)
    tag.py
  crdt/
    or_set.py                 # tag CRDT
    lww_register.py           # name / content_hash
    vector_clock.py
  storage/
    blob_store.py             # filesystem put/get/gc by content_hash
    metadata_store.py         # SQLite CRUD for FileRecord
    tag_index.py               # tag-combination queries
  cluster/
    membership.py             # simplified SWIM
    placement.py               # rendezvous hashing (HRW)
    replication_client.py      # orchestrates W/R quorum for blob read/write
    reconciliation.py          # push-pull anti-entropy, FileRecord merge
    gc.py                      # purge of expired tombstones
  config.py                    # node settings (port, peers, N/R/W, TTLs)
  main.py                      # bootstrap: starts FastAPI + gRPC + gossip loop
docker/
  Dockerfile
  docker-compose.yml            # N nodes + simulated network
tests/
  unit/                         # crdt, placement, domain — no I/O
  integration/                  # real storage (temp sqlite/fs)
  cluster/                      # multi-node via docker-compose, incl. chaos
DAA/                             # algorithm design/analysis write-ups (untracked)
docs/
  project-context.md
  superpowers/specs/...
```

If any module grows too large (candidates: `reconciliation.py`,
`metadata_store.py`), it is split further at that point rather than allowed to
grow.

## 15. Architectural comparison (assignment requirement)

To be written up in full in `docs/` once implementation begins, covering:

- **vs. CephFS:** CephFS separates metadata servers (MDS) from object storage
  daemons (OSDs) for datacenter-scale hardware specialization; this design
  separates the same concerns in software on symmetric nodes instead (§4),
  appropriate at this project's scale.
- **vs. IPFS:** IPFS identity IS the content hash (pure content addressing,
  immutable). This design deliberately decouples stable file identity from
  content hash (§5) to support versioning while keeping content
  deduplicated/addressed by hash underneath — content addressing is used as an
  implementation detail of the blob layer, not as the file's identity.
- **vs. semantic/tag-based filesystems (historical research):** informs the
  choice of a full-replica tag index (§4) for fast arbitrary tag-combination
  queries, rather than a single hierarchical namespace.

## 16. Phased implementation roadmap

Each phase is independently demoable and gets its own plan later (via
`writing-plans`), not implemented all at once:

1. **Single-node core** — `domain/`, `storage/` (blob_store + metadata_store),
   full REST API against one node, no networking.
2. **Cluster without failures** — gRPC replication, static membership (fixed
   peer list, no gossip yet), HRW placement, happy-path W/R replication, N-node
   Docker Compose.
3. **CRDTs and concurrency** — `or_set.py`, `lww_register.py`,
   `vector_clock.py` integrated into `FileRecord`; dedicated tests proving
   concurrent writes from different nodes converge correctly.
4. **Gossip, partitions, reconciliation** — dynamic SWIM-style membership,
   push-pull anti-entropy, partition injection in Compose, demo of
   post-partition reconciliation.
5. **Deletion and GC** — tombstones, grace TTL, reference counting, orphaned
   blob purge.
6. **Polish** — `/cluster/status`, structlog, Typer CLI, chaos tests, final
   documentation including the CephFS/IPFS comparison (§15).

## 17. Out of scope (for now)

- Merkle-tree-based anti-entropy (noted as a future optimization, §11).
- Dynamic replica-set resizing / rebalancing beyond what HRW gives for free.
- Multi-tenant access control / authentication (not requested by the
  assignment).
- Sibling-version exposure for conflicting writes (Dynamo-style multi-version
  reads) — conflicts are resolved via LWW instead, with conflicts logged rather
  than surfaced to the client, to keep the client-facing API simple.
