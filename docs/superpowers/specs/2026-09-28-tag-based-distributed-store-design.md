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
- **Content**: durable via a W-of-R write quorum against a replica set of size R
  (see §8 for the exact mechanism), with asynchronous anti-entropy catching up
  any replica that missed the write (e.g. because it was partitioned).

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
  tombstone: bool          # LWW-register (timestamp, node_id) — see below
}
```

**Tombstone merge rule:** `tombstone` is itself an LWW-register, merged the same
way as `name` and `content_hash` (§9): each node's current `tombstone` value
carries its own `(timestamp, node_id)`, and on merge the higher timestamp wins.
A `FileRecord` starts with `tombstone=(false, created_at, creating_node_id)`;
`DELETE` is the only operation that ever writes to this field, setting it to
`(true, delete_time, deleting_node_id)`. Because there is no "undelete" API, a
delete's timestamp is always later than the field's prior (untouched) value, so
in practice this LWW rule is equivalent to "delete is monotonic": a concurrent
delete on one node always outlasts a concurrent non-delete edit (rename, tag
change) to the same record on another node, since that edit never writes to the
`tombstone` field and so cannot produce a competing timestamp for it. The other
fields' own LWW/OR-Set values from the concurrent edit still merge normally and
remain in the stored record — they are just not visible through the API once
`tombstone=true` (§10).

**Vector clock semantics:** `vector_clock` is a single clock per `FileRecord`
(not per field). Every local mutation to the record on a given node — rename,
content update, tag add, tag remove, delete — increments that node's own
component, `vector_clock[node_id] += 1`, and the resulting value is the one used
everywhere a "counter" is needed for that mutation (including as the `counter` in
an OR-Set tag element, §9 — there is only one counter source, not two). When
merging two copies of the same `file_id` from different nodes:

- If one record's vector clock dominates the other's (≥ in every component, >
  in at least one), it is causally newer: its LWW field values are taken as-is,
  no conflict.
- If neither dominates (concurrent), it is a genuine conflict for the LWW fields
  (name, content_hash), resolved as described in §9. Tags never need this
  distinction — the OR-Set merge rule (union of adds minus observed removes) is
  correct regardless of causality.
- The merged record's vector clock is the component-wise max of both inputs.

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
- **Tombstone (delete vs. concurrent edit):** `tombstone` is a third LWW-register
  field, resolved the same way — see the "Tombstone merge rule" in §6 for the
  exact rule and why it behaves as delete-always-wins in practice.

## 10. Deletion semantics and blob garbage collection

**Deletion (`FileRecord` lifecycle):**

1. `DELETE` sets `tombstone=true` with a timestamp on the `FileRecord`; nothing
   is physically deleted yet. Once tombstoned, the file is inaccessible via
   **every** API path immediately on the node that processed the delete — not
   just listings/tag queries, but also a direct `GET /files/{file_id}` or
   `GET /files/{file_id}/tags`, which return 404 exactly as if the file_id were
   unknown. There is no "hidden from search but still directly fetchable"
   window. The same applies to every mutating endpoint on that `file_id`
   (`PATCH`, `POST .../tags`, `DELETE .../tags/{tag}`) — all return 404, not a
   silent no-op.
2. The tombstone propagates via gossip/anti-entropy like any other metadata
   change. Until it reaches a given node, that node still serves the file
   normally — this propagation delay is the expected, bounded inconsistency
   window inherent to the AP model (§3), not a special case.
3. After a configurable **grace TTL** (default: 24h, but see "TTL in the demo"
   below) from when the tombstone originated, a local GC process purges the
   `FileRecord` permanently.
4. The grace TTL exists to give the tombstone time to reach nodes that were
   partitioned when the delete happened — preventing an isolated node from
   "resurrecting" a deleted file via anti-entropy with stale data after it
   reconnects.

**Blob garbage collection (reference counting):** a physical blob (`content_hash`)
can be orphaned two ways, not just one: (a) its owning `FileRecord` is tombstoned
and purged (step 3 above), or (b) a `PATCH` repoints a still-live `FileRecord`'s
`content_hash` to new content, dropping its reference to the old blob. Both cases
are handled by the same generic mechanism, run by the local GC process on every
node:

- For each blob in the local `blob_store`, the count of *local* `FileRecord`s
  (tombstoned-and-purged records don't count; live records pointing elsewhere
  don't count) currently referencing that `content_hash` is checked periodically.
- The instant a blob's local reference count drops to zero, it becomes a **GC
  candidate** with a recorded "became unreferenced at" timestamp — it is not
  deleted yet.
- Only after the same grace TTL has elapsed since a blob became a candidate, and
  its reference count is still zero at that point, is it physically deleted.
- The TTL here serves the same purpose as for tombstones: a peer that is still
  partitioned may hold a stale `FileRecord` pointing at this blob and will need
  it once anti-entropy catches that peer up — deleting immediately on the first
  node to drop its reference would break that peer's read.
- For the tombstone-purge path specifically, this means a blob's total retention
  after the original delete is roughly **two grace TTLs**, not one: one TTL for
  the `FileRecord` itself to become purge-eligible (step 3 above), plus another
  TTL after the blob becomes reference-count-zero before it is physically
  deleted. This stacking is intentional — it errs toward over-retention rather
  than under-retention — not an oversight.

**TTL in the demo:** the grace TTL is a `config.py` setting, not hardcoded. The
default (24h) is the production-realistic value; the Docker Compose demo
overrides it to a short duration (e.g. minutes) so tombstone purge, blob GC, and
post-partition reconciliation are all observable within a live demo session.

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
PATCH  /files/{file_id}            # update name and/or content pointer
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
    gc.py                      # purge of expired tombstones and orphaned blobs
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
   blob purge (covering both tombstone-purge orphans and `PATCH`-repoint
   orphans, §10).
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
- Version history: `PATCH` replaces a file's current content pointer, it does
  not create a retrievable past version. `GET /files/{file_id}` only ever
  returns the current content; there is no API to fetch a prior version of a
  file's content.
