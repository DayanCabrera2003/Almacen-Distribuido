# Project Context — Tag-Based Distributed File Store

This document preserves the original assignment brief and the early framing that
led to the architecture, so any future session can pick up the project without
re-deriving context.

**It is no longer the source of truth for the design.** That is
`docs/superpowers/specs/2026-09-28-tag-based-distributed-store-design.md`, which
is approved and supersedes every design statement below. Where the two disagree,
the spec wins. What is kept here is the brief (verbatim, as the requirement of
record) and the reasoning behind the earliest decisions, which the spec states as
conclusions without re-arguing them.

## Assignment brief (original, verbatim)

> Almacén distribuido de archivos basado en etiquetas
>
> **Descripción**
> La organización por etiquetas permite que un mismo archivo pertenezca a varias
> colecciones lógicas. En un sistema distribuido, los datos y los metadatos pueden
> replicarse de forma distinta, por lo que la frescura de ambos puede divergir y
> generar problemas de consistencia.
>
> **Objetivo general**
> Construir un almacén distribuido donde cada archivo tenga identidad estable,
> contenido, nombre y conjunto de etiquetas, con operaciones de alta, eliminación,
> listado y modificación de etiquetas.
>
> **Funcionalidades y alcance**
> - Identidad estable de archivos.
> - Almacenamiento distribuido del contenido.
> - Índices basados en etiquetas.
> - Operaciones para agregar y eliminar etiquetas.
> - Consultas sobre combinaciones de etiquetas.
> - Definición de consistencia y durabilidad tanto para contenido como para metadatos.
> - Tratamiento de actualizaciones concurrentes.
> - Semántica clara de eliminación.
> - Reconciliación después de particiones.
>
> **Referencias para comparación arquitectónica**
> - CephFS: comparación para separar metadatos y datos distribuidos.
> - IPFS: comparación para identificar contenido independientemente del nombre o ubicación.
> - Investigación sobre sistemas de archivos semánticos o basados en etiquetas:
>   referencia histórica para organización centrada en metadatos.

## Project framing (from brainstorming)

- **Quality bar**: serious/portfolio-grade, not a minimal course demo. Should hold
  up as a real design, even though scope is scaled to one developer.
- **Team size**: solo developer.
- **Language/stack**: Python.
- **Deployment/demo environment**: Docker Compose — each node is a container;
  cluster simulated locally, with network partitions injectable (e.g. via
  `tc`/`iptables` or a proxy like toxiproxy) between containers.

## Architecture decisions made so far

### 1. Consistency model
**AP with reconciliation** (Dynamo-style), not CP/Raft-style consensus. Rationale:
the brief explicitly asks for "reconciliación después de particiones" and
"tratamiento de actualizaciones concurrentes" — both are AP/eventual-consistency
concerns. A CP design (majority quorum, minority stops accepting writes) would
contradict "reconcile after the partition heals," since a true CP system prevents
divergence instead of reconciling it after the fact.

- Metadata (file records + tags): eventually consistent, merged via CRDTs.
- Content: durability via replication to a subset of nodes (see placement below).

### 2. File identity model
**Stable ID ≠ content hash** (inode + blob style, not pure IPFS-style content
addressing). Each file gets a UUID that never changes for its lifetime. The file
record points to a `content_hash` (SHA-256 of the current blob). Updating a file's
content changes which blob the record points to, but the file's identity, name
history, and tags persist. This enables real versioning and keeps name/tags stable
across content updates, which a pure content-addressed identity (where identity
IS the hash) would not allow without manual linking.

### 3. Topology
Symmetric peer nodes — no dedicated coordinator, no separate metadata-server role
like CephFS's MDS. Every node runs the same binary and plays two roles
simultaneously:
- **Data node**: stores a shard of content blobs (content-addressed, SHA-256),
  placed via rendezvous hashing (HRW) across a replica set of size R out of N nodes.
- **Metadata node**: stores a full replica of all file metadata (records + tag
  index), kept eventually consistent via gossip + CRDT merge.

Rationale for not mirroring CephFS's MDS/OSD split: that separation earns its
complexity at datacenter scale (metadata needs different hardware/latency
characteristics than bulk data). At this project's scale (a handful of nodes,
demo/portfolio purposes), splitting roles across physical nodes adds operational
complexity without benefit. Instead, responsibilities are separated **in software**
(independent packages: `storage/`, `cluster/`, `crdt/`, etc.), not across node
types. This trade-off vs. CephFS will be written up explicitly in the final docs,
per the assignment's request for architectural comparison.

Full metadata replication (vs. sharding metadata like content) is a deliberate
choice: it makes tag-combination queries (AND/OR across tags) trivial and fast on
any single node, with no scatter-gather across the cluster — directly serving the
"tag index" and "queries over tag combinations" requirements. Content, being much
larger, is sharded/replicated only to R of N nodes.

### 4. Communication planes
- **Client-facing**: REST over HTTP (FastAPI) — upload/download files, add/remove
  tags, query by tag combination.
- **Internal (node-to-node)**: gRPC — blob replication, gossip membership
  exchange, metadata anti-entropy.

### 5. Membership & failure detection
Gossip protocol, SWIM-inspired but simplified, implemented by hand on top of gRPC
(no mature off-the-shelf SWIM library in Python, and it's a core learning/portfolio
component in its own right): periodic heartbeats, suspicion before declaring a
node dead, propagation of membership changes via gossip.

## Design questions that were open here — and where they were settled

Every item this document once listed as undecided is now resolved in the approved
spec. Kept as an index so the reasoning is findable rather than re-derived:

| Question | Settled in |
| --- | --- |
| Local persistence for blobs and metadata | spec §7 — flat filesystem for blobs, SQLite in WAL mode for metadata |
| Placement and replication parameters | spec §2 and §8 — HRW over `content_hash`, N=5, R=3, W=2 |
| Concurrency and conflict resolution | spec §9 — OR-Set for tags, LWW-register for name/content_hash/tombstone |
| Causality tracking | spec §6 — one vector clock per `FileRecord`, not per field |
| Deletion semantics | spec §10 — tombstones, grace TTL, reference-counted blob GC |
| Anti-entropy mechanics | spec §11 — push-pull gossip of `file_id → vector_clock` digests; Merkle diffing deferred |
| Tech stack | spec §13 |
| Repository structure | spec §14 |
| Testing and observability | spec §13 and §16 (phase 6) |
| Phased roadmap | spec §16 — six phases |

## Status

The design phase is complete; implementation is under way, one spec phase at a
time. Plans live in `docs/superpowers/plans/`. See `README.md` for what currently
works and how to run it.
