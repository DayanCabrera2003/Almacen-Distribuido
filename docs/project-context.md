# Project Context — Tag-Based Distributed File Store

This document tracks the assignment brief and the design decisions made so far, so
any future session (with or without prior conversation history) can pick up the
project without re-deriving context. It will be superseded by a formal design spec
once the full architecture discussion is finished; until then, this is the source
of truth.

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

## Still to be decided (remaining brainstorming blocks)

- Storage layer details: local persistence for blobs (filesystem) and metadata
  (likely SQLite per node) — not yet finalized.
- Replication/placement mechanics: rendezvous hashing parameters, replication
  factor R, write/read quorum behavior for content.
- Concurrency & conflict resolution: CRDT choice for tags (leaning OR-Set),
  LWW-register vs. sibling-versions for single-value fields (name, content
  pointer), vector clocks for causality tracking.
- Deletion semantics: tombstone-based soft delete, propagation via anti-entropy,
  GC/purge policy, reference counting for deduplicated blobs.
- Anti-entropy / reconciliation mechanics: push-pull gossip of metadata state,
  possible Merkle-tree diffing for efficiency at larger scale.
- Full tech stack (FastAPI, grpcio, SQLite, etc.) — to be confirmed as a block.
- Repository/package structure (atomic files, separated responsibilities).
- Testing strategy, chaos testing (partition injection), observability.
- Phased roadmap for solo, incremental implementation.

## Process note

This project is being designed via the `brainstorming` workflow: clarifying
questions → proposed approaches → design presented in approved sections → written
spec → spec review → implementation plan. This file is an intermediate checkpoint,
not the final spec. The formal spec will land under
`docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md` once the full design is
approved.
