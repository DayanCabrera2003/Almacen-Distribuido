# Architectural comparison

The assignment asks for this design to be compared against three references:

- **CephFS** — on separating metadata from distributed data;
- **IPFS** — on identifying content independently of its name or location;
- **semantic and tag-based file systems** — the research lineage of
  metadata-centred organisation.

For each, this document summarises how the reference system works, what this
design borrowed from it, what it did differently and why, and what the
difference costs. Statements about the reference systems are limited to their
published designs and documentation; the sources are listed at the end. For the
design of this system itself, see [architecture.md](architecture.md).

---

## Summary

| | **This store** | **CephFS** | **IPFS** | **Semantic / tag FS** |
| --- | --- | --- | --- | --- |
| Goal | Tag-organised file store that survives partitions | POSIX file system at datacenter scale | Location-independent, content-addressed data distribution | Organise files by attributes instead of a hierarchy |
| Node roles | One symmetric node type | Monitors, OSDs, MDSs, clients | One peer type (plus optional pinning services / IPFS Cluster) | Usually a single machine |
| Metadata | Fully replicated on every node, merged by CRDTs | Owned by MDS daemons, partitioned by subtree, persisted in RADOS | Directory structure is itself content-addressed in the DAG; no tags | Attribute index on one machine |
| Content placement | Rendezvous hashing, R of N, computed | CRUSH, computed from the cluster map | Wherever a peer stores it; found through a DHT | Local disk |
| File identity | Stable UUID; content hash is a separate pointer | Inode number | The content's CID (identity *is* the hash) | Path or inode; queries resolve to files |
| Mutability | Mutable records over immutable blobs | Mutable files, objects written in place | Immutable; mutability layered on top (IPNS) | Mutable |
| Consistency | AP, eventual, convergent | Strong (POSIX-oriented), blocks rather than diverges | Immutable data needs none; IPNS records ordered by sequence number | Local |
| Durability | W=2 of R=3 per write | All replicas of the placement group (or erasure coding) | None by default; data persists while someone pins it | Local disk |
| Queries | AND / OR over tags, on any node | Path lookup | CID lookup | Attribute queries as virtual directories or search |

---

## 1. CephFS

### How it works

Ceph (Weil et al., OSDI 2006) is built on **RADOS**, a reliable distributed
object store. A Ceph cluster has several daemon types:

- **Monitors** keep the authoritative *cluster map* (which OSDs exist, which are
  up, the placement rules) and agree on it with a Paxos-based protocol, so a
  majority of monitors must be reachable for the map to change.
- **OSDs** (object storage daemons) store objects on local disks, replicate them
  to each other, and detect and recover from each other's failures.
- **MDS** (metadata server) daemons, used only by CephFS, manage the POSIX
  namespace: directories, inodes, permissions, and the capabilities that let
  clients cache safely.

File **data** never passes through the MDS. A file is striped into objects
(4 MiB by default) whose names derive from the inode number; each object is
hashed into a *placement group*, and **CRUSH** (Weil et al., SC 2006) maps each
placement group to an ordered list of OSDs, computed from the cluster map
rather than looked up. Clients therefore read and write objects directly on the
OSDs.

File **metadata** is served by the MDS cluster but persisted in RADOS itself
(a metadata pool, with a journal). With several active MDS daemons, the
namespace is divided by **dynamic subtree partitioning**: subtrees of the
directory hierarchy are assigned to, and migrated between, MDS daemons to
balance load.

Replication in RADOS is primary-copy: a write goes to the placement group's
primary OSD, which forwards it to the replicas and acknowledges once they have
all applied it. When too few replicas of a placement group are available (below
its `min_size`), I/O to it blocks rather than proceeding; when an OSD stays down
past a configurable interval it is marked *out*, CRUSH remaps its placement
groups, and the cluster re-replicates automatically.

### What this design borrowed

- **Separate metadata from data.** The assignment names CephFS for this, and the
  separation is kept: a `FileRecord` (name, tags, pointer, CRDT state) is
  stored, replicated and repaired by different code paths, with different rules,
  from the bytes it points to. Content is sharded and quorum-written; metadata is
  fully replicated and merged.
- **Computed placement instead of a lookup table.** Like CRUSH, rendezvous
  hashing lets every node compute a blob's replica set from the membership list
  alone, with no placement directory to store, replicate or keep consistent.
  The two are closely related: CRUSH's `straw2` bucket algorithm chooses items
  by a weighted pseudo-random draw per item, which is a weighted form of
  rendezvous hashing.
- **Data does not flow through the metadata path.** In CephFS clients talk to
  OSDs directly; here blob transfer (`Replication.PutBlob` / `GetBlob`) is a
  separate gRPC service from metadata replication (`Cluster.ReplicateRecord`,
  `Cluster.Gossip`).

### What this design does differently, and why

**No metadata server tier.** CephFS separates metadata from data across
*processes and machines*; this design separates them *in software* on one node
type. The MDS tier earns its complexity at datacenter scale, where metadata
traffic needs its own servers and can be scaled independently of storage. At
five nodes it would add a second daemon to deploy, a second failure domain, and
a routing step, for no measurable benefit. The cost of the choice is that
metadata cannot be scaled separately from storage.

**Full metadata replication instead of partitioning.** CephFS partitions the
namespace across MDS daemons, which lets metadata capacity grow with the number
of MDSs but requires a client to find the right MDS for each path. Here every
node holds every record, which makes tag queries — the operation the assignment
centres on — local to whichever node receives them, and keeps them available
during a partition. CephFS can partition by subtree because a hierarchy has
natural subtrees; a tag query cuts across any partitioning, so partitioned
metadata would force a scatter-gather for every query. The cost is that metadata
storage and replication traffic grow with the number of nodes, which caps this
design well below Ceph's scale.

**AP instead of strong consistency.** CephFS provides POSIX semantics: what one
client writes, another reads, coordinated by MDS capabilities and RADOS's
all-replica acknowledgement, and a placement group that cannot reach enough
replicas stops serving. This design does the opposite on purpose: every node
keeps accepting metadata changes during a partition and reconciles afterwards,
because reconciling after partitions is a requirement of the assignment. The
price is weaker semantics: no read-your-writes across nodes, and last-writer-wins
for concurrent renames (see architecture §12).

**Quorum writes instead of all-replica writes.** RADOS acknowledges after every
replica in the acting set has the write; this design acknowledges after W=2 of
R=3. That tolerates one slow or unreachable replica per write without blocking,
which suits an AP system, and is safe for content specifically because blobs are
immutable and hash-verified, so a lagging replica can never serve a wrong
version — only a missing one, which anti-entropy fills.

**Static membership, no automatic recovery.** Ceph's monitors maintain a
consensus-agreed, versioned cluster map, and a lost OSD triggers
re-replication. Here membership is a static peer list and placement ignores
liveness, so a permanently lost node leaves its blobs under-replicated until an
operator intervenes. A Paxos-backed map was out of proportion with the project;
this is the largest operational gap relative to Ceph.

**Objects are immutable and content-addressed.** RADOS objects are named by
inode and stripe and updated in place. Blobs here are named by their SHA-256 and
never modified, which removes write conflicts at the content level entirely and
makes every read self-verifying, at the cost of no partial updates: changing one
byte of a file writes a whole new blob.

**Tags, not a hierarchy.** CephFS is a POSIX tree; this store has a flat set of
files with tags and no directories, because the assignment is about tag-based
organisation. It is not mountable, and existing tools cannot use it without the
REST API.

---

## 2. IPFS

### How it works

IPFS (Benet, 2014) identifies data by a **content identifier (CID)**: a
self-describing encoding of a cryptographic hash of the content (by default
SHA-256), plus the codec used to interpret it. Files are split into blocks
(256 KiB by default) arranged in a **Merkle DAG**: the file's CID is the hash of
its root node, which links to its blocks by their CIDs; directories are DAG
nodes linking to their entries. Any change to a file changes its CID and the
CIDs of every directory above it.

To find data, a peer asks a **Kademlia-based distributed hash table** which
peers have announced that they can provide a CID (provider records), then
fetches blocks from them with the **Bitswap** exchange protocol, verifying each
block against its hash. Data is location-independent: the same CID can be
served by any peer, and the receiver can verify it without trusting the sender.

Content is **immutable**. Mutable references are layered on top: an **IPNS**
name is derived from a public key and resolves, through a signed record with a
sequence number, to a CID that its owner can update.

IPFS makes no durability promise by itself. A node keeps the blocks it adds or
fetches in a local cache; blocks that are not **pinned** can be removed by its
garbage collector. Data survives only while some peer pins it — the role of
pinning services and of IPFS Cluster, a separate tool that coordinates a shared
pinset across a group of IPFS peers.

### What this design borrowed

- **Content addressing for bytes.** Blobs are stored under the SHA-256 of their
  content, exactly as IPFS blocks are under their hash. This gives:
  - **integrity** — every read, from local disk or from a peer, is re-hashed and
    rejected on mismatch, and `PutBlob` refuses content whose hash differs from
    the declared one;
  - **deduplication** — identical content is stored once;
  - **immutability** — a blob's replicas cannot disagree, which is why content
    needs no conflict resolution and why reading a single replica is safe
    (architecture §7).
- **Verify, don't trust.** As in Bitswap, a node does not trust a peer's bytes;
  it checks them against the address it asked for.

### What this design does differently, and why

**Identity is a stable UUID, not the content hash.** In IPFS a file's identity
*is* its CID, so editing a file produces a different object, and anything
attached to the old CID (a name, a link, tags) must be re-pointed. The
assignment requires stable identity for files whose content, name and tags can
all change. Here `file_id` never changes; `content_hash` is a mutable pointer
inside the record. Content addressing is used where it helps — the immutable
blob layer — and not for identity. The `FileRecord` plays a role similar to an
IPNS name (a stable handle onto changing immutable content), with two
differences: an IPNS record has a single owner who signs every update, whereas a
`FileRecord` can be changed concurrently on any node and is merged with CRDTs;
and the record carries the file's name and tags, not just a pointer.

**Deterministic placement instead of discovery.** In IPFS content lives wherever
someone decided to store it, and must be found through DHT lookups. Here
rendezvous hashing decides where a blob lives, and every node can compute it
with no lookup. That only works in a closed cluster with a known membership —
which is the setting of this project and exactly what IPFS is designed not to
assume.

**Durability is part of the write.** An IPFS `add` stores data on one node; how
many copies exist later depends on who pins it. Here an upload returns success
only after W=2 of the blob's R=3 replicas stored it, and anti-entropy keeps
filling in the third. The assignment asks for defined durability, which a
pin-based model leaves to the operator.

**Garbage collection tied to references, with a grace period.** IPFS GC removes
whatever is unpinned on one node. Here a blob is collected only after no record
on the node has referenced it for a full grace TTL, and tombstones themselves are
only purged after a TTL, because in a replicated mutable system a premature
deletion can be undone by a peer that has not yet heard of it — a problem
immutable, independently-pinned data does not have. For the same reason a node
refuses to re-adopt a tombstone that is already past the TTL, so a purged record
stays purged (architecture §9).

**Whole-file blobs, no Merkle DAG.** IPFS chunks files into a DAG, which gives
deduplication of shared blocks across files, verified streaming of large files,
and partial retrieval. Here a file is one blob with one hash: simpler, and
sufficient for the target scale, but identical regions of different files are
not shared, a one-byte edit writes a whole new blob, and the current
implementation holds a whole file in memory while hashing and transferring it
(it is streamed between nodes in 1 MiB chunks, but addressed as a unit).

**A trusted cluster, not an open network.** IPFS is permissionless and must
tolerate untrusted peers. This design assumes cooperative nodes, with no
authentication between them; it trades that robustness for simplicity.

---

## 3. Semantic and tag-based file systems

### How they work

The idea that files should be found by what they *are* rather than where they
are filed has a long research history. Representative points:

- **Semantic File System** (Gifford, Jouvelot, Sheldon and O'Toole, SOSP 1991).
  Attributes are extracted from files automatically by *transducers* (for
  example sender and subject from mail). Queries are expressed as **virtual
  directories**: a path such as `/sfs/owner:/smith` lists the files with that
  attribute value, and nesting further attribute directories narrows the result
  (a conjunction). It was exposed through the ordinary file-system interface
  (served over NFS), so unmodified tools could use it.
- **BeOS's Be File System** (described by Giampaolo, 1999). Files carry typed
  extended attributes, selected attributes are **indexed** in B+trees, and the
  file system itself answers queries over them, including *live* queries that
  keep notifying the caller as matching files appear or disappear.
- **Placeless Documents** (Dourish et al., Xerox PARC, around 2000). Documents
  are organised by user-assigned and computed properties rather than by
  location, with properties that can be specific to each user.
- **"Hierarchical File Systems are Dead"** (Seltzer and Murphy, HotOS 2009).
  Argues that the directory hierarchy is the wrong primary namespace for modern
  storage and proposes a design (hFAD) whose naming is based on tags and search,
  with the hierarchy as just one possible view.
- **Tag file systems on FUSE**, such as Tagsistant, expose tags as directories
  so that ordinary path navigation becomes tag intersection.

What these designs share: **many-to-many organisation** (a file belongs to every
collection whose attributes it has), **queries as the primary means of finding
files**, and an **index over metadata** as the core data structure. With few
exceptions they target a single machine, and the hard problems they address are
index maintenance, query expressiveness, and compatibility with
path-based tools — not replication.

### What this design borrowed

- **Tags as first-class metadata.** A file's tag set is part of its record, not
  a side table bolted onto a hierarchy. There are no directories at all: tags are
  the only organisation, which is what the assignment asks for ("one file can
  belong to several logical collections").
- **Queries over combinations of attributes.** `GET /files?tags=a,b&mode=and`
  is the conjunctive query of the Semantic File System's nested virtual
  directories; `mode=or` adds disjunction.
- **Identity independent of organisation.** As in hFAD's proposal, a file is
  named by an identifier that has nothing to do with where it is filed; adding or
  removing a tag never changes it.
- **The index must be cheap to query everywhere.** The research systems treat
  the attribute index as the heart of the system. Here that conviction is what
  drives full metadata replication: every node holds the whole index, so any
  query is answered locally.

### What this design does differently, and why

**Distributed and concurrently edited.** The research systems were, almost all,
single-machine: an attribute update is a local transaction. Here the same file's
tags can be edited on two partitioned nodes at once, and both edits must
survive. That is a problem they did not have to solve, and it is why the tag set
is an **observed-remove set** (a concurrent add survives a concurrent remove)
rather than a plain set, with its full add/remove history replicated.

**An API, not a file system.** The Semantic File System and the FUSE tag file
systems put queries behind path names so existing tools keep working. This
store exposes a REST API and is not mountable. That keeps the query semantics
explicit (a path-encoded query has to invent conventions for disjunction and for
"is this a tag or a file") and keeps the project focused on the distributed
problems, at the cost of tool compatibility.

**Manual tags, not extracted attributes.** Tags are whatever clients assign;
there are no transducers or content indexing. Attributes are plain strings, not
typed key-value pairs.

**A small query language.** One mode per query — all of the tags, or any of
them. No negation, no nesting of AND inside OR, no ranges over typed attributes,
and no live queries. These would be straightforward extensions over a local
index; they were not needed by the assignment.

**A scan instead of an index structure.** BFS keeps B+tree indexes; a tag file
system would keep posting lists per tag. `TagIndex` currently filters every live
record on each query — O(live files) per query. At the target scale this is
simpler and fast enough, and correctness (tombstoned files never appear) is easy
to see. An inverted index (tag → file ids) maintained on each merge is the
obvious next step if the corpus grows.

---

## 4. Other influences

The three references above were requested by the assignment. The mechanisms of
this design come mostly from elsewhere, and are listed here so the comparison is
complete:

- **Amazon Dynamo** (DeCandia et al., SOSP 2007) — the overall model: an AP
  store with N/R/W replication, gossip-based membership, and anti-entropy.
  Differences: Dynamo places keys on a consistent-hashing ring with virtual
  nodes; this design uses rendezvous hashing. Dynamo uses a *sloppy* quorum with
  hinted handoff so writes succeed whenever any W healthy nodes exist; this
  design uses a strict replica set, so a write fails if two of its three
  replicas are down. Dynamo returns conflicting versions (siblings) to the
  client for reconciliation; this design merges automatically with CRDTs and
  logs the LWW conflicts it detects when a record is pushed to a node
  (not those first met on the gossip pull). Dynamo uses Merkle trees for anti-entropy; this design
  exchanges a flat `file_id → vector clock` digest and defers Merkle trees.
- **Rendezvous hashing** (Thaler and Ravishankar, 1998) — placement.
- **SWIM** (Das, Gupta and Motivala, DSN 2002) — suspicion before declaring a
  node dead, refutation by incarnation number, and dissemination by gossip.
  Simplified here: no indirect probing through other members, and the gossip
  exchange itself serves as the probe.
- **CRDTs** (Shapiro, Preguiça, Baquero and Zawirski, 2011) — LWW-registers and
  the observed-remove set; state-based merge that is commutative, associative
  and idempotent.
- **Vector clocks** (Fidge 1988; Mattern 1989) — causality per record, used to
  detect concurrency and to drive anti-entropy, not to resolve conflicts.

---

## Sources

- S. A. Weil, S. A. Brandt, E. L. Miller, D. D. E. Long, C. Maltzahn. *Ceph: A
  Scalable, High-Performance Distributed File System.* OSDI 2006.
- S. A. Weil, S. A. Brandt, E. L. Miller, C. Maltzahn. *CRUSH: Controlled,
  Scalable, Decentralized Placement of Replicated Data.* SC 2006.
- S. A. Weil, A. W. Leung, S. A. Brandt, C. Maltzahn. *RADOS: A Scalable,
  Reliable Storage Service for Petabyte-scale Storage Clusters.* PDSW 2007.
- Ceph documentation: CephFS architecture, MDS, placement groups and CRUSH maps
  (docs.ceph.com).
- J. Benet. *IPFS — Content Addressed, Versioned, P2P File System.* 2014.
- IPFS documentation: content identifiers, Merkle DAGs, DHT, Bitswap, IPNS,
  pinning and garbage collection (docs.ipfs.tech).
- D. K. Gifford, P. Jouvelot, M. A. Sheldon, J. W. O'Toole Jr. *Semantic File
  Systems.* SOSP 1991.
- D. Giampaolo. *Practical File System Design with the Be File System.* Morgan
  Kaufmann, 1999.
- P. Dourish et al. *Extending Document Management Systems with User-Specific
  Active Properties.* ACM TOIS, 2000 (Placeless Documents).
- M. Seltzer, N. Murphy. *Hierarchical File Systems are Dead.* HotOS 2009.
- G. DeCandia et al. *Dynamo: Amazon's Highly Available Key-value Store.* SOSP
  2007.
- D. G. Thaler, C. V. Ravishankar. *Using Name-Based Mappings to Increase Hit
  Rates.* IEEE/ACM Transactions on Networking, 1998.
- A. Das, I. Gupta, A. Motivala. *SWIM: Scalable Weakly-consistent
  Infection-style Process Group Membership Protocol.* DSN 2002.
- M. Shapiro, N. Preguiça, C. Baquero, M. Zawirski. *Conflict-free Replicated
  Data Types.* SSS 2011.
