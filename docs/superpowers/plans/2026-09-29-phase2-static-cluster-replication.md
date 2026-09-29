# Phase 2 — Static Cluster & Content Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the working single node into a 5-node cluster where content blobs are replicated to R=3 of N=5 nodes chosen by rendezvous hashing and acknowledged after W=2 writes, and where file metadata is pushed to every node so any node can answer any query — all on the happy path, with no failure handling, gossip, or CRDTs yet.

**Architecture:** Two new packages on top of the Phase 1 layers. `almacen/cluster/` holds the distributed *decisions* — `placement.py` (pure HRW function, no I/O), `replication_client.py` (W-of-R quorum orchestration), `metadata_replicator.py` + `replicated_metadata_store.py` (push metadata to peers). `almacen/rpc/` holds the node-to-node *transport* — two `.proto` files, their generated stubs, and the two servicers that implement them against the existing `BlobStore`/`MetadataStore`. `main.py` grows to start a gRPC server alongside FastAPI. The REST API's shape does not change at all: the same nine endpoints, now backed by a cluster.

**Tech Stack:** Everything from Phase 1, plus `grpcio` + `grpcio-tools` (verified to ship cp314 wheels, 1.84.0), `concurrent.futures.ThreadPoolExecutor` for parallel replica writes, and Docker Compose for the 5-node demo.

**Spec:** `docs/superpowers/specs/2026-09-28-tag-based-distributed-store-design.md` — §2 (N=5, R=3, W=2), §4 (symmetric nodes, two communication planes), §8 (HRW placement, write/read paths), §12 (gRPC service definitions), §14 (repository structure), §16 (this is phase 2 of 6).

---

## Scope: what Phase 2 does and does not include

**In scope** (from spec §16: *"Cluster without failures — gRPC replication, static membership (fixed peer list, no gossip yet), HRW placement, happy-path W/R replication, N-node Docker Compose"*):

- Rendezvous hashing (HRW) to choose a blob's replica set.
- gRPC transport between nodes: blob put/get, metadata record push, ping.
- W-of-R quorum on the content write path; read-with-fallback on the content read path.
- Full metadata replication by direct push to every peer.
- Static membership: every node is handed the full peer list at boot and it never changes.
- A 5-node Docker Compose cluster and an in-process multi-node integration test.

**Explicitly out of scope, by phase:**

| Deferred to | What |
|---|---|
| Phase 3 | CRDTs (`or_set.py`, `lww_register.py`, `vector_clock.py`). Phase 2 metadata merge is last-writer-wins-by-arrival, which is *wrong* under concurrency — that is exactly what Phase 3 fixes. |
| Phase 4 | Gossip/SWIM membership, push-pull anti-entropy, partition injection. Phase 2 metadata push is best-effort: if a peer is down its copy silently diverges, and nothing repairs it. |
| Phase 5 | Tombstone TTL, reference counting, orphaned-blob GC. |
| Phase 6 | `GET /cluster/status`, structlog, Typer CLI, chaos tests. |

**Do not** build toward those while implementing this phase. The point of phasing is that each layer is demoable on its own; adding a vector clock "while we're here" makes Phase 3's plan unimplementable as written.

---

## Decisions made while planning (not pinned down by the spec)

The spec fixes the mechanisms and the numbers but leaves these mechanics open. Resolved here:

1. **Metadata replication in Phase 2 uses a direct push RPC, not gossip.** The spec's `Cluster` service (§12) lists only `Gossip` and `Ping`, because gossip is Phase 4. But without *some* metadata replication, a file uploaded on node 1 is invisible on node 2, and the phase has no coherent demo (spec §16 requires each phase be "independently demoable"). So Phase 2 adds `ReplicateRecord(FileRecordMsg) -> ReplicateAck` to the `Cluster` service: the coordinator writes locally, then pushes the record to every peer. This is the honest happy-path predecessor of anti-entropy — Phase 4's push-pull gossip *subsumes* it by handling the case where the push failed, and `ReplicateRecord` stays useful as the fast path.

2. **The metadata push is best-effort and never fails the client request.** Phase 2 assumes no failures, but a dead peer must not turn a successful local write into a 500. Failures are logged and swallowed; the resulting divergence is Phase 4's problem by design. (Content writes are the opposite — they *do* enforce a quorum, because durability of content is a spec §3 guarantee.)

3. **The coordinator does not keep a local copy of a blob it is not a replica for.** Storing one anyway would quietly defeat HRW placement and make the "content is sharded across R of N" claim false. A consequence worth expecting: after uploading to node 1, the blob may not exist in node 1's `blobs/` directory, and node 1 serves downloads by fetching from a replica.

4. **Effective R and W shrink to the cluster size.** With N < R (notably N=1 in every Phase 1 test and in local dev), the replica set is all N nodes and the required acks are `min(W, len(replica_set))`. Without this, all 39 existing tests break and single-node development becomes impossible.

5. **A blob that no replica can serve is a 503, not a 404 or a 500.** Phase 1 raised `RuntimeError` (→ 500) because a missing local blob could only mean local corruption. In a cluster it means "every replica is unreachable right now", which is a transient availability problem: `503 Service Unavailable`. A 404 would be a lie (the file exists), and a 500 would be wrong (the node is not broken).

6. **Generated protobuf stubs are committed to the repository.** They are build output, which argues for ignoring them, but committing them keeps `pip install -e . && pytest` working with no build step, keeps the Dockerfile trivial, and makes the repo readable on GitHub. A `scripts/gen_protos.sh` regenerates them and the README documents when to run it.

7. **The multi-node integration test runs in-process, not under Docker.** Five real gRPC servers on loopback ports with five separate temp data directories exercise the same code paths as Compose, in well under a second, with no Docker dependency in CI. Compose is for the human demo; the test suite is the correctness guard.

8. **Metadata replication is wired in as a decorator around `MetadataStore`, not as calls in each router.** There are five mutation sites (upload, patch, delete, add tags, remove tag); adding a push call to each is five chances to forget one. A `ReplicatedMetadataStore` wrapper that replicates after `insert`/`update` gets all five right by construction, and leaves the routers untouched.

---

## File Structure

New and modified files, with each one's single responsibility:

```
pyproject.toml                          # MODIFY: grpcio, grpcio-tools deps
scripts/gen_protos.sh                   # NEW: regenerate stubs from .proto
almacen/
  config.py                             # MODIFY: node_id, grpc_port, peers, R, W + validation
  main.py                               # MODIFY: build cluster wiring, start gRPC server
  storage/
    protocols.py                        # NEW: MetadataStoreLike Protocol (honest typing for the wrapper)
    metadata_store.py                   # MODIFY: add upsert() for applying remote records
  rpc/
    __init__.py                         # NEW
    replication.proto                   # NEW: PutBlob / GetBlob
    cluster.proto                       # NEW: ReplicateRecord / Ping
    *_pb2.py, *_pb2_grpc.py, *.pyi      # NEW: generated, committed
    replication_servicer.py             # NEW: serves blobs from the local BlobStore
    cluster_servicer.py                 # NEW: applies pushed records, answers Ping
    server.py                           # NEW: build/start/stop the gRPC server
  cluster/
    __init__.py                         # NEW
    placement.py                        # NEW: HRW replica_set() — pure, no I/O
    replication_client.py               # NEW: W-of-R quorum write, read with fallback
    metadata_replicator.py              # NEW: push a record to every peer, best-effort
    replicated_metadata_store.py        # NEW: MetadataStore decorator that replicates writes
  api/
    deps.py                             # MODIFY: provide replication client; retype to Protocol
    routers/files.py                    # MODIFY: use replication client; 503 on unavailable blob
docker/
  Dockerfile                            # NEW
  docker-compose.yml                    # NEW: 5 nodes
tests/
  __init__.py                           # NEW: makes `tests` importable (see Task 1)
  unit/__init__.py                      # NEW: same
  integration/__init__.py               # NEW: same
  helpers.py                            # NEW: free_port(), shared across packages
  unit/cluster/test_placement.py        # NEW: HRW properties
  unit/test_config.py                   # NEW: peer parsing + validation
  integration/storage/test_metadata_store.py   # MODIFY: upsert tests
  integration/rpc/test_replication_servicer.py # NEW: blob put/get over a real channel
  integration/rpc/test_cluster_servicer.py     # NEW: record push applies locally
  integration/cluster/test_replication_client.py # NEW: quorum behaviour
  cluster/test_five_node_cluster.py     # NEW: in-process 5-node end-to-end
README.md                               # MODIFY: cluster setup, env vars, Compose, proto regen
DAA/rendezvous-hashing.md               # NEW (untracked): HRW analysis
DAA/write-quorum-and-read-repair.md     # NEW (untracked): why W=2 of R=3, read-one safety
```

`cluster/` holds decisions, `rpc/` holds transport. Keeping them apart is what lets Phase 3 swap the merge rule and Phase 4 swap membership without touching the wire format, and lets `placement.py` stay a pure function that unit-tests in microseconds.

---

### Task 1: Dependencies, proto generation script, and test package layout

**Files:**
- Modify: `pyproject.toml`
- Create: `scripts/gen_protos.sh`
- Create: `tests/__init__.py`, `tests/unit/__init__.py`, `tests/integration/__init__.py` (all empty)
- Create: `tests/helpers.py`

- [ ] **Step 1: Add the gRPC dependencies**

In `pyproject.toml`, extend `dependencies`:

```toml
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.32",
    "python-multipart>=0.0.12",
    "grpcio>=1.84",
    "protobuf>=7.35.1",
]
```

**`protobuf` must be listed explicitly, and this is not optional.** It was
verified on this machine that:

- `grpcio` depends only on `typing-extensions` — it does **not** pull in
  `protobuf`. Only `grpcio-tools` does, and that is a dev-only dependency.
- Every generated `*_pb2.py` begins with a hard runtime check:
  `_runtime_version.ValidateProtobufRuntimeVersion(..., 7, 35, 1, ...)`.

So a runtime-only install (which is exactly what the Docker image in Task 15
does) would import the committed stubs with no `protobuf` present at all, or with
an older one, and die at import with
`VersionError: gencode 7.35.1 runtime 6.33.6 ...`. The floor must match the
gencode emitted by the `grpcio-tools` version used to generate the stubs; if you
regenerate with a newer `grpcio-tools`, raise this floor to match.

Add `grpcio-tools>=1.84` to `[project.optional-dependencies] dev` — it is a
build-time tool, so only developers regenerating stubs need it:

```toml
dev = [
    "pytest>=8.0",
    "httpx>=0.27",
    "grpcio-tools>=1.84",
]
```

- [ ] **Step 2: Install them**

Run: `pip install -e ".[dev]"`
Expected: `grpcio`, `protobuf` and `grpcio-tools` install successfully (cp314
wheels exist for grpcio 1.84.0 — verified).

- [ ] **Step 2b: Confirm the runtime/gencode versions agree**

Run: `pip list | grep -i -e grpcio -e protobuf`
Expected: `protobuf` is at least 7.35.1. If it is older, the committed stubs
generated in Task 5 will fail to import, and you must either upgrade `protobuf`
or regenerate with a matching `grpcio-tools`.

Beware of a subtler version of the same trap while developing: running `python`
**without** the virtualenv activated can pick up a system-wide `protobuf` from
`~/.local/lib/...` and produce this exact `VersionError` even though the venv is
configured correctly. Always work with the venv activated.

- [ ] **Step 3: Write the generation script**

```bash
# scripts/gen_protos.sh
#!/usr/bin/env bash
# Regenerate the gRPC stubs from the .proto files.
# Run from the repository root after editing any .proto:
#   ./scripts/gen_protos.sh
#
# --proto_path=. (the repo root) is what makes the generated *_pb2_grpc.py use
# package-qualified imports (`from almacen.rpc import replication_pb2`) instead
# of bare top-level ones, which would fail to import inside the package.
set -euo pipefail

cd "$(dirname "$0")/.."

python -m grpc_tools.protoc \
    --proto_path=. \
    --python_out=. \
    --grpc_python_out=. \
    --pyi_out=. \
    almacen/rpc/replication.proto \
    almacen/rpc/cluster.proto

echo "Generated stubs in almacen/rpc/"
```

- [ ] **Step 4: Make it executable**

Run: `chmod +x scripts/gen_protos.sh`

- [ ] **Step 5: Make `tests/` a real package — this is load-bearing, not tidying**

Create three empty files:

```bash
touch tests/__init__.py tests/unit/__init__.py tests/integration/__init__.py
```

Today only the *leaf* test directories have `__init__.py`. Under pytest's default
prepend import mode each leaf therefore becomes a **top-level** package named
after itself, and the repository root never lands on `sys.path`. This phase breaks
that arrangement in two ways at once, both reproduced before this plan was
finalized:

1. **Duplicate top-level names.** This plan adds `tests/unit/rpc/` *and*
   `tests/integration/rpc/`, plus `tests/unit/cluster/` *and*
   `tests/integration/cluster/`. Two directories cannot both own the top-level
   name `rpc`, and collection then dies for the **entire** suite:
   `Interrupted: 2 errors during collection`. Not one failing test — zero tests
   run, which makes every later "Expected: N passed" step unreachable.
2. **`tests.` imports do not resolve.** Several test modules below import a shared
   helper as `from tests.helpers import free_port`. Without `tests/__init__.py`
   that raises `ModuleNotFoundError: No module named 'tests'` under the `pytest`
   console script. It *appears* to work under `python -m pytest`, because `-m`
   adds the working directory to `sys.path` and `tests` then resolves as a PEP 420
   namespace package — do not be fooled by that, since every command in this plan
   uses the `pytest` script.

Adding `tests/__init__.py` makes pytest's basedir walk reach the repository root
and put *that* on `sys.path`, which fixes both problems together. Verified: with
these three files the `pytest` console script collects duplicate-named directories
and resolves `tests.*` imports. A root-level `conftest.py` is **not** a substitute
— that was tried and still fails on the duplicate names.

- [ ] **Step 6: Add the shared test helper**

`free_port` is needed by the RPC, cluster and end-to-end tests, so it lives in one
importable module. It is deliberately **not** in a `conftest.py`: pytest's own
documentation discourages importing from conftest files, because pytest may load
them more than once.

```python
# tests/helpers.py
"""Helpers shared across test packages."""
from __future__ import annotations

import socket


def free_port() -> int:
    """Ask the OS for an unused loopback port.

    Binding to port 0 and reading back the assignment avoids the hard-coded-port
    collisions that make multi-node tests flaky when a previous run has not yet
    released its sockets.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
```

- [ ] **Step 7: Verify the suite still runs under the console script**

Run: `pytest -q`
Expected: 39 passed. Use the `pytest` script here, not `python -m pytest` — the
whole point of this step is that the console script works.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml scripts/gen_protos.sh tests/__init__.py tests/unit/__init__.py tests/integration/__init__.py tests/helpers.py
git commit -m "Add gRPC dependencies, proto generation script, and test package layout"
```

---

### Task 2: Rendezvous hashing (HRW) placement

This is the algorithmic core of the phase and it is a pure function — no I/O, no
network, no state. Test it like one.

**Files:**
- Create: `almacen/cluster/__init__.py` (empty), `almacen/cluster/placement.py`
- Test: `tests/unit/cluster/__init__.py` (empty), `tests/unit/cluster/test_placement.py`

- [ ] **Step 1: Write the failing tests**

Note the properties being tested. The first three are basic contract; the last
two are the *reasons* HRW was chosen over modulo hashing, and they are what would
silently regress if someone "simplified" the implementation later.

```python
# tests/unit/cluster/test_placement.py
import pytest

from almacen.cluster.placement import replica_set

NODES = ["node1", "node2", "node3", "node4", "node5"]


def test_returns_exactly_r_nodes():
    result = replica_set("abc123", NODES, r=3)
    assert len(result) == 3
    assert len(set(result)) == 3, "replica set must not contain duplicates"
    assert set(result) <= set(NODES)


def test_is_deterministic():
    first = replica_set("abc123", NODES, r=3)
    second = replica_set("abc123", NODES, r=3)
    assert first == second


def test_is_independent_of_node_list_order():
    # Two nodes handed the same membership in a different order must agree on
    # the replica set, or a blob would be written to one set and read from
    # another.
    forward = replica_set("abc123", NODES, r=3)
    backward = replica_set("abc123", list(reversed(NODES)), r=3)
    assert forward == backward


def test_returns_all_nodes_when_cluster_smaller_than_r():
    assert replica_set("abc123", ["node1"], r=3) == ["node1"]
    assert sorted(replica_set("abc123", ["node1", "node2"], r=3)) == ["node1", "node2"]


def test_rejects_empty_cluster():
    with pytest.raises(ValueError):
        replica_set("abc123", [], r=3)


def test_rejects_non_positive_r():
    with pytest.raises(ValueError):
        replica_set("abc123", NODES, r=0)


def test_removing_a_node_only_moves_keys_it_was_hosting():
    # This is the whole point of HRW over `hash(key) % N`: losing one node must
    # not reshuffle the placement of keys that node was not part of.
    hashes = [f"hash-{i}" for i in range(500)]
    before = {h: replica_set(h, NODES, r=3) for h in hashes}

    survivors = [n for n in NODES if n != "node3"]
    after = {h: replica_set(h, survivors, r=3) for h in hashes}

    for h in hashes:
        if "node3" not in before[h]:
            assert after[h] == before[h], (
                f"placement of {h} changed although node3 never hosted it"
            )
        else:
            # It lost one replica and gained exactly one replacement; the two
            # replicas it kept stay put.
            kept = [n for n in before[h] if n != "node3"]
            assert set(kept) <= set(after[h])


def test_distributes_keys_roughly_evenly():
    hashes = [f"hash-{i}" for i in range(3000)]
    counts = dict.fromkeys(NODES, 0)
    for h in hashes:
        for node in replica_set(h, NODES, r=3):
            counts[node] += 1

    expected = 3000 * 3 / len(NODES)  # 1800 placements per node
    for node, count in counts.items():
        assert abs(count - expected) < expected * 0.15, (
            f"{node} got {count} placements, expected ~{expected}"
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/cluster/test_placement.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almacen.cluster'`.

- [ ] **Step 3: Write `placement.py`**

```python
# almacen/cluster/placement.py
"""Rendezvous hashing (HRW) — choosing which nodes store a given blob."""
from __future__ import annotations

import hashlib
from collections.abc import Sequence


def replica_set(content_hash: str, node_ids: Sequence[str], r: int) -> list[str]:
    """Return the R nodes responsible for `content_hash`, highest score first.

    Highest Random Weight (HRW): score every node against the key and take the
    top R. Every node computes this independently from the same membership list
    and reaches the same answer, so no placement metadata has to be stored or
    agreed on.

    If the cluster is smaller than R, the whole cluster is the replica set.
    """
    if r <= 0:
        raise ValueError(f"replication factor must be positive, got {r}")
    if not node_ids:
        raise ValueError("cannot place a blob in an empty cluster")

    # Sorting on (score, node_id) makes the result independent of the order
    # node_ids arrives in, and breaks score ties deterministically, so two
    # nodes with the same membership always agree.
    ranked = sorted(
        ((_score(content_hash, node_id), node_id) for node_id in set(node_ids)),
        reverse=True,
    )
    return [node_id for _, node_id in ranked[:r]]


def _score(content_hash: str, node_id: str) -> int:
    """Weight of one (key, node) pair: a hash of the two combined.

    Hashing the pair — rather than combining separate hashes arithmetically —
    keeps the scores of a single key across nodes independent of each other,
    which is what gives HRW its even distribution.
    """
    digest = hashlib.sha256(f"{content_hash}:{node_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/cluster/test_placement.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add almacen/cluster/__init__.py almacen/cluster/placement.py tests/unit/cluster/
git commit -m "Add rendezvous hashing for content placement"
```

---

### Task 3: DAA note for rendezvous hashing

Required by the project rules: HRW is a data-structure/algorithm choice with
complexity trade-offs, so it gets a write-up. `DAA/` sits at the project root,
*outside* this repository, and is never committed.

**Files:**
- Create: `../DAA/rendezvous-hashing.md` (i.e. `Proyecto/DAA/rendezvous-hashing.md`)

- [ ] **Step 1: Write the note**

It must state the problem and the proposed solution *with the reasoning*, not
just the conclusion. Cover at minimum:

- **Problem:** choose R of N nodes for a blob, with no shared placement state, such that every node independently agrees, load is even, and a membership change relocates as few blobs as possible.
- **Rejected: `hash(key) % N`.** O(1) and trivial, but changing N remaps ~(N-1)/N of all keys — catastrophic re-replication on a single node join/leave.
- **Rejected: consistent hashing ring with virtual nodes.** Correct and O(log(N·V)) lookup via binary search on the ring, but needs V virtual nodes per real node (V≈100-200) to get even load, plus ring state to maintain; more machinery than this scale needs, and easier to get subtly wrong.
- **Chosen: HRW.** O(N) per lookup (score every node, take top R) with no state at all. At N=5 the O(N) scan is nothing — 5 SHA-256 calls — and it buys provably minimal disruption: removing a node only relocates the blobs that node actually hosted, which is the `test_removing_a_node_only_moves_keys_it_was_hosting` property.
- **The complexity trade-off explicitly:** HRW is O(N) per lookup vs the ring's O(log(N·V)). This is the right trade at N=5 and the wrong one at N=10,000; state the crossover reasoning rather than pretending HRW is universally better. Note that `sorted()` is O(N log N) as written and a `heapq.nlargest(r, ...)` would be O(N log R), which does not matter at this scale but is the natural fix if N grows.
- **Why sort on `(score, node_id)`:** determinism under score ties and independence from the order membership is listed in. Two nodes disagreeing on a replica set means a blob written to one set and read from another.
- **Why hash the concatenated pair** rather than XOR-ing or adding separate hashes of key and node: keeps per-node scores for one key independent, which is what makes the distribution even. A weighted-HRW variant (`score / weight` for heterogeneous node capacity) is noted as available but not needed here.

- [ ] **Step 2: Verify it is not tracked by git**

Run: `git status --short`
Expected: no mention of `DAA/` — it lives outside the repository and is also
listed in `.gitignore`.

---

### Task 4: Cluster settings

**Files:**
- Modify: `almacen/config.py`
- Test: `tests/unit/test_config.py`

The existing `Settings(data_dir=..., db_path=...)` call appears in every API and
storage test. All new fields therefore need defaults that describe a one-node
cluster, so the 39 existing tests keep passing unchanged.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_config.py
from pathlib import Path

import pytest

from almacen.config import Peer, Settings


def test_defaults_describe_a_single_node_cluster():
    settings = Settings(data_dir=Path("/tmp/blobs"), db_path=Path("/tmp/m.db"))
    assert settings.node_id == "node1"
    assert settings.node_ids == ["node1"]
    assert settings.replication_factor == 3
    assert settings.write_quorum == 2


def test_from_env_parses_the_peer_list(monkeypatch):
    monkeypatch.setenv("ALMACEN_NODE_ID", "node2")
    monkeypatch.setenv("ALMACEN_GRPC_PORT", "50052")
    monkeypatch.setenv(
        "ALMACEN_PEERS", "node1@node1:50051,node2@node2:50051,node3@node3:50051"
    )
    monkeypatch.setenv("ALMACEN_REPLICATION_FACTOR", "3")
    monkeypatch.setenv("ALMACEN_WRITE_QUORUM", "2")

    settings = Settings.from_env()

    assert settings.node_id == "node2"
    assert settings.grpc_port == 50052
    assert settings.node_ids == ["node1", "node2", "node3"]
    assert settings.peer_address("node3") == "node3:50051"


def test_from_env_tolerates_whitespace_in_the_peer_list(monkeypatch):
    monkeypatch.setenv("ALMACEN_PEERS", " node1@node1:50051 , node2@node2:50051 ")
    settings = Settings.from_env()
    assert settings.node_ids == ["node1", "node2"]


def test_remote_peers_excludes_self():
    peers = (Peer("node1", "node1:50051"), Peer("node2", "node2:50051"))
    settings = Settings(
        data_dir=Path("/tmp/b"), db_path=Path("/tmp/m.db"), node_id="node1", peers=peers
    )
    assert [p.node_id for p in settings.remote_peers] == ["node2"]


def test_rejects_write_quorum_larger_than_replication_factor():
    with pytest.raises(ValueError, match="write_quorum"):
        Settings(
            data_dir=Path("/tmp/b"),
            db_path=Path("/tmp/m.db"),
            replication_factor=2,
            write_quorum=3,
        )


def test_rejects_node_id_missing_from_the_peer_list():
    with pytest.raises(ValueError, match="node_id"):
        Settings(
            data_dir=Path("/tmp/b"),
            db_path=Path("/tmp/m.db"),
            node_id="ghost",
            peers=(Peer("node1", "node1:50051"),),
        )


def test_rejects_a_malformed_peer_entry(monkeypatch):
    monkeypatch.setenv("ALMACEN_PEERS", "node1-no-at-sign")
    with pytest.raises(ValueError, match="peer"):
        Settings.from_env()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_config.py -v`
Expected: FAIL — `ImportError: cannot import name 'Peer'`.

- [ ] **Step 3: Rewrite `config.py`**

Validation happens in `__post_init__` so a misconfigured node fails at boot with
a clear message, rather than mid-request with a confusing quorum error.

```python
# almacen/config.py
"""Node configuration: local storage plus static cluster membership."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_GRPC_PORT = 50051
DEFAULT_NODE_ID = "node1"


@dataclass(frozen=True)
class Peer:
    """One node in the cluster, addressable over gRPC."""

    node_id: str
    address: str  # "host:port"

    @classmethod
    def parse(cls, raw: str) -> "Peer":
        """Parse a `node_id@host:port` entry from ALMACEN_PEERS."""
        node_id, _, address = raw.strip().partition("@")
        if not node_id or not address:
            raise ValueError(
                f"malformed peer entry {raw!r}: expected 'node_id@host:port'"
            )
        return cls(node_id=node_id, address=address)


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    node_id: str = DEFAULT_NODE_ID
    grpc_port: int = DEFAULT_GRPC_PORT
    # Every node in the cluster, including this one. Empty means "just me",
    # which is how single-node development and the Phase 1 tests run.
    peers: tuple[Peer, ...] = field(default_factory=tuple)
    replication_factor: int = 3
    write_quorum: int = 2

    def __post_init__(self) -> None:
        if self.replication_factor < 1:
            raise ValueError(
                f"replication_factor must be >= 1, got {self.replication_factor}"
            )
        if self.write_quorum < 1:
            raise ValueError(f"write_quorum must be >= 1, got {self.write_quorum}")
        if self.write_quorum > self.replication_factor:
            raise ValueError(
                f"write_quorum ({self.write_quorum}) cannot exceed "
                f"replication_factor ({self.replication_factor})"
            )
        if self.peers and self.node_id not in {p.node_id for p in self.peers}:
            raise ValueError(
                f"node_id {self.node_id!r} does not appear in the peer list "
                f"{[p.node_id for p in self.peers]}"
            )

    @property
    def node_ids(self) -> list[str]:
        """Every node id in the cluster, for placement decisions."""
        return [p.node_id for p in self.peers] if self.peers else [self.node_id]

    @property
    def remote_peers(self) -> tuple[Peer, ...]:
        """Every peer except this node."""
        return tuple(p for p in self.peers if p.node_id != self.node_id)

    def peer_address(self, node_id: str) -> str | None:
        for peer in self.peers:
            if peer.node_id == node_id:
                return peer.address
        return None

    @classmethod
    def from_env(cls) -> "Settings":
        base_dir = Path(os.environ.get("ALMACEN_DATA_DIR", "./data"))
        raw_peers = os.environ.get("ALMACEN_PEERS", "").strip()
        peers = tuple(
            Peer.parse(entry) for entry in raw_peers.split(",") if entry.strip()
        )
        return cls(
            data_dir=base_dir / "blobs",
            db_path=base_dir / "metadata.db",
            node_id=os.environ.get("ALMACEN_NODE_ID", DEFAULT_NODE_ID),
            grpc_port=int(os.environ.get("ALMACEN_GRPC_PORT", DEFAULT_GRPC_PORT)),
            peers=peers,
            replication_factor=int(os.environ.get("ALMACEN_REPLICATION_FACTOR", 3)),
            write_quorum=int(os.environ.get("ALMACEN_WRITE_QUORUM", 2)),
        )
```

Note: `test_from_env_tolerates_whitespace_in_the_peer_list` sets no
`ALMACEN_NODE_ID`, so `node_id` defaults to `"node1"`, which *is* in the peer
list — the validation passes. Keep that in mind if you reorder the tests.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_config.py -v`
Expected: 7 passed

- [ ] **Step 5: Confirm nothing else broke**

Run: `pytest -q`
Expected: 46 passed (39 from before + 7 new). If any Phase 1 test fails, a
default is wrong — fix the default, do not change the Phase 1 test.

- [ ] **Step 6: Commit**

```bash
git add almacen/config.py tests/unit/test_config.py
git commit -m "Add static cluster membership and replication parameters to Settings"
```

---

### Task 5: Protocol definitions and generated stubs

**Files:**
- Create: `almacen/rpc/__init__.py` (empty), `almacen/rpc/replication.proto`, `almacen/rpc/cluster.proto`
- Create (generated, committed): `almacen/rpc/replication_pb2.py`, `replication_pb2.pyi`, `replication_pb2_grpc.py`, `cluster_pb2.py`, `cluster_pb2.pyi`, `cluster_pb2_grpc.py`

There is no test in this task — generated code is not hand-written logic, and the
next three tasks test it through real channels. The verification here is that the
stubs import.

- [ ] **Step 1: Write `replication.proto`**

`BlobChunk` carries `content_hash` on every chunk rather than only the first. It
costs 64 bytes per MiB and it means the receiver never has to track "did I see a
header yet", which removes a whole class of stream-handling bug.

```protobuf
// almacen/rpc/replication.proto
syntax = "proto3";

package almacen;

// Blob transfer between nodes. Content is streamed because a blob can be far
// larger than gRPC's 4 MiB default message limit.
service Replication {
  rpc PutBlob(stream BlobChunk) returns (PutBlobAck);
  rpc GetBlob(BlobRequest) returns (stream BlobChunk);
}

message BlobChunk {
  // Repeated on every chunk so the receiver never needs stream-position state.
  string content_hash = 1;
  bytes data = 2;
}

message PutBlobAck {
  string content_hash = 1;
  bool stored = 2;
}

message BlobRequest {
  string content_hash = 1;
}
```

- [ ] **Step 2: Write `cluster.proto`**

`FileRecordMsg` mirrors today's `FileRecord` exactly. Phase 3 will add
`vector_clock` and turn the LWW fields into registers; leaving space for them now
would be speculative. Timestamps travel as ISO-8601 strings (matching how
`MetadataStore` already persists them) rather than as protobuf `Timestamp`, to
avoid a second representation of the same value and the conversions between them.

```protobuf
// almacen/rpc/cluster.proto
syntax = "proto3";

package almacen;

service Cluster {
  // Phase 2: direct push of a metadata record to a peer. Phase 4's push-pull
  // anti-entropy supersedes this as the repair mechanism, but this stays as the
  // fast path.
  rpc ReplicateRecord(FileRecordMsg) returns (ReplicateAck);
  rpc Ping(PingRequest) returns (PingResponse);
}

message FileRecordMsg {
  string file_id = 1;
  string name = 2;
  string content_hash = 3;
  repeated string tags = 4;
  bool tombstone = 5;
  // ISO-8601; empty string means "not set" (proto3 has no null for scalars).
  string tombstone_at = 6;
  string created_at = 7;
  string updated_at = 8;
}

message ReplicateAck {
  bool applied = 1;
}

message PingRequest {
  string from_node_id = 1;
}

message PingResponse {
  string node_id = 1;
}
```

- [ ] **Step 3: Generate the stubs**

Run: `./scripts/gen_protos.sh`
Expected: `Generated stubs in almacen/rpc/` and six new files present.

- [ ] **Step 4: Verify the generated imports are package-qualified**

Run: `grep -n "^from almacen" almacen/rpc/replication_pb2_grpc.py almacen/rpc/cluster_pb2_grpc.py`
Expected: each file shows `from almacen.rpc import <name>_pb2 as ...`. If instead
you see a bare `import replication_pb2`, `--proto_path` was wrong — the script
must run from the repository root with `--proto_path=.`, otherwise the stubs fail
to import inside the package.

- [ ] **Step 5: Verify the stubs import and the runtime version is satisfied**

Run:

```bash
python -c "
from almacen.rpc import replication_pb2, replication_pb2_grpc, cluster_pb2, cluster_pb2_grpc
print('stubs import cleanly')
print(cluster_pb2.FileRecordMsg(file_id='x', tags=['a','b']))
"
```

Expected: `stubs import cleanly` followed by the printed message. A
`VersionError` here means the `protobuf` floor from Task 1 is not satisfied in
this environment.

- [ ] **Step 6: Commit**

```bash
git add almacen/rpc/
git commit -m "Add gRPC service definitions for blob and metadata replication"
```

---

### Task 6: `MetadataStore.upsert` for applying remote records

A pushed record may or may not already exist locally, so the receiving side needs
a single operation that works either way. `insert` would violate the primary key;
`update` would silently no-op on a record this node has never seen.

**Files:**
- Modify: `almacen/storage/metadata_store.py`
- Test: `tests/integration/storage/test_metadata_store.py` (append)

- [ ] **Step 1: Write the failing tests**

```python
def test_upsert_inserts_a_record_the_store_has_never_seen(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})

    store.upsert(record)

    stored = store.get(record.file_id)
    assert stored is not None
    assert stored.name == "a.txt"
    assert stored.tags == {"x"}


def test_upsert_overwrites_an_existing_record(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})
    store.insert(record)

    record.rename("b.txt")
    record.add_tag("y")
    record.remove_tag("x")
    store.upsert(record)

    stored = store.get(record.file_id)
    assert stored is not None
    assert stored.name == "b.txt"
    assert stored.tags == {"y"}


def test_upsert_is_idempotent(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x", "y"})

    store.upsert(record)
    store.upsert(record)

    assert len(store.list_live()) == 1
    stored = store.get(record.file_id)
    assert stored is not None and stored.tags == {"x", "y"}


def test_upsert_preserves_a_tombstone(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(name="a.txt", content_hash="h1")
    record.mark_deleted()

    store.upsert(record)

    stored = store.get(record.file_id)
    assert stored is not None and stored.tombstone is True
    assert store.list_live() == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/storage/test_metadata_store.py -v -k upsert`
Expected: FAIL — `AttributeError: 'MetadataStore' object has no attribute 'upsert'`.

- [ ] **Step 3: Implement `upsert`**

Add to `MetadataStore`, after `update`. Note it follows the existing locking
discipline exactly — `with self._lock, self._conn:` for the whole multi-statement
operation — for the reasons written up in
`DAA/mutual-exclusion-shared-sqlite-connection.md`. Do not take the lock in a
helper and again here.

```python
    def upsert(self, record: FileRecord) -> None:
        """Insert the record, or replace it wholesale if it already exists.

        Used when applying a record replicated from another node, where this node
        may or may not already know the file.
        """
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO files
                    (file_id, name, content_hash, tombstone, tombstone_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_id) DO UPDATE SET
                    name = excluded.name,
                    content_hash = excluded.content_hash,
                    tombstone = excluded.tombstone,
                    tombstone_at = excluded.tombstone_at,
                    updated_at = excluded.updated_at
                """,
                (
                    str(record.file_id),
                    record.name,
                    record.content_hash,
                    int(record.tombstone),
                    _dt_to_str(record.tombstone_at),
                    _dt_to_str(record.created_at),
                    _dt_to_str(record.updated_at),
                ),
            )
            self._conn.execute(
                "DELETE FROM file_tags WHERE file_id = ?", (str(record.file_id),)
            )
            self._insert_tags(record.file_id, record.tags)
```

`created_at` is deliberately absent from the `DO UPDATE SET` list: a file's
creation time is immutable, and the copy that already exists locally is at least
as authoritative as the incoming one.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/integration/storage/test_metadata_store.py -v`
Expected: 12 passed (8 existing + 4 new).

- [ ] **Step 5: Commit**

```bash
git add almacen/storage/metadata_store.py tests/integration/storage/test_metadata_store.py
git commit -m "Add MetadataStore.upsert for applying replicated records"
```

---

### Task 7: Replication servicer — serving blobs to peers

**Files:**
- Create: `almacen/rpc/replication_servicer.py`
- Test: `tests/integration/rpc/__init__.py` (empty), `tests/integration/rpc/conftest.py` (fixtures only; `free_port` comes from `tests/helpers.py`), `tests/integration/rpc/test_replication_servicer.py`

- [ ] **Step 1: Write the shared test helper**

Every RPC test needs a real server on a real port. This fixture pattern was
validated end-to-end before this plan was written: streaming a 3 MiB blob over
loopback, `NOT_FOUND` on a missing blob, `INVALID_ARGUMENT` on a hash mismatch,
and a clean `server.stop(0).wait()`.

```python
# tests/integration/rpc/conftest.py
from collections.abc import Iterator
from concurrent import futures

import grpc
import pytest

from tests.helpers import free_port


@pytest.fixture
def grpc_server_factory() -> Iterator[object]:
    """Start gRPC servers that are always torn down, even if a test fails."""
    servers: list[grpc.Server] = []

    def start(register) -> str:
        """`register(server)` wires servicers in; returns the server address."""
        port = free_port()
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        register(server)
        server.add_insecure_port(f"127.0.0.1:{port}")
        server.start()
        servers.append(server)
        return f"127.0.0.1:{port}"

    yield start

    for server in servers:
        server.stop(0).wait()
```

- [ ] **Step 2: Write the failing tests**

```python
# tests/integration/rpc/test_replication_servicer.py
import hashlib
from pathlib import Path

import grpc
import pytest

from almacen.rpc import replication_pb2 as pb
from almacen.rpc import replication_pb2_grpc as pb_grpc
from almacen.rpc.replication_servicer import CHUNK_SIZE, ReplicationServicer
from almacen.storage.blob_store import BlobStore


@pytest.fixture
def blob_store(tmp_path: Path) -> BlobStore:
    return BlobStore(tmp_path / "blobs")


@pytest.fixture
def stub(blob_store: BlobStore, grpc_server_factory):
    address = grpc_server_factory(
        lambda server: pb_grpc.add_ReplicationServicer_to_server(
            ReplicationServicer(blob_store), server
        )
    )
    channel = grpc.insecure_channel(address)
    yield pb_grpc.ReplicationStub(channel)
    channel.close()


def _chunks(content: bytes, content_hash: str):
    for offset in range(0, len(content), CHUNK_SIZE):
        yield pb.BlobChunk(
            content_hash=content_hash, data=content[offset : offset + CHUNK_SIZE]
        )


def test_put_blob_stores_the_content(stub, blob_store: BlobStore):
    content = b"hello cluster"
    content_hash = hashlib.sha256(content).hexdigest()

    ack = stub.PutBlob(_chunks(content, content_hash))

    assert ack.stored is True
    assert ack.content_hash == content_hash
    assert blob_store.get(content_hash) == content


def test_put_blob_reassembles_a_multi_chunk_stream(stub, blob_store: BlobStore):
    content = b"x" * (CHUNK_SIZE * 2 + 17)  # forces three chunks
    content_hash = hashlib.sha256(content).hexdigest()

    stub.PutBlob(_chunks(content, content_hash))

    assert blob_store.get(content_hash) == content


def test_put_blob_rejects_content_that_does_not_match_its_hash(stub, blob_store):
    bogus_hash = "0" * 64

    with pytest.raises(grpc.RpcError) as error:
        stub.PutBlob(iter([pb.BlobChunk(content_hash=bogus_hash, data=b"tampered")]))

    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert blob_store.get(bogus_hash) is None, "corrupt content must not be stored"


def test_put_blob_rejects_an_empty_stream(stub):
    with pytest.raises(grpc.RpcError) as error:
        stub.PutBlob(iter([]))
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_put_blob_is_idempotent(stub, blob_store: BlobStore):
    content = b"same bytes twice"
    content_hash = hashlib.sha256(content).hexdigest()

    first = stub.PutBlob(_chunks(content, content_hash))
    second = stub.PutBlob(_chunks(content, content_hash))

    assert first.stored and second.stored
    assert blob_store.get(content_hash) == content


def test_get_blob_streams_the_content_back(stub, blob_store: BlobStore):
    content = b"y" * (CHUNK_SIZE + 5)
    content_hash = blob_store.put(content)

    received = b"".join(
        chunk.data for chunk in stub.GetBlob(pb.BlobRequest(content_hash=content_hash))
    )

    assert received == content


def test_get_blob_reports_not_found_for_an_unknown_hash(stub):
    with pytest.raises(grpc.RpcError) as error:
        list(stub.GetBlob(pb.BlobRequest(content_hash="f" * 64)))
    assert error.value.code() == grpc.StatusCode.NOT_FOUND
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/integration/rpc/test_replication_servicer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'almacen.rpc.replication_servicer'`.

- [ ] **Step 4: Write the servicer**

```python
# almacen/rpc/replication_servicer.py
"""Serves blob content to peer nodes over gRPC."""
from __future__ import annotations

import hashlib
from collections.abc import Iterator

import grpc

from almacen.rpc import replication_pb2 as pb
from almacen.rpc import replication_pb2_grpc as pb_grpc
from almacen.storage.blob_store import BlobStore

# 1 MiB: comfortably under gRPC's 4 MiB default per-message limit, and large
# enough that per-message overhead is irrelevant.
CHUNK_SIZE = 1 << 20


class ReplicationServicer(pb_grpc.ReplicationServicer):
    def __init__(self, blob_store: BlobStore) -> None:
        self._blob_store = blob_store

    def PutBlob(
        self, request_iterator: Iterator[pb.BlobChunk], context: grpc.ServicerContext
    ) -> pb.PutBlobAck:
        declared_hash = ""
        buffer = bytearray()
        received_any = False

        for chunk in request_iterator:
            received_any = True
            if chunk.content_hash:
                declared_hash = chunk.content_hash
            buffer.extend(chunk.data)

        if not received_any or not declared_hash:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "PutBlob requires at least one chunk carrying a content_hash",
            )

        # Verify before storing, never after: a blob is addressed by its hash, so
        # storing content whose hash does not match would poison every future
        # read of that address across the whole cluster.
        actual_hash = hashlib.sha256(bytes(buffer)).hexdigest()
        if actual_hash != declared_hash:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"content hash mismatch: declared {declared_hash}, got {actual_hash}",
            )

        stored_hash = self._blob_store.put(bytes(buffer))
        return pb.PutBlobAck(content_hash=stored_hash, stored=True)

    def GetBlob(
        self, request: pb.BlobRequest, context: grpc.ServicerContext
    ) -> Iterator[pb.BlobChunk]:
        content = self._blob_store.get(request.content_hash)
        if content is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"blob {request.content_hash} not held by this node",
            )

        for offset in range(0, len(content), CHUNK_SIZE):
            yield pb.BlobChunk(
                content_hash=request.content_hash,
                data=content[offset : offset + CHUNK_SIZE],
            )
```

`context.abort()` raises, so the code after it is unreachable — that is why there
is no `return` after the aborts. Type checkers do not always know this; if one
complains about `content` being `bytes | None` below the abort, that is a false
positive.

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/integration/rpc/test_replication_servicer.py -v`
Expected: 7 passed

- [ ] **Step 6: Commit**

```bash
git add almacen/rpc/replication_servicer.py tests/integration/rpc/
git commit -m "Add replication servicer for streaming blob transfer between nodes"
```

---

### Task 8: Cluster servicer — applying pushed records and answering Ping

**Files:**
- Create: `almacen/rpc/cluster_servicer.py`, `almacen/rpc/record_codec.py`
- Test: `tests/unit/rpc/__init__.py` (empty), `tests/unit/rpc/test_record_codec.py`, `tests/integration/rpc/test_cluster_servicer.py`

The wire format conversion goes in its own module: it is pure, it is needed by
both the servicer (decode) and the replicator client (encode), and it is the
piece Phase 3 has to change when `FileRecord` grows a vector clock.

- [ ] **Step 1: Write the failing codec tests**

```python
# tests/unit/rpc/test_record_codec.py
import uuid

import pytest

from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2 as pb
from almacen.rpc.record_codec import message_to_record, record_to_message


def test_round_trips_a_live_record():
    original = FileRecord.new(name="a.txt", content_hash="h1", tags={"x", "y"})

    restored = message_to_record(record_to_message(original))

    assert restored.file_id == original.file_id
    assert restored.name == original.name
    assert restored.content_hash == original.content_hash
    assert restored.tags == original.tags
    assert restored.tombstone is False
    assert restored.tombstone_at is None
    assert restored.created_at == original.created_at
    assert restored.updated_at == original.updated_at


def test_round_trips_a_tombstoned_record():
    original = FileRecord.new(name="a.txt", content_hash="h1")
    original.mark_deleted()

    restored = message_to_record(record_to_message(original))

    assert restored.tombstone is True
    assert restored.tombstone_at == original.tombstone_at


def test_round_trips_a_record_with_no_tags():
    original = FileRecord.new(name="a.txt", content_hash="h1")
    restored = message_to_record(record_to_message(original))
    assert restored.tags == set()


def test_timestamps_survive_as_timezone_aware_values():
    original = FileRecord.new(name="a.txt", content_hash="h1")
    restored = message_to_record(record_to_message(original))
    assert restored.created_at.tzinfo is not None
    assert restored.created_at == original.created_at


def test_rejects_a_message_without_timestamps():
    # A record with no created_at would violate the NOT NULL schema on upsert, so
    # it has to be rejected at the edge instead.
    incomplete = pb.FileRecordMsg(file_id=str(uuid.uuid4()), name="a.txt")
    with pytest.raises(ValueError):
        message_to_record(incomplete)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/rpc/test_record_codec.py -v`
Expected: FAIL — no module `almacen.rpc.record_codec`.

- [ ] **Step 3: Write the codec**

```python
# almacen/rpc/record_codec.py
"""Conversion between FileRecord and its gRPC wire representation.

Kept separate from the servicers because both directions are needed on both
sides of a call, and because this is the single place Phase 3 has to touch when
FileRecord gains a vector clock and CRDT-valued fields.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2 as pb


def record_to_message(record: FileRecord) -> pb.FileRecordMsg:
    return pb.FileRecordMsg(
        file_id=str(record.file_id),
        name=record.name,
        content_hash=record.content_hash,
        # Sorted so the encoding of a record is deterministic, which makes
        # messages comparable in tests and diffable in logs.
        tags=sorted(record.tags),
        tombstone=record.tombstone,
        # proto3 scalars have no null; empty string is the absent value.
        tombstone_at=record.tombstone_at.isoformat() if record.tombstone_at else "",
        created_at=record.created_at.isoformat(),
        updated_at=record.updated_at.isoformat(),
    )


def message_to_record(message: pb.FileRecordMsg) -> FileRecord:
    created_at = _parse(message.created_at)
    updated_at = _parse(message.updated_at)
    # `files.created_at` and `files.updated_at` are NOT NULL in the schema. Left
    # unchecked, a truncated or hand-built message would reach `upsert` and fail
    # there as a sqlite3.IntegrityError, surfacing to the caller as an opaque
    # gRPC UNKNOWN. Rejecting here turns it into INVALID_ARGUMENT, which is what
    # it actually is.
    if created_at is None or updated_at is None:
        raise ValueError("created_at and updated_at are required")

    return FileRecord(
        file_id=uuid.UUID(message.file_id),
        name=message.name,
        content_hash=message.content_hash,
        tags=set(message.tags),
        tombstone=message.tombstone,
        tombstone_at=_parse(message.tombstone_at),
        created_at=created_at,
        updated_at=updated_at,
    )


def _parse(value: str) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
```

- [ ] **Step 4: Run codec tests**

Run: `pytest tests/unit/rpc/test_record_codec.py -v`
Expected: 5 passed

- [ ] **Step 5: Write the failing servicer tests**

```python
# tests/integration/rpc/test_cluster_servicer.py
from pathlib import Path

import grpc
import pytest

from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2 as pb
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.cluster_servicer import ClusterServicer
from almacen.rpc.record_codec import record_to_message
from almacen.storage.metadata_store import MetadataStore


@pytest.fixture
def metadata_store(tmp_path: Path) -> MetadataStore:
    return MetadataStore(tmp_path / "metadata.db")


@pytest.fixture
def stub(metadata_store: MetadataStore, grpc_server_factory):
    address = grpc_server_factory(
        lambda server: pb_grpc.add_ClusterServicer_to_server(
            ClusterServicer(metadata_store, node_id="node2"), server
        )
    )
    channel = grpc.insecure_channel(address)
    yield pb_grpc.ClusterStub(channel)
    channel.close()


def test_replicate_record_applies_a_new_record(stub, metadata_store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})

    ack = stub.ReplicateRecord(record_to_message(record))

    assert ack.applied is True
    stored = metadata_store.get(record.file_id)
    assert stored is not None
    assert stored.name == "a.txt"
    assert stored.tags == {"x"}


def test_replicate_record_overwrites_a_known_record(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})
    metadata_store.insert(record)

    record.rename("renamed.txt")
    stub.ReplicateRecord(record_to_message(record))

    stored = metadata_store.get(record.file_id)
    assert stored is not None and stored.name == "renamed.txt"


def test_replicate_record_propagates_a_tombstone(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h1")
    metadata_store.insert(record)

    record.mark_deleted()
    stub.ReplicateRecord(record_to_message(record))

    assert metadata_store.list_live() == []


def test_replicate_record_rejects_a_malformed_file_id(stub):
    with pytest.raises(grpc.RpcError) as error:
        stub.ReplicateRecord(pb.FileRecordMsg(file_id="not-a-uuid", name="a.txt"))
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_ping_identifies_the_responding_node(stub):
    response = stub.Ping(pb.PingRequest(from_node_id="node1"))
    assert response.node_id == "node2"
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `pytest tests/integration/rpc/test_cluster_servicer.py -v`
Expected: FAIL — no module `almacen.rpc.cluster_servicer`.

- [ ] **Step 7: Write the servicer**

```python
# almacen/rpc/cluster_servicer.py
"""Receives metadata replicated from peer nodes."""
from __future__ import annotations

import logging

import grpc

from almacen.rpc import cluster_pb2 as pb
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.record_codec import message_to_record
from almacen.storage.metadata_store import MetadataStore

logger = logging.getLogger(__name__)


class ClusterServicer(pb_grpc.ClusterServicer):
    def __init__(self, metadata_store: MetadataStore, node_id: str) -> None:
        # Deliberately the *local* store, never the replicating wrapper: applying
        # a record received from a peer must not push it back out, or every
        # write would ping-pong around the cluster forever.
        self._metadata_store = metadata_store
        self._node_id = node_id

    def ReplicateRecord(
        self, request: pb.FileRecordMsg, context: grpc.ServicerContext
    ) -> pb.ReplicateAck:
        try:
            record = message_to_record(request)
        except ValueError as error:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"undecodable record: {error}"
            )

        # Phase 2 applies the incoming record wholesale (last write to arrive
        # wins). That is genuinely wrong under concurrency: two nodes editing the
        # same file during a partition will clobber each other depending on
        # arrival order. Phase 3 replaces this line with a CRDT merge, which is
        # the whole reason that phase exists.
        self._metadata_store.upsert(record)
        logger.debug("applied replicated record %s", record.file_id)
        return pb.ReplicateAck(applied=True)

    def Ping(
        self, request: pb.PingRequest, context: grpc.ServicerContext
    ) -> pb.PingResponse:
        return pb.PingResponse(node_id=self._node_id)
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `pytest tests/unit/rpc/ tests/integration/rpc/ -v`
Expected: 17 passed (5 codec + 7 replication + 5 cluster)

- [ ] **Step 9: Commit**

```bash
git add almacen/rpc/record_codec.py almacen/rpc/cluster_servicer.py tests/unit/rpc/ tests/integration/rpc/test_cluster_servicer.py
git commit -m "Add cluster servicer for metadata record replication"
```

---

### Task 9: gRPC server module

**Files:**
- Create: `almacen/rpc/server.py`
- Test: `tests/integration/rpc/test_server.py`

One function that assembles both servicers into an unstarted server. Returning it
unstarted keeps lifecycle control with the caller (`main.py`'s lifespan in Task
13, the test fixtures elsewhere) instead of hiding it.

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/rpc/test_server.py
from pathlib import Path

import grpc

from almacen.rpc import cluster_pb2 as cluster_pb
from almacen.rpc import cluster_pb2_grpc as cluster_grpc
from almacen.rpc import replication_pb2 as repl_pb
from almacen.rpc import replication_pb2_grpc as repl_grpc
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port


def test_build_server_exposes_both_services(tmp_path: Path):
    blob_store = BlobStore(tmp_path / "blobs")
    metadata_store = MetadataStore(tmp_path / "metadata.db")
    port = free_port()

    server = build_server(
        blob_store=blob_store,
        metadata_store=metadata_store,
        node_id="node1",
        port=port,
    )
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            # Cluster service answers.
            ping = cluster_grpc.ClusterStub(channel).Ping(
                cluster_pb.PingRequest(from_node_id="tester")
            )
            assert ping.node_id == "node1"

            # Replication service answers on the same port.
            content_hash = blob_store.put(b"present")
            received = b"".join(
                chunk.data
                for chunk in repl_grpc.ReplicationStub(channel).GetBlob(
                    repl_pb.BlobRequest(content_hash=content_hash)
                )
            )
            assert received == b"present"
    finally:
        server.stop(0).wait()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/integration/rpc/test_server.py -v`
Expected: FAIL — no module `almacen.rpc.server`.

- [ ] **Step 3: Write `server.py`**

```python
# almacen/rpc/server.py
"""Assembly of the node's gRPC server (internal, node-to-node plane)."""
from __future__ import annotations

from concurrent import futures

import grpc

from almacen.rpc import cluster_pb2_grpc, replication_pb2_grpc
from almacen.rpc.cluster_servicer import ClusterServicer
from almacen.rpc.replication_servicer import ReplicationServicer
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore

DEFAULT_MAX_WORKERS = 8


def build_server(
    blob_store: BlobStore,
    metadata_store: MetadataStore,
    node_id: str,
    port: int,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> grpc.Server:
    """Build the node's gRPC server, bound but not started.

    The caller starts and stops it, so server lifetime is tied to whatever owns
    it (the FastAPI lifespan in production, a fixture in tests) instead of being
    an invisible side effect of construction.

    `metadata_store` must be the plain local store, not the replicating wrapper:
    records arriving from peers are applied locally and must not be pushed back
    out (see ClusterServicer).
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    replication_pb2_grpc.add_ReplicationServicer_to_server(
        ReplicationServicer(blob_store), server
    )
    cluster_pb2_grpc.add_ClusterServicer_to_server(
        ClusterServicer(metadata_store, node_id=node_id), server
    )
    # Binding to all interfaces is required inside a container, where peers reach
    # this node by its service name rather than loopback.
    server.add_insecure_port(f"[::]:{port}")
    return server
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/integration/rpc/test_server.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add almacen/rpc/server.py tests/integration/rpc/test_server.py
git commit -m "Add gRPC server assembly for the node-to-node plane"
```

---

### Task 10: Channel pool

Both the blob replication client and the metadata replicator talk to the same
peers. Caching channels in one shared place keeps connection reuse (gRPC channels
are meant to be long-lived) and gives a single `close()` to call at shutdown.

**Files:**
- Create: `almacen/cluster/channels.py`
- Test: `tests/unit/cluster/test_channels.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/cluster/test_channels.py
from almacen.cluster.channels import ChannelPool


def test_returns_the_same_channel_for_the_same_address():
    pool = ChannelPool()
    try:
        first = pool.channel("127.0.0.1:50051")
        second = pool.channel("127.0.0.1:50051")
        assert first is second
    finally:
        pool.close()


def test_returns_distinct_channels_for_distinct_addresses():
    pool = ChannelPool()
    try:
        assert pool.channel("127.0.0.1:50051") is not pool.channel("127.0.0.1:50052")
    finally:
        pool.close()


def test_close_is_idempotent():
    pool = ChannelPool()
    pool.channel("127.0.0.1:50051")
    pool.close()
    pool.close()  # must not raise


def test_channel_after_close_is_a_fresh_channel():
    pool = ChannelPool()
    first = pool.channel("127.0.0.1:50051")
    pool.close()
    second = pool.channel("127.0.0.1:50051")
    try:
        assert second is not first
    finally:
        pool.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/cluster/test_channels.py -v`
Expected: FAIL — no module `almacen.cluster.channels`.

- [ ] **Step 3: Write `channels.py`**

```python
# almacen/cluster/channels.py
"""Shared, reusable gRPC channels to peer nodes."""
from __future__ import annotations

import threading

import grpc


class ChannelPool:
    """Caches one long-lived gRPC channel per peer address.

    gRPC channels are designed to be created once and reused: each one manages
    its own connection state and reconnects on its own. Creating a channel per
    request would add a TCP/HTTP2 handshake to every replicated write.
    """

    def __init__(self) -> None:
        self._channels: dict[str, grpc.Channel] = {}
        # Guards the dict: FastAPI serves sync handlers from a thread pool, so
        # two requests can ask for the same peer's channel at the same time and
        # would otherwise each create one, leaking all but the last.
        self._lock = threading.Lock()

    def channel(self, address: str) -> grpc.Channel:
        with self._lock:
            channel = self._channels.get(address)
            if channel is None:
                channel = grpc.insecure_channel(address)
                self._channels[address] = channel
            return channel

    def close(self) -> None:
        with self._lock:
            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/cluster/test_channels.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add almacen/cluster/channels.py tests/unit/cluster/test_channels.py
git commit -m "Add pooled gRPC channels to peer nodes"
```

---

### Task 11: Replication client — quorum write and fallback read

This is where the spec's §8 write and read paths become code.

**Files:**
- Create: `almacen/cluster/replication_client.py`
- Test: `tests/integration/cluster/__init__.py` (empty), `tests/integration/cluster/test_replication_client.py`

- [ ] **Step 1: Write the failing tests**

The fixture builds a real N-node ring of gRPC servers — no mocks — because the
thing under test is quorum behaviour across real channels. It starts the servers
directly rather than through the `grpc_server_factory` fixture, since it needs
five of them at once; only `free_port` is shared.

```python
# tests/integration/cluster/test_replication_client.py
import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from almacen.cluster.channels import ChannelPool
from almacen.cluster.placement import replica_set
from almacen.cluster.replication_client import QuorumNotReached, ReplicationClient
from almacen.config import Peer, Settings
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port

NODE_COUNT = 5


@dataclass
class FakeNode:
    node_id: str
    blob_store: BlobStore
    server: object


@pytest.fixture
def cluster(tmp_path: Path) -> Iterator[tuple[list[FakeNode], tuple[Peer, ...]]]:
    """Five real gRPC nodes on loopback, each with its own blob directory."""
    nodes: list[FakeNode] = []
    peers: list[Peer] = []

    for index in range(1, NODE_COUNT + 1):
        node_id = f"node{index}"
        port = free_port()
        blob_store = BlobStore(tmp_path / node_id / "blobs")
        metadata_store = MetadataStore(tmp_path / node_id / "metadata.db")
        server = build_server(
            blob_store=blob_store,
            metadata_store=metadata_store,
            node_id=node_id,
            port=port,
        )
        server.start()
        nodes.append(FakeNode(node_id, blob_store, server))
        peers.append(Peer(node_id=node_id, address=f"127.0.0.1:{port}"))

    yield nodes, tuple(peers)

    for node in nodes:
        node.server.stop(0).wait()


@pytest.fixture
def client_on(cluster, tmp_path: Path):
    """Build a ReplicationClient that coordinates from a chosen node.

    The channel pool is owned by the fixture and closed on teardown, so channels
    do not leak across tests.
    """
    nodes, peers = cluster
    channels = ChannelPool()

    def build(node_id: str) -> ReplicationClient:
        settings = Settings(
            data_dir=tmp_path / node_id / "blobs",
            db_path=tmp_path / node_id / "metadata.db",
            node_id=node_id,
            peers=peers,
            replication_factor=3,
            write_quorum=2,
        )
        local = next(n for n in nodes if n.node_id == node_id)
        return ReplicationClient(settings, local.blob_store, channels)

    yield build
    channels.close()


def test_put_blob_stores_on_exactly_the_hrw_replica_set(cluster, client_on):
    nodes, peers = cluster
    client = client_on("node1")
    content = b"replicated content"
    content_hash = hashlib.sha256(content).hexdigest()

    returned_hash = client.put_blob(content)

    assert returned_hash == content_hash
    expected = set(replica_set(content_hash, [p.node_id for p in peers], r=3))
    holders = {n.node_id for n in nodes if n.blob_store.exists(content_hash)}
    assert holders == expected, (
        "content must land on exactly the HRW replica set, no more and no fewer"
    )
    assert len(holders) == 3


def test_put_blob_does_not_keep_a_local_copy_when_the_coordinator_is_not_a_replica(
    cluster, client_on
):
    nodes, peers = cluster
    node_ids = [p.node_id for p in peers]

    # Find content whose replica set excludes node1, so node1 coordinates a write
    # it is not a replica for.
    for index in range(2000):
        content = f"payload-{index}".encode()
        content_hash = hashlib.sha256(content).hexdigest()
        if "node1" not in replica_set(content_hash, node_ids, r=3):
            break
    else:
        pytest.fail("could not find content whose replica set excludes node1")

    client = client_on("node1")
    client.put_blob(content)

    local = next(n for n in nodes if n.node_id == "node1")
    assert not local.blob_store.exists(content_hash), (
        "coordinator must not hoard a copy it is not responsible for"
    )


# The coordinator writes its own replica through the local BlobStore, never over
# gRPC, so stopping the coordinator's *server* removes no acknowledgement at all.
# Both tests below therefore disable only REMOTE replicas. Getting this wrong is
# easy and silent: with r=3 over node1..node5, b"not enough replicas" maps to
# ['node3', 'node1', 'node2'], so killing the last two would kill the coordinator
# and the write would still reach quorum (1 local + 1 remote = W).
def _remote_replicas(content_hash: str, peers, coordinator: str) -> list[str]:
    node_ids = [p.node_id for p in peers]
    return [n for n in replica_set(content_hash, node_ids, r=3) if n != coordinator]


def test_put_blob_succeeds_when_exactly_the_write_quorum_is_reachable(
    cluster, client_on
):
    nodes, peers = cluster
    content = b"quorum edge case"
    content_hash = hashlib.sha256(content).hexdigest()

    # Drop one remote replica. Whether or not node1 is itself a replica, exactly
    # two acknowledgements remain, which is exactly W=2.
    doomed_id = _remote_replicas(content_hash, peers, "node1")[-1]
    next(n for n in nodes if n.node_id == doomed_id).server.stop(0).wait()

    client = client_on("node1")
    client.put_blob(content)  # must not raise

    holders = {n.node_id for n in nodes if n.blob_store.exists(content_hash)}
    assert len(holders) == 2


def test_put_blob_raises_when_the_write_quorum_cannot_be_met(cluster, client_on):
    nodes, peers = cluster
    content = b"not enough replicas"
    content_hash = hashlib.sha256(content).hexdigest()

    # Drop every remote replica, leaving at most the coordinator's own local
    # write — one acknowledgement at best, below W=2 either way.
    for node_id in _remote_replicas(content_hash, peers, "node1"):
        next(n for n in nodes if n.node_id == node_id).server.stop(0).wait()

    client = client_on("node1")

    with pytest.raises(QuorumNotReached):
        client.put_blob(content)


def test_get_blob_reads_a_local_copy_without_a_network_call(cluster, client_on):
    nodes, peers = cluster
    local = next(n for n in nodes if n.node_id == "node1")
    content_hash = local.blob_store.put(b"already here")

    client = client_on("node1")

    # Stop every other node: a local read must not need any of them.
    for node in nodes:
        if node.node_id != "node1":
            node.server.stop(0).wait()

    assert client.get_blob(content_hash) == b"already here"


def test_get_blob_fetches_from_a_replica_when_absent_locally(cluster, client_on):
    nodes, peers = cluster
    content = b"fetch me from a peer"

    writer = client_on("node1")
    content_hash = writer.put_blob(content)

    # Read from a node that is definitely not holding it.
    node_ids = [p.node_id for p in peers]
    replicas = replica_set(content_hash, node_ids, r=3)
    outsider = next(n for n in nodes if n.node_id not in replicas)
    reader = client_on(outsider.node_id)

    assert reader.get_blob(content_hash) == content


def test_get_blob_returns_none_when_no_replica_can_serve_it(cluster, client_on):
    nodes, peers = cluster
    client = client_on("node1")
    assert client.get_blob("a" * 64) is None


def test_get_blob_rejects_content_that_fails_its_hash_check(cluster, client_on):
    nodes, peers = cluster
    node_ids = [p.node_id for p in peers]
    bogus_hash = "b" * 64

    # Plant content on a replica under a hash that does not describe it, the way
    # a corrupted disk would.
    holder_id = replica_set(bogus_hash, node_ids, r=3)[0]
    holder = next(n for n in nodes if n.node_id == holder_id)
    (holder.blob_store._root / bogus_hash).write_bytes(b"corrupted payload")

    reader_id = next(
        n.node_id for n in nodes if n.node_id not in replica_set(bogus_hash, node_ids, 3)
    )
    client = client_on(reader_id)

    assert client.get_blob(bogus_hash) is None, (
        "content that does not match its address must be discarded, not served"
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/cluster/test_replication_client.py -v`
Expected: FAIL — no module `almacen.cluster.replication_client`.

- [ ] **Step 3: Write `replication_client.py`**

```python
# almacen/cluster/replication_client.py
"""Orchestrates content writes and reads across the cluster (spec §8)."""
from __future__ import annotations

import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor

import grpc

from almacen.cluster.channels import ChannelPool
from almacen.cluster.placement import replica_set
from almacen.config import Settings
from almacen.rpc import replication_pb2 as pb
from almacen.rpc import replication_pb2_grpc as pb_grpc
from almacen.rpc.replication_servicer import CHUNK_SIZE
from almacen.storage.blob_store import BlobStore

logger = logging.getLogger(__name__)

RPC_TIMEOUT_SECONDS = 5.0


class QuorumNotReached(RuntimeError):
    """Fewer than W replicas accepted a write, so it is not durable enough."""


class ReplicationClient:
    def __init__(
        self,
        settings: Settings,
        blob_store: BlobStore,
        channels: ChannelPool | None = None,
    ) -> None:
        self._settings = settings
        self._blob_store = blob_store
        self._channels = channels or ChannelPool()

    def put_blob(self, content: bytes) -> str:
        """Write content to its replica set, requiring W acknowledgements.

        Returns the content hash. Raises QuorumNotReached if fewer than W
        replicas stored it, in which case the caller must not record metadata
        pointing at this hash.
        """
        content_hash = hashlib.sha256(content).hexdigest()
        replicas = replica_set(
            content_hash, self._settings.node_ids, self._settings.replication_factor
        )
        # A cluster smaller than R cannot produce R acks, so the requirement
        # shrinks with it. Without this, a single-node deployment could never
        # complete a write.
        required = min(self._settings.write_quorum, len(replicas))

        acknowledged = 0
        if self._settings.node_id in replicas:
            # The local write is just another replica, done without the network.
            self._blob_store.put(content)
            acknowledged += 1

        remotes = [n for n in replicas if n != self._settings.node_id]
        if remotes:
            with ThreadPoolExecutor(max_workers=len(remotes)) as pool:
                results = pool.map(
                    lambda node_id: self._put_remote(node_id, content, content_hash),
                    remotes,
                )
                acknowledged += sum(1 for succeeded in results if succeeded)

        if acknowledged < required:
            raise QuorumNotReached(
                f"only {acknowledged} of {len(replicas)} replicas stored "
                f"{content_hash[:12]}, need {required}"
            )
        return content_hash

    def get_blob(self, content_hash: str) -> bytes | None:
        """Return the content, from this node if it has it or from a replica.

        Returns None when no replica could serve it — a transient availability
        problem, which the API layer surfaces as 503.
        """
        local = self._blob_store.get(content_hash)
        if local is not None:
            return local

        # HRW order doubles as a preference order: every node tries the replicas
        # in the same sequence, so reads concentrate on the same node and benefit
        # from its page cache.
        for node_id in replica_set(
            content_hash, self._settings.node_ids, self._settings.replication_factor
        ):
            if node_id == self._settings.node_id:
                continue
            content = self._get_remote(node_id, content_hash)
            if content is not None:
                return content
        return None

    def _put_remote(self, node_id: str, content: bytes, content_hash: str) -> bool:
        address = self._settings.peer_address(node_id)
        if address is None:
            logger.warning("no address known for replica %s", node_id)
            return False
        try:
            stub = pb_grpc.ReplicationStub(self._channels.channel(address))
            stub.PutBlob(
                _chunk(content, content_hash), timeout=RPC_TIMEOUT_SECONDS
            )
            return True
        except grpc.RpcError as error:
            # Swallowed on purpose: a single failed replica is not a failed
            # write, it is one fewer acknowledgement. The quorum check decides.
            logger.warning(
                "replica %s rejected blob %s: %s", node_id, content_hash[:12], error
            )
            return False

    def _get_remote(self, node_id: str, content_hash: str) -> bytes | None:
        address = self._settings.peer_address(node_id)
        if address is None:
            return None
        try:
            stub = pb_grpc.ReplicationStub(self._channels.channel(address))
            received = b"".join(
                chunk.data
                for chunk in stub.GetBlob(
                    pb.BlobRequest(content_hash=content_hash),
                    timeout=RPC_TIMEOUT_SECONDS,
                )
            )
        except grpc.RpcError as error:
            logger.info("replica %s could not serve %s: %s", node_id, content_hash[:12], error)
            return None

        # Content is addressed by its hash, so the address is also a checksum.
        # Verifying is nearly free and turns silent corruption into a miss.
        if hashlib.sha256(received).hexdigest() != content_hash:
            logger.error(
                "replica %s served content that does not match %s", node_id, content_hash[:12]
            )
            return None
        return received

    def close(self) -> None:
        self._channels.close()


def _chunk(content: bytes, content_hash: str):
    for offset in range(0, len(content), CHUNK_SIZE):
        yield pb.BlobChunk(
            content_hash=content_hash, data=content[offset : offset + CHUNK_SIZE]
        )
```

**Note on a deliberate simplification:** `put_blob` waits for all R replicas and
*then* checks whether W succeeded, rather than acknowledging the client as soon as
the W-th ack arrives. Early acknowledgement is the Dynamo behaviour and is faster,
but it only makes sense once something repairs the replicas that were still in
flight or failed — which is Phase 4's anti-entropy. Doing it now would mean
silently accepting writes that no mechanism ever completes. `pool.map` also
preserves this simplicity: it waits for every replica and yields results in order.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/integration/cluster/test_replication_client.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add almacen/cluster/replication_client.py tests/integration/cluster/
git commit -m "Add replication client with W-of-R write quorum and fallback reads"
```

---

### Task 12: Metadata replication

**Files:**
- Create: `almacen/storage/protocols.py`, `almacen/cluster/metadata_replicator.py`, `almacen/cluster/replicated_metadata_store.py`
- Test: `tests/integration/cluster/test_metadata_replication.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/cluster/test_metadata_replication.py
from pathlib import Path

import pytest

from almacen.cluster.metadata_replicator import MetadataReplicator
from almacen.cluster.replicated_metadata_store import ReplicatedMetadataStore
from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port

NODE_COUNT = 3


@pytest.fixture
def three_nodes(tmp_path: Path):
    """Three real nodes; index 0 is the coordinator under test."""
    stores: list[MetadataStore] = []
    servers = []
    peers: list[Peer] = []

    for index in range(1, NODE_COUNT + 1):
        node_id = f"node{index}"
        port = free_port()
        metadata_store = MetadataStore(tmp_path / node_id / "metadata.db")
        server = build_server(
            blob_store=BlobStore(tmp_path / node_id / "blobs"),
            metadata_store=metadata_store,
            node_id=node_id,
            port=port,
        )
        server.start()
        stores.append(metadata_store)
        servers.append(server)
        peers.append(Peer(node_id=node_id, address=f"127.0.0.1:{port}"))

    settings = Settings(
        data_dir=tmp_path / "node1" / "blobs",
        db_path=tmp_path / "node1" / "metadata.db",
        node_id="node1",
        peers=tuple(peers),
    )
    replicated = ReplicatedMetadataStore(stores[0], MetadataReplicator(settings))

    yield replicated, stores, servers

    for server in servers:
        server.stop(0).wait()


def test_insert_reaches_every_peer(three_nodes):
    replicated, stores, _ = three_nodes
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})

    replicated.insert(record)

    for index, store in enumerate(stores):
        stored = store.get(record.file_id)
        assert stored is not None, f"node{index + 1} never received the record"
        assert stored.name == "a.txt"
        assert stored.tags == {"x"}


def test_update_reaches_every_peer(three_nodes):
    replicated, stores, _ = three_nodes
    record = FileRecord.new(name="a.txt", content_hash="h1")
    replicated.insert(record)

    record.rename("renamed.txt")
    replicated.update(record)

    for store in stores:
        stored = store.get(record.file_id)
        assert stored is not None and stored.name == "renamed.txt"


def test_delete_propagates_as_a_tombstone(three_nodes):
    replicated, stores, _ = three_nodes
    record = FileRecord.new(name="a.txt", content_hash="h1")
    replicated.insert(record)

    record.mark_deleted()
    replicated.update(record)

    for store in stores:
        assert store.list_live() == []


def test_a_dead_peer_does_not_fail_the_write(three_nodes):
    replicated, stores, servers = three_nodes
    servers[2].stop(0).wait()  # node3 is gone

    record = FileRecord.new(name="a.txt", content_hash="h1")
    replicated.insert(record)  # must not raise

    # The reachable nodes have it; node3's divergence is Phase 4's problem.
    assert stores[0].get(record.file_id) is not None
    assert stores[1].get(record.file_id) is not None


def test_reads_are_served_locally(three_nodes):
    replicated, stores, servers = three_nodes
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})
    replicated.insert(record)

    for server in servers[1:]:
        server.stop(0).wait()

    assert replicated.get(record.file_id) is not None
    assert len(replicated.list_live()) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/cluster/test_metadata_replication.py -v`
Expected: FAIL — no module `almacen.cluster.metadata_replicator`.

- [ ] **Step 3: Write the Protocol**

The wrapper is not a `MetadataStore` subclass, so annotating it as one would be a
lie. A Protocol describes what the API layer actually needs.

```python
# almacen/storage/protocols.py
"""Structural types for the storage layer."""
from __future__ import annotations

import uuid
from typing import Protocol

from almacen.domain.file_record import FileRecord


class MetadataStoreLike(Protocol):
    """What the API layer needs from a metadata store.

    Satisfied by both the local `MetadataStore` and the cluster's
    `ReplicatedMetadataStore`, which lets the routers stay unaware of whether
    they are running in a cluster.
    """

    def insert(self, record: FileRecord) -> None: ...

    def get(self, file_id: uuid.UUID) -> FileRecord | None: ...

    def update(self, record: FileRecord) -> None: ...

    def list_live(self) -> list[FileRecord]: ...
```

- [ ] **Step 4: Write the replicator**

```python
# almacen/cluster/metadata_replicator.py
"""Pushes metadata records to every peer (spec §4: full metadata replication)."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import grpc

from almacen.cluster.channels import ChannelPool
from almacen.config import Settings
from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.record_codec import record_to_message

logger = logging.getLogger(__name__)

RPC_TIMEOUT_SECONDS = 5.0


class MetadataReplicator:
    """Best-effort push of a record to all peers.

    Deliberately best-effort: unlike content (where W acks are a durability
    guarantee from spec §3), metadata replication failing must not turn a
    successful local write into a client-visible error. A peer that misses a push
    diverges, and nothing here repairs it — that is exactly the gap Phase 4's
    push-pull anti-entropy closes.
    """

    def __init__(self, settings: Settings, channels: ChannelPool | None = None) -> None:
        self._settings = settings
        self._channels = channels or ChannelPool()

    def replicate(self, record: FileRecord) -> None:
        peers = self._settings.remote_peers
        if not peers:
            return

        message = record_to_message(record)
        with ThreadPoolExecutor(max_workers=len(peers)) as pool:
            # list() forces every push to complete; exceptions are handled inside
            # _push, so nothing escapes to the caller.
            list(pool.map(lambda peer: self._push(peer, message), peers))

    def _push(self, peer, message) -> None:
        try:
            stub = pb_grpc.ClusterStub(self._channels.channel(peer.address))
            stub.ReplicateRecord(message, timeout=RPC_TIMEOUT_SECONDS)
        except grpc.RpcError as error:
            logger.warning(
                "could not replicate record %s to %s: %s",
                message.file_id,
                peer.node_id,
                error,
            )

    def close(self) -> None:
        self._channels.close()
```

- [ ] **Step 5: Write the decorator**

```python
# almacen/cluster/replicated_metadata_store.py
"""A MetadataStore that also pushes its writes to the rest of the cluster."""
from __future__ import annotations

import uuid

from almacen.cluster.metadata_replicator import MetadataReplicator
from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore


class ReplicatedMetadataStore:
    """Writes locally, then replicates; reads are always local.

    Implemented as a decorator rather than as calls sprinkled through the
    routers: there are five mutation sites, and wrapping the store means none of
    them can forget to replicate. Reads need no cluster involvement because
    every node holds a full metadata replica (spec §4).
    """

    def __init__(self, local: MetadataStore, replicator: MetadataReplicator) -> None:
        self._local = local
        self._replicator = replicator

    @property
    def local(self) -> MetadataStore:
        """The underlying local store, for components that must not replicate."""
        return self._local

    def insert(self, record: FileRecord) -> None:
        self._local.insert(record)
        self._replicator.replicate(record)

    def update(self, record: FileRecord) -> None:
        self._local.update(record)
        self._replicator.replicate(record)

    def get(self, file_id: uuid.UUID) -> FileRecord | None:
        return self._local.get(file_id)

    def list_live(self) -> list[FileRecord]:
        return self._local.list_live()
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/integration/cluster/test_metadata_replication.py -v`
Expected: 5 passed

- [ ] **Step 7: Commit**

```bash
git add almacen/storage/protocols.py almacen/cluster/metadata_replicator.py almacen/cluster/replicated_metadata_store.py tests/integration/cluster/test_metadata_replication.py
git commit -m "Add best-effort metadata replication to all peers"
```

---

### Task 13: Wire the cluster into the API

**Files:**
- Modify: `almacen/main.py`, `almacen/api/deps.py`, `almacen/api/routers/files.py`
- Test: `tests/integration/api/test_cluster_error_paths.py`

**⚠️ The one thing to get right in this task:** `grpc.Server.add_insecure_port()`
**binds the socket immediately**. `almacen/main.py` ends with a module-level
`app = create_app()`, which runs on *import* — so building the gRPC server inside
`create_app` would bind port 50051 every time anything imports `almacen.main`,
including pytest collection, and would make the 39 existing API tests fight over
one port. The server is therefore built **inside the lifespan handler**, which only
runs when the app is actually served (or when a test opts in with
`with TestClient(app)`).

- [ ] **Step 1: Write the failing tests**

These cover the two new failure modes the cluster introduces. Neither needs a real
cluster — unreachable peer addresses are enough.

```python
# tests/integration/api/test_cluster_error_paths.py
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.main import create_app


@pytest.fixture
def client_with_dead_peers(tmp_path: Path) -> TestClient:
    """A node whose two peers do not exist, so W=2 is unreachable."""
    settings = Settings(
        data_dir=tmp_path / "blobs",
        db_path=tmp_path / "metadata.db",
        node_id="node1",
        peers=(
            Peer("node1", "127.0.0.1:59991"),
            Peer("node2", "127.0.0.1:59992"),  # nothing listening
            Peer("node3", "127.0.0.1:59993"),  # nothing listening
        ),
        replication_factor=3,
        write_quorum=2,
    )
    # No lifespan: this node's own gRPC server is irrelevant here, and not
    # binding it keeps the test independent of port availability.
    return TestClient(create_app(settings))


def test_upload_returns_503_when_the_write_quorum_cannot_be_met(client_with_dead_peers):
    response = client_with_dead_peers.post(
        "/files",
        files={"file": ("a.txt", b"content", "text/plain")},
        data={"name": "a.txt"},
    )
    assert response.status_code == 503


def test_no_metadata_is_recorded_when_the_write_quorum_fails(client_with_dead_peers):
    client_with_dead_peers.post(
        "/files",
        files={"file": ("a.txt", b"content", "text/plain")},
        data={"name": "a.txt"},
    )
    # A record pointing at content that is not durable would be a dangling
    # reference — the upload must leave no trace.
    assert client_with_dead_peers.get("/files").json() == []


def test_download_returns_503_when_no_replica_holds_the_blob(tmp_path: Path):
    settings = Settings(data_dir=tmp_path / "blobs", db_path=tmp_path / "metadata.db")
    app = create_app(settings)
    client = TestClient(app)

    # A record whose content was never stored anywhere: the file is known, the
    # bytes are unreachable.
    record = FileRecord.new(name="ghost.txt", content_hash="c" * 64)
    app.state.local_metadata_store.insert(record)

    response = client.get(f"/files/{record.file_id}")

    assert response.status_code == 503, "the file exists, so 404 would be a lie"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/api/test_cluster_error_paths.py -v`
Expected: FAIL — uploads currently return 201 (no replication yet), and the
download raises `RuntimeError` → 500 rather than 503.

- [ ] **Step 3: Rewrite `main.py`**

```python
# almacen/main.py
"""FastAPI application bootstrap for a single node of the cluster."""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from almacen.api.routers import files as files_router
from almacen.api.routers import tags as tags_router
from almacen.cluster.channels import ChannelPool
from almacen.cluster.metadata_replicator import MetadataReplicator
from almacen.cluster.replicated_metadata_store import ReplicatedMetadataStore
from almacen.cluster.replication_client import ReplicationClient
from almacen.config import Settings
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex

logger = logging.getLogger(__name__)

GRPC_SHUTDOWN_GRACE_SECONDS = 2.0


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Run the node's gRPC server for as long as the HTTP app is serving.

    The server is built here, not in `create_app`, because `add_insecure_port`
    binds the socket immediately and `create_app` runs at import time.
    """
    settings: Settings = app.state.settings
    server = build_server(
        blob_store=app.state.blob_store,
        # The local store, never the replicating wrapper: records arriving from
        # peers must not be pushed straight back out.
        metadata_store=app.state.local_metadata_store,
        node_id=settings.node_id,
        port=settings.grpc_port,
    )
    server.start()
    app.state.grpc_server = server
    logger.info(
        "node %s serving gRPC on port %s, peers: %s",
        settings.node_id,
        settings.grpc_port,
        [p.node_id for p in settings.remote_peers],
    )
    try:
        yield
    finally:
        server.stop(GRPC_SHUTDOWN_GRACE_SECONDS).wait()
        app.state.channels.close()
        logger.info("node %s stopped", settings.node_id)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(title="Almacen Distribuido", lifespan=_lifespan)

    blob_store = BlobStore(settings.data_dir)
    local_metadata_store = MetadataStore(settings.db_path)
    # One pool shared by both cluster clients, so peers get one connection each.
    channels = ChannelPool()

    app.state.settings = settings
    app.state.channels = channels
    app.state.blob_store = blob_store
    app.state.local_metadata_store = local_metadata_store
    app.state.replication_client = ReplicationClient(settings, blob_store, channels)
    app.state.metadata_store = ReplicatedMetadataStore(
        local_metadata_store, MetadataReplicator(settings, channels)
    )
    # Queries read the local replica directly; there is nothing to replicate.
    app.state.tag_index = TagIndex(local_metadata_store)

    app.include_router(files_router.router)
    app.include_router(tags_router.router)

    return app


app = create_app()
```

- [ ] **Step 4: Update `deps.py`**

Two changes: a provider for the replication client, and honest types now that the
metadata store may be the wrapper.

```python
# almacen/api/deps.py
"""FastAPI dependency providers and shared request-handling helpers."""
from __future__ import annotations

import uuid

from fastapi import HTTPException, Request

from almacen.cluster.replication_client import ReplicationClient
from almacen.domain.file_record import FileRecord
from almacen.storage.blob_store import BlobStore
from almacen.storage.protocols import MetadataStoreLike
from almacen.storage.tag_index import TagIndex


def get_blob_store(request: Request) -> BlobStore:
    return request.app.state.blob_store


def get_metadata_store(request: Request) -> MetadataStoreLike:
    return request.app.state.metadata_store


def get_tag_index(request: Request) -> TagIndex:
    return request.app.state.tag_index


def get_replication_client(request: Request) -> ReplicationClient:
    return request.app.state.replication_client


def get_live_record(
    metadata_store: MetadataStoreLike, file_id: uuid.UUID
) -> FileRecord:
    """Look up a file, raising 404 if it's missing or tombstoned (spec §10: a
    tombstoned file is indistinguishable from an unknown one via the API)."""
    record = metadata_store.get(file_id)
    if record is None or record.tombstone:
        raise HTTPException(status_code=404, detail="file not found")
    return record
```

- [ ] **Step 5: Update `files.py`**

The router no longer touches `BlobStore` at all — content goes through the
replication client. Replace the three affected handlers; `list_files` is unchanged.

```python
# almacen/api/routers/files.py — replace the imports and the three handlers below

from almacen.api.deps import (
    get_live_record,
    get_metadata_store,
    get_replication_client,
    get_tag_index,
)
from almacen.cluster.replication_client import QuorumNotReached, ReplicationClient
from almacen.storage.protocols import MetadataStoreLike
# `get_blob_store` and `BlobStore` are no longer used here.


@router.post("", response_model=FileMetadata, status_code=201)
async def upload_file(
    file: UploadFile,
    name: Annotated[str | None, Form()] = None,
    tags: Annotated[str | None, Form()] = None,
    replication_client: ReplicationClient = Depends(get_replication_client),
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
) -> FileMetadata:
    content = await file.read()
    try:
        content_hash = replication_client.put_blob(content)
    except QuorumNotReached as error:
        # Recording metadata now would leave a file pointing at content that is
        # not durable, so nothing is written.
        raise HTTPException(status_code=503, detail=str(error)) from error
    record = FileRecord.new(
        name=name or file.filename or "untitled",
        content_hash=content_hash,
        tags=_parse_tags(tags),
    )
    metadata_store.insert(record)
    return to_file_metadata(record)


@router.get("/{file_id}")
def download_file(
    file_id: uuid.UUID,
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
    replication_client: ReplicationClient = Depends(get_replication_client),
) -> Response:
    record = get_live_record(metadata_store, file_id)
    content = replication_client.get_blob(record.content_hash)
    if content is None:
        # The file exists; its bytes are temporarily unreachable. 404 would deny
        # the file, 500 would blame this node.
        raise HTTPException(
            status_code=503,
            detail=f"no replica currently holds content for file {file_id}",
        )
    return Response(
        content=content,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{record.name}"'},
    )


@router.patch("/{file_id}", response_model=FileMetadata)
async def update_file(
    file_id: uuid.UUID,
    file: Annotated[UploadFile | None, File()] = None,
    name: Annotated[str | None, Form()] = None,
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
    replication_client: ReplicationClient = Depends(get_replication_client),
) -> FileMetadata:
    record = get_live_record(metadata_store, file_id)
    if name is not None:
        record.rename(name)
    if file is not None:
        content = await file.read()
        try:
            content_hash = replication_client.put_blob(content)
        except QuorumNotReached as error:
            raise HTTPException(status_code=503, detail=str(error)) from error
        record.update_content(content_hash)
    metadata_store.update(record)
    return to_file_metadata(record)
```

Also update `list_files`' and `delete_file`'s `metadata_store` annotations to
`MetadataStoreLike`, and do the same in `almacen/api/routers/tags.py` (three
handlers). No logic changes there.

**Why `upload_file` and `update_file` stay `async def`, even though they now call
blocking code.** `replication_client.put_blob` blocks while it writes to R
replicas, and in an `async def` handler that occupies the event loop rather than a
worker thread. They stay `async` because they must `await file.read()` —
`UploadFile.read()` is a coroutine, so a plain `def` handler cannot call it; it
would have to reach for the underlying synchronous `file.file.read()` instead. In
practice this is fine here: the gRPC calls execute on gRPC's own threads, so the
loop is blocked only for the handful of milliseconds of local hashing and the
wait. Left as a deliberate, documented choice so nobody "fixes" it blindly in
either direction. If replication latency ever becomes a problem, the right move is
`def` handlers plus `file.file.read()`, not scattering `run_in_executor` calls.

- [ ] **Step 6: Run the new tests**

Run: `pytest tests/integration/api/test_cluster_error_paths.py -v`
Expected: 3 passed

- [ ] **Step 7: Confirm the whole Phase 1 API surface still works**

Run: `pytest -q`
Expected: all pass. The 13 Phase 1 API tests must still pass **unchanged** — they
run as a one-node cluster where R and W both collapse to 1.

- [ ] **Step 8: Commit**

```bash
git add almacen/main.py almacen/api/ tests/integration/api/test_cluster_error_paths.py
git commit -m "Route content through the replication client and serve gRPC alongside HTTP"
```

---

### Task 14: Five-node end-to-end test

The test that proves the phase. Five real nodes, five real gRPC servers, five
separate data directories, driven entirely through the public REST API.

**Files:**
- Create: `tests/cluster/__init__.py` (empty), `tests/cluster/test_five_node_cluster.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/cluster/test_five_node_cluster.py
"""End-to-end: a five-node cluster behaves like one store from any entry point."""
from collections.abc import Iterator
from contextlib import ExitStack
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from almacen.cluster.placement import replica_set
from almacen.config import Peer, Settings
from almacen.main import create_app
from tests.helpers import free_port

NODE_COUNT = 5


@pytest.fixture
def cluster(tmp_path: Path) -> Iterator[dict[str, TestClient]]:
    """Five nodes, each with its own storage, all sharing one membership list.

    Ports are allocated before any app is built so that every node's peer list
    contains every other node's real address.
    """
    ports = {f"node{i}": free_port() for i in range(1, NODE_COUNT + 1)}
    peers = tuple(
        Peer(node_id=node_id, address=f"127.0.0.1:{port}")
        for node_id, port in ports.items()
    )

    with ExitStack() as stack:
        clients: dict[str, TestClient] = {}
        for node_id, port in ports.items():
            settings = Settings(
                data_dir=tmp_path / node_id / "blobs",
                db_path=tmp_path / node_id / "metadata.db",
                node_id=node_id,
                grpc_port=port,
                peers=peers,
                replication_factor=3,
                write_quorum=2,
            )
            # Entering the TestClient context runs the lifespan, which starts the
            # node's real gRPC server.
            clients[node_id] = stack.enter_context(TestClient(create_app(settings)))
        yield clients


def test_a_file_uploaded_to_one_node_downloads_from_every_other(cluster):
    content = b"the same bytes everywhere"

    upload = cluster["node1"].post(
        "/files",
        files={"file": ("report.txt", content, "text/plain")},
        data={"name": "report.txt", "tags": "invoice,urgent"},
    )
    assert upload.status_code == 201
    file_id = upload.json()["file_id"]

    for node_id, client in cluster.items():
        download = client.get(f"/files/{file_id}")
        assert download.status_code == 200, f"{node_id} could not serve the content"
        assert download.content == content


def test_content_lands_on_exactly_three_of_five_nodes(cluster, tmp_path: Path):
    upload = cluster["node2"].post(
        "/files",
        files={"file": ("a.txt", b"sharded content", "text/plain")},
        data={"name": "a.txt"},
    )
    content_hash = upload.json()["content_hash"]

    holders = {
        node_id
        for node_id in cluster
        if (tmp_path / node_id / "blobs" / content_hash).exists()
    }

    assert len(holders) == 3, f"expected R=3 replicas, content is on {holders}"
    assert holders == set(replica_set(content_hash, list(cluster), r=3))


def test_metadata_is_queryable_from_every_node(cluster):
    cluster["node3"].post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "invoice,draft"},
    )
    cluster["node4"].post(
        "/files",
        files={"file": ("b.txt", b"b", "text/plain")},
        data={"name": "b.txt", "tags": "invoice"},
    )

    for node_id, client in cluster.items():
        both = client.get("/files", params={"tags": "invoice", "mode": "or"})
        assert {f["name"] for f in both.json()} == {"a.txt", "b.txt"}, (
            f"{node_id} has an incomplete metadata replica"
        )

        intersection = client.get("/files", params={"tags": "invoice,draft"})
        assert [f["name"] for f in intersection.json()] == ["a.txt"]


def test_a_tag_added_on_one_node_is_visible_on_the_others(cluster):
    upload = cluster["node1"].post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "draft"},
    )
    file_id = upload.json()["file_id"]

    cluster["node5"].post(f"/files/{file_id}/tags", json={"tags": ["final"]})

    for node_id, client in cluster.items():
        tags = client.get(f"/files/{file_id}/tags").json()["tags"]
        assert sorted(tags) == ["draft", "final"], f"{node_id} missed the tag change"


def test_a_delete_on_one_node_tombstones_the_file_everywhere(cluster):
    upload = cluster["node1"].post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]

    assert cluster["node2"].delete(f"/files/{file_id}").status_code == 204

    for node_id, client in cluster.items():
        assert client.get(f"/files/{file_id}").status_code == 404, node_id
        assert client.get("/files").json() == [], node_id


def test_content_update_propagates_to_the_new_replica_set(cluster):
    upload = cluster["node1"].post(
        "/files",
        files={"file": ("a.txt", b"version one", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]

    patch = cluster["node3"].patch(
        f"/files/{file_id}", files={"file": ("a.txt", b"version two", "text/plain")}
    )
    assert patch.status_code == 200

    for node_id, client in cluster.items():
        assert client.get(f"/files/{file_id}").content == b"version two", node_id
```

- [ ] **Step 2: Run the test**

Run: `pytest tests/cluster/test_five_node_cluster.py -v`
Expected: 6 passed. If `test_content_lands_on_exactly_three_of_five_nodes` fails
with 4 or 5 holders, the coordinator is keeping a copy it is not a replica for
(decision 3) — fix `put_blob`, not the test.

- [ ] **Step 3: Commit**

```bash
git add tests/cluster/
git commit -m "Add five-node end-to-end cluster test"
```

---

### Task 15: Docker image and five-node Compose cluster

**Files:**
- Create: `docker/Dockerfile`, `docker/docker-compose.yml`

No `.gitignore` change is needed: the Compose file uses *named* volumes, which
Docker keeps in its own storage rather than in the working tree.

- [ ] **Step 1: Write the Dockerfile**

Python 3.13 rather than 3.14: `grpcio` and `protobuf` both ship 3.13 wheels
reliably, so the image builds without a compiler.

```dockerfile
# docker/Dockerfile
FROM python:3.13-slim

WORKDIR /app

# Copy only what the install needs first, so dependency layers cache across
# source edits.
COPY pyproject.toml ./
COPY almacen ./almacen

RUN pip install --no-cache-dir .

# Blobs and the SQLite database live here; mounted as a volume per node.
ENV ALMACEN_DATA_DIR=/data
RUN mkdir -p /data

EXPOSE 8000 50051

CMD ["uvicorn", "almacen.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

The committed protobuf stubs (Task 5, decision 6) are what make this work with a
plain `pip install .` and no `grpcio-tools` in the image.

- [ ] **Step 2: Write the Compose file**

Every node gets the identical `ALMACEN_PEERS` value — that is what "static
membership" means here. Peers address each other by Compose service name on the
internal network, which is why `build_server` binds `[::]` and not loopback.

```yaml
# docker/docker-compose.yml
name: almacen

x-node: &node
  build:
    context: ..
    dockerfile: docker/Dockerfile
  environment: &node-env
    ALMACEN_DATA_DIR: /data
    ALMACEN_GRPC_PORT: "50051"
    ALMACEN_REPLICATION_FACTOR: "3"
    ALMACEN_WRITE_QUORUM: "2"
    ALMACEN_PEERS: "node1@node1:50051,node2@node2:50051,node3@node3:50051,node4@node4:50051,node5@node5:50051"

services:
  node1:
    <<: *node
    environment:
      <<: *node-env
      ALMACEN_NODE_ID: node1
    ports: ["8001:8000"]
    volumes: ["node1-data:/data"]

  node2:
    <<: *node
    environment:
      <<: *node-env
      ALMACEN_NODE_ID: node2
    ports: ["8002:8000"]
    volumes: ["node2-data:/data"]

  node3:
    <<: *node
    environment:
      <<: *node-env
      ALMACEN_NODE_ID: node3
    ports: ["8003:8000"]
    volumes: ["node3-data:/data"]

  node4:
    <<: *node
    environment:
      <<: *node-env
      ALMACEN_NODE_ID: node4
    ports: ["8004:8000"]
    volumes: ["node4-data:/data"]

  node5:
    <<: *node
    environment:
      <<: *node-env
      ALMACEN_NODE_ID: node5
    ports: ["8005:8000"]
    volumes: ["node5-data:/data"]

volumes:
  node1-data:
  node2-data:
  node3-data:
  node4-data:
  node5-data:
```

- [ ] **Step 3: Build and start the cluster**

Run: `docker compose -f docker/docker-compose.yml up --build -d`
Expected: five containers running. Check with
`docker compose -f docker/docker-compose.yml ps`.

- [ ] **Step 4: Verify replication across real containers by hand**

```bash
# Upload to node1.
FILE_ID=$(curl -s -F "file=@README.md" -F "name=README.md" -F "tags=demo" \
  http://127.0.0.1:8001/files | python -c "import sys,json; print(json.load(sys.stdin)['file_id'])")

# Download from node4 — a different node entirely.
curl -s http://127.0.0.1:8004/files/$FILE_ID | head -3

# Query metadata from node5.
curl -s "http://127.0.0.1:8005/files?tags=demo"

# Count which containers actually hold the blob: must be exactly 3 of 5.
for n in 1 2 3 4 5; do
  echo -n "node$n: "
  docker compose -f docker/docker-compose.yml exec -T node$n sh -c 'ls /data/blobs | wc -l'
done
```

Expected: the download from node4 returns the file, node5 lists it, and exactly
three of the five nodes report a blob.

- [ ] **Step 5: Tear the cluster down**

Run: `docker compose -f docker/docker-compose.yml down -v`

- [ ] **Step 6: Commit**

```bash
git add docker/
git commit -m "Add Docker image and five-node Compose cluster"
```

---

### Task 16: Documentation and full verification

**Files:**
- Modify: `README.md`
- Create: `../DAA/write-quorum-and-read-repair.md`

- [ ] **Step 1: Run the entire suite**

Run: `pytest -q`
Expected: 0 failures. Summing the per-task expectations gives about 101 tests
(39 from Phase 1 plus roughly 62 new). The exact count matters far less than there
being no failures and no skips.

- [ ] **Step 2: Write the DAA note on quorum choice**

`DAA/write-quorum-and-read-repair.md` — again, problem then solution with the
reasoning. Cover:

- **Problem:** choose W and the read path so a write survives node loss without making every write wait for the slowest node.
- **Why W=2, R=3 and read-one is sound here even though `W + R_read = 3 < R + 1 = 4`.** The classic strong-consistency inequality `W + R_read > R` does *not* need to hold for this data, because blobs are **immutable and content-addressed**: there is only ever one possible value for a given `content_hash`, so a stale read is impossible by construction. Reading a single replica cannot return an outdated version — only a missing one. This is the key insight, and it is what makes read-one safe. Contrast explicitly with the mutable metadata plane, where LWW/CRDT merge is needed precisely because values *do* have competing versions.
- **The read is self-verifying.** Since the address is the SHA-256 of the content, the reader checks the hash of what it received and treats a mismatch as a miss (`_get_remote`). That converts silent corruption into a fallback to the next replica, which a non-content-addressed store could not do without extra checksums.
- **Durability arithmetic:** W=2 of R=3 tolerates one replica loss with the write still acknowledged, and one further loss with the data still present on one node. Compare against W=1 (faster, but a single disk loss can lose an acknowledged write) and W=3 (no tolerance for a single slow/down node — availability collapses to the least available replica, contradicting the AP choice in spec §3).
- **Cost:** O(R) network transfers per write regardless of W, since all R are attempted; W only decides when to declare success. Note the deliberate simplification that this implementation waits for all R before evaluating the quorum, so write latency is that of the *slowest* replica rather than the W-th fastest — and state why (early acknowledgement needs Phase 4's anti-entropy to finish the laggards, otherwise it accepts writes nothing ever completes).
- **Read latency:** O(1) network calls in the common case (local hit), worst case O(R) when the coordinator is not a replica and the first replicas tried are down.

- [ ] **Step 3: Update the README**

Replace the `## Status` section and add cluster documentation. Keep the existing
Setup / API / Layout sections, extending Layout with the new packages.

```markdown
## Status

Phase 2 (static cluster) — a fixed set of nodes replicate content and metadata.
Content is stored on R=3 of N=5 nodes chosen by rendezvous hashing, acknowledged
after W=2 replicas confirm. Metadata is replicated in full to every node, so any
node answers any query. No failure handling yet: membership is static (no
gossip), and concurrent conflicting writes are resolved by arrival order rather
than CRDT merge. Those are Phases 3 and 4.

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

With `ALMACEN_PEERS` unset the node is a one-node cluster and R and W collapse to
1, which is how local development and most of the test suite run.

## Run a five-node cluster

    docker compose -f docker/docker-compose.yml up --build -d

Nodes are exposed on ports 8001-8005. Any node accepts any request:

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
```

Also add to `## Layout`:

```
      rpc/               # node-to-node gRPC: protos, generated stubs, servicers
      cluster/           # placement (HRW), replication client, metadata replication
    tests/
      cluster/           # multi-node end-to-end
```

- [ ] **Step 4: Verify the documentation matches reality**

Run: `pytest -q && grep -c ALMACEN_ README.md`
Expected: tests pass, and every environment variable the README lists is one
`config.py` actually reads. Check each name against `Settings.from_env`.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "Document cluster configuration, Compose demo, and stub regeneration"
```

- [ ] **Step 6: Confirm the repository hygiene rules still hold**

The project rules file at the project root (a sibling of `DAA/`, outside this
repository) forbids committing references to the tooling used to develop the
project, and requires `DAA/` to stay untracked. Run the tracked-file scan it
specifies for the terms it names, plus:

```bash
git status --short   # DAA/ must not appear
```

Expected: the scan reports no matches, and no `DAA/` in the status output.

---

## Definition of done

- [ ] `pytest -q` passes with no failures and no skips.
- [ ] A file uploaded to any node downloads from every other node.
- [ ] Content for a given hash exists on exactly R=3 of the 5 nodes, and they are the HRW-selected three.
- [ ] Tag changes and deletes made on one node are visible on all five.
- [ ] Killing one replica of a blob still allows writes (W=2 reachable); killing two rejects them with 503.
- [ ] `docker compose -f docker/docker-compose.yml up --build -d` brings up five working nodes.
- [ ] `DAA/rendezvous-hashing.md` and `DAA/write-quorum-and-read-repair.md` exist and are untracked.
- [ ] The tracked-file and commit-message hygiene rules from the project rules file are satisfied.
