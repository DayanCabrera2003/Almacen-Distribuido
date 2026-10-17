"""Random client operations, issued over HTTP at random running nodes.

Every response is checked against what the receiving node can know at that
moment, not merely against a set of plausible codes: an upload must succeed
exactly when W of its replicas are reachable, an edit must be a 404 exactly when
the node holds no live record, a download must be served exactly when some
reachable copy exists. A wrong status is either a bug or a wrong model of the
system, and both are worth a failing test.
"""
from __future__ import annotations

import hashlib
import random
import uuid
from collections.abc import Mapping
from dataclasses import dataclass

from almacen.cluster.placement import replica_set
from tests.chaos.cluster import NODE_IDS, ChaosCluster, ChaosNode
from tests.chaos.history import FileTruth, History

TAGS = ("red", "green", "blue", "urgent", "draft")
REPLICATION_FACTOR = 3
WRITE_QUORUM = 2


@dataclass(frozen=True)
class OperationMix:
    """Relative weights of each operation."""

    upload: int = 6
    patch_content: int = 2
    rename: int = 2
    add_tags: int = 4
    remove_tag: int = 3
    delete: int = 2
    download: int = 4
    # Chance that new content repeats a live file's bytes, sharing its blob.
    dedup_probability: float = 0.15

    def weights(self) -> Mapping[str, int]:
        return {
            "upload": self.upload,
            "patch_content": self.patch_content,
            "rename": self.rename,
            "add_tags": self.add_tags,
            "remove_tag": self.remove_tag,
            "delete": self.delete,
            "download": self.download,
        }


class OperationGenerator:
    def __init__(
        self,
        cluster: ChaosCluster,
        history: History,
        rng: random.Random,
        mix: OperationMix,
    ) -> None:
        self._cluster = cluster
        self._history = history
        self._rng = rng
        self._mix = mix
        self._kinds = list(mix.weights())
        self._weights = list(mix.weights().values())

    def random_operation(self) -> None:
        up = self._cluster.up_nodes()
        if not up:
            return
        node = self._rng.choice(up)
        kind = self._rng.choices(self._kinds, self._weights)[0]
        if kind == "upload" or not self._history.files:
            self.upload(node)
            return
        target = self._pick_file()
        getattr(self, kind)(node, target)

    def _pick_file(self) -> FileTruth:
        # Mostly live files; deleted ones too, to exercise the 404 paths and a
        # node that has not yet heard of the delete.
        live = self._history.live_files()
        if live and self._rng.random() < 0.85:
            return self._rng.choice(live)
        return self._rng.choice(self._history.files)

    # ---- operations ----

    def upload(self, node: ChaosNode) -> None:
        seq = self._history.next_seq()
        content = self._new_content(seq)
        name = f"file-{seq}"
        tags = sorted(self._rng.sample(TAGS, self._rng.randint(0, 2)))
        response = node.client.post(
            "/files",
            files={"file": (name, content, "application/octet-stream")},
            data={"name": name, "tags": ",".join(tags)},
        )
        content_hash = hashlib.sha256(content).hexdigest()
        expected = 201 if self._quorum_reachable(node.node_id, content_hash) else 503
        self._log(seq, node, f"upload {name} tags={tags} {content_hash[:8]}", response)
        self._expect(response, expected)
        if response.status_code != 201:
            return
        body = response.json()
        assert body["content_hash"] == content_hash
        truth = FileTruth(
            index=len(self._history.files),
            file_id=uuid.UUID(body["file_id"]),
            names={name},
            hashes={content_hash},
            tag_events=[(seq, "add", tag) for tag in tags],
        )
        self._history.files.append(truth)

    def patch_content(self, node: ChaosNode, target: FileTruth) -> None:
        seq = self._history.next_seq()
        content = self._new_content(seq)
        content_hash = hashlib.sha256(content).hexdigest()
        knows = self._knows_live(node, target)
        response = node.client.patch(
            f"/files/{target.file_id}",
            files={"file": ("x", content, "application/octet-stream")},
        )
        if not knows:
            expected = 404
        elif self._quorum_reachable(node.node_id, content_hash):
            expected = 200
        else:
            expected = 503
        self._log(seq, node, f"patch f{target.index} -> {content_hash[:8]}", response)
        self._expect(response, expected)
        if response.status_code == 200:
            assert response.json()["content_hash"] == content_hash
            target.hashes.add(content_hash)

    def rename(self, node: ChaosNode, target: FileTruth) -> None:
        seq = self._history.next_seq()
        name = f"renamed-{seq}"
        expected = 200 if self._knows_live(node, target) else 404
        response = node.client.patch(f"/files/{target.file_id}", data={"name": name})
        self._log(seq, node, f"rename f{target.index} -> {name}", response)
        self._expect(response, expected)
        if response.status_code == 200:
            assert response.json()["name"] == name
            target.names.add(name)

    def add_tags(self, node: ChaosNode, target: FileTruth) -> None:
        seq = self._history.next_seq()
        tags = sorted(self._rng.sample(TAGS, self._rng.randint(1, 2)))
        expected = 200 if self._knows_live(node, target) else 404
        response = node.client.post(
            f"/files/{target.file_id}/tags", json={"tags": tags}
        )
        self._log(seq, node, f"tag+ f{target.index} {tags}", response)
        self._expect(response, expected)
        if response.status_code == 200:
            assert set(tags) <= set(response.json()["tags"]), (
                f"tags just added are not visible: {response.json()['tags']}"
            )
            target.tag_events.extend((seq, "add", tag) for tag in tags)

    def remove_tag(self, node: ChaosNode, target: FileTruth) -> None:
        seq = self._history.next_seq()
        tag = self._rng.choice(TAGS)
        expected = 200 if self._knows_live(node, target) else 404
        complete = self._observed_every_add(node, target, tag)
        response = node.client.delete(f"/files/{target.file_id}/tags/{tag}")
        kind = "remove" if complete else "partial-remove"
        self._log(seq, node, f"tag- f{target.index} {tag} ({kind})", response)
        self._expect(response, expected)
        if response.status_code == 200:
            # Locally, a remove cancels every occurrence the node has seen.
            assert tag not in response.json()["tags"], (
                f"{tag!r} still visible right after its removal"
            )
            target.tag_events.append((seq, kind, tag))

    def delete(self, node: ChaosNode, target: FileTruth) -> None:
        seq = self._history.next_seq()
        expected = 204 if self._knows_live(node, target) else 404
        response = node.client.delete(f"/files/{target.file_id}")
        self._log(seq, node, f"delete f{target.index}", response)
        self._expect(response, expected)
        if response.status_code == 204:
            target.deleted = True

    def download(self, node: ChaosNode, target: FileTruth) -> None:
        seq = self._history.next_seq()
        record = node.store.get(target.file_id)
        if record is None or record.tombstone:
            expected = 404
        elif self._copy_reachable(node, record.content_hash):
            expected = 200
        else:
            expected = 503
        response = node.client.get(f"/files/{target.file_id}")
        self._log(seq, node, f"download f{target.index}", response)
        self._expect(response, expected)
        if response.status_code == 200:
            served = hashlib.sha256(response.content).hexdigest()
            assert served == record.content_hash, (
                f"served {served[:12]}, the record points at {record.content_hash[:12]}"
            )

    # ---- what the receiving node can know ----

    def _knows_live(self, node: ChaosNode, target: FileTruth) -> bool:
        record = node.store.get(target.file_id)
        return record is not None and not record.tombstone

    def _observed_every_add(
        self, node: ChaosNode, target: FileTruth, tag: str
    ) -> bool:
        """Whether `node` holds every add of `tag` that exists anywhere yet.

        If it does, its remove cancels them all and the tag can only come back
        through a later add — so the final state is exact for that tag. If not,
        the unobserved adds survive by design (add wins over a concurrent
        remove) and the remove constrains nothing.
        """
        def adds(records) -> set:
            return {
                occurrence
                for r in records
                if r.file_id == target.file_id
                for occurrence in r.tag_set.adds
                if occurrence[0] == tag
            }

        everywhere = set().union(*(adds(self._cluster.records(n)) for n in NODE_IDS))
        local = node.store.get(target.file_id)
        return local is not None and everywhere <= adds([local])

    def _quorum_reachable(self, node_id: str, content_hash: str) -> bool:
        replicas = replica_set(content_hash, list(NODE_IDS), REPLICATION_FACTOR)
        network = self._cluster.network
        return sum(network.can_reach(node_id, r) for r in replicas) >= WRITE_QUORUM

    def _copy_reachable(self, node: ChaosNode, content_hash: str) -> bool:
        if node.blob_store.exists(content_hash):
            return True
        network = self._cluster.network
        return any(
            network.can_reach(node.node_id, r)
            and self._cluster.nodes[r].blob_store.exists(content_hash)
            for r in replica_set(content_hash, list(NODE_IDS), REPLICATION_FACTOR)
            if r != node.node_id
        )

    # ---- helpers ----

    def _new_content(self, seq: int) -> bytes:
        """Fresh bytes, or occasionally the bytes of a live file (dedup).

        Repeated bytes are taken only from a file that is live and was never
        repointed, so its blob is referenced on every node that holds its
        record. Re-uploading a blob some node already considers orphaned is a
        different case — a node cut off from the uploader may collect its copy
        before it hears of the new referrer (the GC docstring's known window,
        which costs a replica, not the file) — and would make the strict "never
        delete a referenced blob" check unsound.
        """
        stable = [f for f in self._history.live_files() if len(f.hashes) == 1]
        if stable and self._rng.random() < self._mix.dedup_probability:
            (content_hash,) = self._rng.choice(stable).hashes
            return self._history.contents[content_hash]
        content = f"seed {self._history.seed} op {seq} ".encode() + self._rng.randbytes(
            self._rng.randint(0, 48)
        )
        self._history.contents[hashlib.sha256(content).hexdigest()] = content
        return content

    def _log(self, seq: int, node: ChaosNode, what: str, response) -> None:
        self._history.record(f"  #{seq} {node.node_id}: {what} -> {response.status_code}")

    @staticmethod
    def _expect(response, expected: int) -> None:
        assert response.status_code == expected, (
            f"expected {expected}, got {response.status_code}: {response.text}"
        )
