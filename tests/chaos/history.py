"""What clients were told: the ground truth the final state is judged against.

Only acknowledged operations are recorded. A request the cluster refused (404,
503) changed nothing a client can rely on, so it constrains nothing.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field


@dataclass
class FileTruth:
    index: int
    file_id: uuid.UUID
    names: set[str]
    # Every content hash a successful upload or PATCH pointed this file at.
    hashes: set[str]
    # (sequence number, kind, tag), in the order acknowledged. `kind` is "add",
    # "remove" for a remove whose node had observed every add of the tag, or
    # "partial-remove" for one that had not (see `expected_tags`).
    tag_events: list[tuple[int, str, str]] = field(default_factory=list)
    deleted: bool = False

    def expected_tags(self) -> tuple[set[str], set[str]]:
        """(tags that must be present, tags that must be absent).

        An OR-Set remove cancels only the adds its node had observed. So a tag
        is certainly present if no remove of any kind followed its last add,
        and certainly absent if its last event is a remove that observed every
        add. After a partial remove either outcome is allowed.
        """
        present: set[str] = set()
        absent: set[str] = set()
        for _, kind, tag in self.tag_events:
            present.discard(tag)
            absent.discard(tag)
            if kind == "add":
                present.add(tag)
            elif kind == "remove":
                absent.add(tag)
        return present, absent

    def ever_added_tags(self) -> set[str]:
        return {tag for _, kind, tag in self.tag_events if kind == "add"}


@dataclass
class History:
    seed: int
    files: list[FileTruth] = field(default_factory=list)
    # Every payload ever sent, acknowledged or not, by its hash.
    contents: dict[str, bytes] = field(default_factory=dict)
    log: list[str] = field(default_factory=list)
    _seq: int = 0

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def record(self, line: str) -> None:
        self.log.append(line)

    def live_files(self) -> list[FileTruth]:
        return [f for f in self.files if not f.deleted]

    def deleted_ids(self) -> set[uuid.UUID]:
        return {f.file_id for f in self.files if f.deleted}

    def schedule(self) -> str:
        return "\n".join(self.log)
