from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TagOccurrence(_message.Message):
    __slots__ = ("tag", "node_id", "counter")
    TAG_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    COUNTER_FIELD_NUMBER: _ClassVar[int]
    tag: str
    node_id: str
    counter: int
    def __init__(self, tag: _Optional[str] = ..., node_id: _Optional[str] = ..., counter: _Optional[int] = ...) -> None: ...

class FileRecordMsg(_message.Message):
    __slots__ = ("file_id", "name", "name_ts", "name_node", "content_hash", "content_hash_ts", "content_hash_node", "tombstone", "tombstone_ts", "tombstone_node", "created_at", "updated_at", "vector_clock", "tag_adds", "tag_removes")
    class VectorClockEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    NAME_TS_FIELD_NUMBER: _ClassVar[int]
    NAME_NODE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_TS_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_NODE_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONE_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONE_TS_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONE_NODE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    VECTOR_CLOCK_FIELD_NUMBER: _ClassVar[int]
    TAG_ADDS_FIELD_NUMBER: _ClassVar[int]
    TAG_REMOVES_FIELD_NUMBER: _ClassVar[int]
    file_id: str
    name: str
    name_ts: str
    name_node: str
    content_hash: str
    content_hash_ts: str
    content_hash_node: str
    tombstone: bool
    tombstone_ts: str
    tombstone_node: str
    created_at: str
    updated_at: str
    vector_clock: _containers.ScalarMap[str, int]
    tag_adds: _containers.RepeatedCompositeFieldContainer[TagOccurrence]
    tag_removes: _containers.RepeatedCompositeFieldContainer[TagOccurrence]
    def __init__(self, file_id: _Optional[str] = ..., name: _Optional[str] = ..., name_ts: _Optional[str] = ..., name_node: _Optional[str] = ..., content_hash: _Optional[str] = ..., content_hash_ts: _Optional[str] = ..., content_hash_node: _Optional[str] = ..., tombstone: _Optional[bool] = ..., tombstone_ts: _Optional[str] = ..., tombstone_node: _Optional[str] = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ..., vector_clock: _Optional[_Mapping[str, int]] = ..., tag_adds: _Optional[_Iterable[_Union[TagOccurrence, _Mapping]]] = ..., tag_removes: _Optional[_Iterable[_Union[TagOccurrence, _Mapping]]] = ...) -> None: ...

class ReplicateAck(_message.Message):
    __slots__ = ("applied",)
    APPLIED_FIELD_NUMBER: _ClassVar[int]
    applied: bool
    def __init__(self, applied: _Optional[bool] = ...) -> None: ...

class PingRequest(_message.Message):
    __slots__ = ("from_node_id",)
    FROM_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    from_node_id: str
    def __init__(self, from_node_id: _Optional[str] = ...) -> None: ...

class PingResponse(_message.Message):
    __slots__ = ("node_id",)
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    def __init__(self, node_id: _Optional[str] = ...) -> None: ...

class DigestEntry(_message.Message):
    __slots__ = ("file_id", "vector_clock")
    class VectorClockEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    VECTOR_CLOCK_FIELD_NUMBER: _ClassVar[int]
    file_id: str
    vector_clock: _containers.ScalarMap[str, int]
    def __init__(self, file_id: _Optional[str] = ..., vector_clock: _Optional[_Mapping[str, int]] = ...) -> None: ...

class MemberStatus(_message.Message):
    __slots__ = ("node_id", "state", "incarnation", "since")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    INCARNATION_FIELD_NUMBER: _ClassVar[int]
    SINCE_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    state: int
    incarnation: int
    since: str
    def __init__(self, node_id: _Optional[str] = ..., state: _Optional[int] = ..., incarnation: _Optional[int] = ..., since: _Optional[str] = ...) -> None: ...

class GossipDigest(_message.Message):
    __slots__ = ("from_node_id", "entries", "membership")
    FROM_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    MEMBERSHIP_FIELD_NUMBER: _ClassVar[int]
    from_node_id: str
    entries: _containers.RepeatedCompositeFieldContainer[DigestEntry]
    membership: _containers.RepeatedCompositeFieldContainer[MemberStatus]
    def __init__(self, from_node_id: _Optional[str] = ..., entries: _Optional[_Iterable[_Union[DigestEntry, _Mapping]]] = ..., membership: _Optional[_Iterable[_Union[MemberStatus, _Mapping]]] = ...) -> None: ...

class GossipDelta(_message.Message):
    __slots__ = ("records", "wanted_file_ids", "membership")
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    WANTED_FILE_IDS_FIELD_NUMBER: _ClassVar[int]
    MEMBERSHIP_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[FileRecordMsg]
    wanted_file_ids: _containers.RepeatedScalarFieldContainer[str]
    membership: _containers.RepeatedCompositeFieldContainer[MemberStatus]
    def __init__(self, records: _Optional[_Iterable[_Union[FileRecordMsg, _Mapping]]] = ..., wanted_file_ids: _Optional[_Iterable[str]] = ..., membership: _Optional[_Iterable[_Union[MemberStatus, _Mapping]]] = ...) -> None: ...
