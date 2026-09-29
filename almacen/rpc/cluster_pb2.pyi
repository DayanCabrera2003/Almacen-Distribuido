from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class FileRecordMsg(_message.Message):
    __slots__ = ("file_id", "name", "content_hash", "tags", "tombstone", "tombstone_at", "created_at", "updated_at")
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONE_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONE_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    file_id: str
    name: str
    content_hash: str
    tags: _containers.RepeatedScalarFieldContainer[str]
    tombstone: bool
    tombstone_at: str
    created_at: str
    updated_at: str
    def __init__(self, file_id: _Optional[str] = ..., name: _Optional[str] = ..., content_hash: _Optional[str] = ..., tags: _Optional[_Iterable[str]] = ..., tombstone: _Optional[bool] = ..., tombstone_at: _Optional[str] = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ...) -> None: ...

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
