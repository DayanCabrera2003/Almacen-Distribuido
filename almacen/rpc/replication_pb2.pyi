from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class BlobChunk(_message.Message):
    __slots__ = ("content_hash", "data")
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    content_hash: str
    data: bytes
    def __init__(self, content_hash: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class PutBlobAck(_message.Message):
    __slots__ = ("content_hash", "stored")
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    STORED_FIELD_NUMBER: _ClassVar[int]
    content_hash: str
    stored: bool
    def __init__(self, content_hash: _Optional[str] = ..., stored: _Optional[bool] = ...) -> None: ...

class BlobRequest(_message.Message):
    __slots__ = ("content_hash",)
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    content_hash: str
    def __init__(self, content_hash: _Optional[str] = ...) -> None: ...
