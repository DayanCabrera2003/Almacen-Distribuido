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
