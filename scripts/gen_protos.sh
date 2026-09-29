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
