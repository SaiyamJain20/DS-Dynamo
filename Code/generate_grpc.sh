#!/bin/bash

# Script to generate Python gRPC code from .proto file

echo "Generating Python gRPC code from dht.proto..."

python3 -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    dht.proto

if [ $? -eq 0 ]; then
    echo "✓ Successfully generated dht_pb2.py and dht_pb2_grpc.py"
else
    echo "✗ Failed to generate gRPC code"
    exit 1
fi
