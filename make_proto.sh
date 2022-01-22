#!/bin/sh -ex

python3 -m grpc_tools.protoc --proto_path=./proto ./proto/nvme_gw.proto --python_out=./nvme_gw --grpc_python_out=./nvme_gw
sed -i -e 's/^import nvme_gw_pb2/import nvme_gw.nvme_gw_pb2/' nvme_gw/nvme_gw_pb2_grpc.py

