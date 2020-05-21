#!/bin/bash

go build github.com/golang/protobuf/protoc-gen-go
protoc rangedb.proto -I. --go_out=plugins=grpc:./rangedbpb --plugin=./protoc-gen-go
rm ./protoc-gen-go
