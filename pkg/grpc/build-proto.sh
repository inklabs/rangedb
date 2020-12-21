#!/bin/bash

go install google.golang.org/protobuf/cmd/protoc-gen-go \
         google.golang.org/grpc/cmd/protoc-gen-go-grpc
protoc -I=. --go_out=./rangedbpb --go-grpc_out=./rangedbpb ./rangedb.proto
