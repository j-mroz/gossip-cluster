#!/usr/bin/env sh

protoc --go_out=plugins=grpc:. gossip.v1.proto
