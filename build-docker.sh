#!/usr/bin/env sh

dep ensure
go build
docker build . -t gossip_cluster:latest

