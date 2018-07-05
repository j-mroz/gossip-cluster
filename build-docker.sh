#!/usr/bin/env sh

dep ensure
go build
docker build . -t jmroz/gossip_cluster:latest

