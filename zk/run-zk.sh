#!/usr/bin/env bash

# https://hub.docker.com/_/zookeeper
# 2181 - client port
# 2888 - follower port
# 3888 - election port
# 8080 - AdminServer port
#
docker run \
  --name kakafka-zk \
  --rm \
  -p 2181:2181 -p 2888:2888 -p 3888:3888 -p 8080:8080 \
  -d zookeeper:3.7-temurin
