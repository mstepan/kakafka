#!/usr/bin/env bash

# https://hub.docker.com/r/bitnami/etcd

CONTAINER_NAME=etcd-kakafka

docker stop ${CONTAINER_NAME}
docker rm ${CONTAINER_NAME}

docker run \
  --name ${CONTAINER_NAME} \
  --publish 2379:2379 \
  --publish 2380:2380 \
  --env ALLOW_NONE_AUTHENTICATION=yes \
  --env ETCD_ADVERTISE_CLIENT_URLS=http://localhost:2379 \
  -d bitnami/etcd:3.4.27

#docker run -d \
#  -p 4001:4001 -p 2380:2380 -p 2379:2379 \
# --name etcd quay.io/coreos/etcd:v2.3.8 \
# -name etcd0 \
# -advertise-client-urls http://localhost:2379,http://localhost:4001 \
# -listen-client-urls http://0.0.0.0:2379,http://0.0.0.0:4001 \
# -initial-advertise-peer-urls http://localhost:2380 \
# -listen-peer-urls http://0.0.0.0:2380 \
# -initial-cluster-token etcd-cluster-1 \
# -initial-cluster etcd0=http://localhost:2380 \
# -initial-cluster-state new
