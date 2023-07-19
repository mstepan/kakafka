#!/usr/bin/env bash

# https://hub.docker.com/r/bitnami/etcd

CONTAINER_NAME=etcd-kakafka

echo "Stopping docker '${CONTAINER_NAME}' container if any"
docker stop ${CONTAINER_NAME}

echo "Removing docker '${CONTAINER_NAME}' container if any"
docker rm ${CONTAINER_NAME}

echo "Starting '${CONTAINER_NAME}'"
docker run \
  --name ${CONTAINER_NAME} \
  --publish 2379:2379 \
  --publish 2380:2380 \
  --env ALLOW_NONE_AUTHENTICATION=yes \
  --env ETCD_ADVERTISE_CLIENT_URLS=http://localhost:2379 \
  -d bitnami/etcd:3.4.27
