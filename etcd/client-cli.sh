#!/usr/bin/env bash

#
# Help menu
#
# etcdctl help

#
# Get active leader
#
# etcdctl get --prefix  /kakafka/leader/

#
# Get all leases
#
# etcdctl lease list

#
# Get all active brokers
#
# etcdctl get --prefix /kakafka/brokers/


#
# Get all topics
#
# etcdctl get --prefix /kakafka/topics/

docker exec -it etcd-kakafka bash
