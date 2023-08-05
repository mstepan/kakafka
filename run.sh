#!/usr/bin/env bash

# JVM Heap, stack size and GC options
#JAVA_OPTS="-XX:+UseParallelGC -XX:MaxRAMPercentage=75 -XX:MaxMetaspaceSize=580078K -XX:ReservedCodeCacheSize=240M -Xss1M -Xmx1G -Djava.util.concurrent.ForkJoinPool.common.parallelism=0"

# Enable remote debugging
REMOTE_DEBUGGER=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

# Print JVM memory details when process exit
#NATIVE_MEMORY_TRACKER="-XX:+UnlockDiagnosticVMOptions -XX:NativeMemoryTracking=summary -XX:+PrintNMTStatistics"

cluster_mode=false
NODES_CNT=3

echo "Removing all files from data folder './data'"
rm -rf data/*

if [ "$cluster_mode" = true ]; then
    #
    # run $NODES_CNT brokers in background as a cluster
    #
    echo "Starting cluster with $NODES_CNT nodes"

    for i in $(eval echo "{1..$NODES_CNT}")
    do
        port=$((9090 + $i))
        java  -Dbroker.port=$port  -jar "$(find -E target -regex '.*/kakafka-.*-SNAPSHOT\.jar$')" &
    done
else
    # run single broker
    echo "Starting single broker"
    java ${REMOTE_DEBUGGER} -Dbroker.port=9091 -jar "$(find -E target -regex '.*/kakafka-.*-SNAPSHOT\.jar$')"
fi




