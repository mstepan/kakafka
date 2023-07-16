#!/usr/bin/env bash

# JVM Heap, stack size and GC options
#JAVA_OPTS="-XX:+UseParallelGC -XX:MaxRAMPercentage=75 -XX:MaxMetaspaceSize=580078K -XX:ReservedCodeCacheSize=240M -Xss1M -Xmx1G -Djava.util.concurrent.ForkJoinPool.common.parallelism=0"

# Enable remote debugging
#REMOTE_DEBUGGER=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

# Print JVM memory details when process exit
#NATIVE_MEMORY_TRACKER="-XX:+UnlockDiagnosticVMOptions -XX:NativeMemoryTracking=summary -XX:+PrintNMTStatistics"

cluster_mode=true

if [ "$cluster_mode" = true ]; then
    # run 5 brokers in background as a cluster
    echo "Starting cluster with 5 nodes"
    java -Dbroker.port=9091 -jar "$(find -E target -regex '.*/kakafka-.*-SNAPSHOT\.jar$')" &
    java -Dbroker.port=9092 -jar "$(find -E target -regex '.*/kakafka-.*-SNAPSHOT\.jar$')" &
    java -Dbroker.port=9093 -jar "$(find -E target -regex '.*/kakafka-.*-SNAPSHOT\.jar$')" &
    java -Dbroker.port=9094 -jar "$(find -E target -regex '.*/kakafka-.*-SNAPSHOT\.jar$')" &
    java -Dbroker.port=9095 -jar "$(find -E target -regex '.*/kakafka-.*-SNAPSHOT\.jar$')" &
else
    # run single broker
    echo "Starting single broker"
    java -Dbroker.port=9091 -jar "$(find -E target -regex '.*/kakafka-.*-SNAPSHOT\.jar$')"
fi




