package com.github.mstepan.kakafka.broker.core.topic;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public record TopicInfo(List<TopicPartitionInfo> partitions) {

    public TopicInfo {
        Objects.requireNonNull(partitions, "null 'partitions' detected");
    }

    /**
     * Convert TopicInfo to byte[] array. This code will be used to convert TopicInfo into 'etcd'
     * value.
     */
    public byte[] toBytes() {

        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteArrayOut);

        try {
            out.writeInt(partitions().size());

            Iterator<TopicPartitionInfo> partitionInfoIt = partitions().iterator();
            for (int parIdx = 0;
                    parIdx < partitions().size() && partitionInfoIt.hasNext();
                    ++parIdx) {
                TopicPartitionInfo partitionInfo = partitionInfoIt.next();
                out.writeBytes(partitionInfo.leader());
                out.writeInt(partitionInfo.replicas().size());

                for (String singleReplicaValue : partitionInfo.replicas()) {
                    out.writeBytes(singleReplicaValue);
                }
            }
        } catch (IOException ioEx) {
            // We should never have 'IOException' here b/c we are using 'DataOutputStream' that is
            // just in-memory byte[] array value.
            throw new IllegalStateException(ioEx);
        }

        return byteArrayOut.toByteArray();
    }
}
