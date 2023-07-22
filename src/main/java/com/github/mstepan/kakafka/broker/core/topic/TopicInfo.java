package com.github.mstepan.kakafka.broker.core.topic;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.zip.DeflaterOutputStream;

public record TopicInfo(List<TopicPartitionInfo> partitions) {

    public TopicInfo {
        Objects.requireNonNull(partitions, "null 'partitions' detected");
    }

    /**
     * Convert TopicInfo to byte[] array. This code will be used to convert TopicInfo into 'etcd'
     * value. Make sense also compress data with Deflate.
     */
    public byte[] toBytes() {
        try (ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
                DeflaterOutputStream compressedOut = new DeflaterOutputStream(byteArrayOut);
                DataOutputStream out = new DataOutputStream(compressedOut)) {

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

            out.flush();
            return byteArrayOut.toByteArray();

        } catch (IOException ioEx) {
            // We should never have 'IOException' here b/c we are using 'DataOutputStream' that is
            // just in-memory byte[] array value.
            throw new IllegalStateException(ioEx);
        }
    }
}
