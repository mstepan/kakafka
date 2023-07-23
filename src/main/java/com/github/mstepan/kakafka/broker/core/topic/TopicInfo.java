package com.github.mstepan.kakafka.broker.core.topic;

import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public record TopicInfo(String topicName, List<TopicPartitionInfo> partitions) {

    public TopicInfo {
        Objects.requireNonNull(partitions, "null 'partitions' detected");
    }

    /**
     * Unmarshall TopicInfo entity from 'etcd' value.
     */
    public static TopicInfo fromBytes(byte[] bytes) {

        try (ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(bytes);
                GZIPInputStream compressedIn = new GZIPInputStream(byteArrayIn);
                DataInputStream in = new DataInputStream(compressedIn)) {

            final String topicName = in.readUTF();

            final int partitionsCount = in.readInt();

            List<TopicPartitionInfo> partitions = new ArrayList<>(partitionsCount);

            for (int parIdx = 0; parIdx < partitionsCount; ++parIdx) {
                String leader = in.readUTF();

                int replicasCount = in.readInt();
                List<String> replicas = new ArrayList<>(replicasCount);
                for (int repId = 0; repId < replicasCount; ++repId) {
                    replicas.add(in.readUTF());
                }

                partitions.add(new TopicPartitionInfo(parIdx, leader, replicas));
            }

            return new TopicInfo(topicName, partitions);

        } catch (IOException ioEx) {
            // We should never have 'IOException' here b/c we are using 'DataOutputStream' that is
            // just in-memory byte[] array value.
            throw new IllegalStateException(ioEx);
        }
    }

    /**
     * Convert TopicInfo to byte[] array. This code will be used to convert TopicInfo into 'etcd'
     * value. Make sense also compress data with GZIP. WARNING: don't know why but
     * 'DeflateOutputStream' corrupts data.
     */
    public byte[] toBytes() {

        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();

        try (GZIPOutputStream compressedOut = new GZIPOutputStream(byteArrayOut);
                DataOutputStream out = new DataOutputStream(compressedOut)) {

            out.writeUTF(topicName);

            out.writeInt(partitions().size());

            Iterator<TopicPartitionInfo> partitionInfoIt = partitions().iterator();
            for (int parIdx = 0;
                    parIdx < partitions().size() && partitionInfoIt.hasNext();
                    ++parIdx) {
                TopicPartitionInfo partitionInfo = partitionInfoIt.next();
                out.writeUTF(partitionInfo.leader());
                out.writeInt(partitionInfo.replicas().size());

                for (String singleReplicaValue : partitionInfo.replicas()) {
                    out.writeUTF(singleReplicaValue);
                }
            }
        } catch (IOException ioEx) {
            // We should never have 'IOException' here b/c we are using 'DataOutputStream' that is
            // just in-memory byte[] array value.
            throw new IllegalStateException(ioEx);
        }

        return byteArrayOut.toByteArray();
    }


    /**
     * Encode TopicInfo for Netty handler.
     */
    public void encode(DataOut out) {

        // | topicName, string |
        out.writeString(topicName);

        // | partitions size, int |
        out.writeInt(partitions.size());

        for (TopicPartitionInfo partitionInfo : partitions) {

            // | partition idx, int |
            out.writeInt(partitionInfo.idx());

            // | partition leader id, string |
            out.writeString(partitionInfo.leader());

            List<String> replicas = partitionInfo.replicas();

            // | replicas count, int |
            out.writeInt(replicas.size());

            for (String singleReplica : replicas) {
                // | single replica id, string |
                out.writeString(singleReplica);
            }
        }
    }

    /**
     * Decode TopicInfo from Netty data.
     */
    public static TopicInfo decode(DataIn in) {

        // | topicName, string |
        String topicName = in.readString();

        // | partitions size, int |
        int partitionsCount = in.readInt();

        List<TopicPartitionInfo> partitions = new ArrayList<>();

        for (int i = 0; i < partitionsCount; ++i) {

            // | partition idx, int |
            int partitionIdx = in.readInt();

            // | partition leader id string |
            String leader = in.readString();

            // | replicas count, int |
            int replicasCount = in.readInt();

            List<String> replicas = new ArrayList<>();

            for (int repId = 0; repId < replicasCount; ++repId) {

                // | single replica id, string |
                String singleReplicaId = in.readString();

                replicas.add(singleReplicaId);
            }
            partitions.add(new TopicPartitionInfo(partitionIdx, leader, replicas));
        }

        return new TopicInfo(topicName, partitions);
    }
}
