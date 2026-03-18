package wood.mike.custompartition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import java.util.List;
import java.util.Map;

/**
 * Custom Partitioner that routes messages based on the hash of the key,
 * UNLESS the key is null, in which case it uses a round-robin approach.
 * This ensures ordering for messages with the same key, and even distribution for others.
 */
public class CustomPartitioner implements Partitioner {

    private static final String DEFAULT_KEY = "default-key";

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (keyBytes == null || key == null) {
            // If the key is null, assign it to a partition based on a hash of the DEFAULT_KEY.
            // This achieves a pseudo round-robin distribution for key-less messages.
            return Utils.toPositive(Utils.murmur2(DEFAULT_KEY.getBytes())) % numPartitions;
        }

        // Use a hash of the key bytes to determine the partition.
        // This ensures the same key always goes to the same partition,
        // preserving ordering for that specific key (e.g., all updates for Product A).
        System.out.println("Partitioning based on key");
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {}
}
