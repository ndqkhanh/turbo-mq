package turbomq.broker;

/**
 * Configuration for a topic: how many partitions and replicas.
 */
public record TopicConfig(
        String name,
        int partitionCount,
        int replicationFactor
) {
}
