package turbomq.common;

/**
 * Unique identifier for a partition within the cluster.
 */
public record PartitionId(int id) {

    public static PartitionId of(int id) {
        return new PartitionId(id);
    }

    @Override
    public String toString() {
        return "Partition-" + id;
    }
}
