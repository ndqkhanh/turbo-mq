package turbomq.broker;

import turbomq.common.NodeId;
import turbomq.common.PartitionId;

import java.util.List;

/**
 * Assignment of a partition to a set of broker replicas.
 * The first replica in the list is the preferred leader.
 */
public record PartitionAssignment(
        PartitionId partitionId,
        String topic,
        List<NodeId> replicas
) {

    public NodeId preferredLeader() {
        return replicas.getFirst();
    }

    public int replicationFactor() {
        return replicas.size();
    }
}
