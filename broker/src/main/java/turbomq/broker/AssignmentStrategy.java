package turbomq.broker;

import turbomq.common.PartitionId;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Strategy for assigning partitions to consumers within a consumer group.
 */
public interface AssignmentStrategy {

    /**
     * Assign partitions to consumers.
     *
     * @param partitions available partitions to assign
     * @param consumers  list of consumer IDs in the group
     * @return map of consumer ID → assigned partition set
     */
    Map<String, Set<PartitionId>> assign(List<PartitionId> partitions, List<String> consumers);
}
