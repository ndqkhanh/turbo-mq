package turbomq.broker;

import turbomq.common.PartitionId;

import java.util.*;

/**
 * Round-robin assignment: distributes partitions evenly by cycling through consumers.
 * Partition 0 → Consumer 1, Partition 1 → Consumer 2, Partition 2 → Consumer 1, etc.
 */
public final class RoundRobinAssignmentStrategy implements AssignmentStrategy {

    @Override
    public Map<String, Set<PartitionId>> assign(List<PartitionId> partitions, List<String> consumers) {
        Map<String, Set<PartitionId>> result = new LinkedHashMap<>();
        for (String c : consumers) {
            result.put(c, new LinkedHashSet<>());
        }

        if (consumers.isEmpty() || partitions.isEmpty()) {
            return result;
        }

        List<PartitionId> sorted = new ArrayList<>(partitions);
        sorted.sort(Comparator.comparingInt(PartitionId::id));

        List<String> sortedConsumers = new ArrayList<>(consumers);
        Collections.sort(sortedConsumers);

        for (int i = 0; i < sorted.size(); i++) {
            String consumer = sortedConsumers.get(i % sortedConsumers.size());
            result.get(consumer).add(sorted.get(i));
        }

        return result;
    }
}
