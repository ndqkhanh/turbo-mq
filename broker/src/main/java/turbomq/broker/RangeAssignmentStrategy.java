package turbomq.broker;

import turbomq.common.PartitionId;

import java.util.*;

/**
 * Range assignment: divides partitions into contiguous ranges per consumer.
 * Consumer 1 gets [0,1,2], Consumer 2 gets [3,4,5], etc.
 */
public final class RangeAssignmentStrategy implements AssignmentStrategy {

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

        int partitionsPerConsumer = sorted.size() / sortedConsumers.size();
        int extra = sorted.size() % sortedConsumers.size();

        int idx = 0;
        for (int c = 0; c < sortedConsumers.size(); c++) {
            int count = partitionsPerConsumer + (c < extra ? 1 : 0);
            Set<PartitionId> assigned = result.get(sortedConsumers.get(c));
            for (int i = 0; i < count; i++) {
                assigned.add(sorted.get(idx++));
            }
        }

        return result;
    }
}
