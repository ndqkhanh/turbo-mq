package turbomq.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import turbomq.common.NodeId;
import turbomq.common.PartitionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages partition lifecycle: topic creation, partition-to-broker assignment,
 * and rebalancing when brokers join or leave.
 *
 * Uses round-robin assignment for initial placement and greedy rebalancing
 * to minimize partition movement.
 */
public final class PartitionManager {

    private static final Logger log = LoggerFactory.getLogger(PartitionManager.class);

    private final Set<NodeId> brokers = ConcurrentHashMap.newKeySet();
    private final Map<String, List<PartitionAssignment>> topicAssignments = new ConcurrentHashMap<>();

    /**
     * Register a broker as available for partition assignment.
     */
    public void registerBroker(NodeId brokerId) {
        brokers.add(brokerId);
    }

    /**
     * Deregister a broker (e.g., it has been declared dead by gossip).
     */
    public void deregisterBroker(NodeId brokerId) {
        brokers.remove(brokerId);
    }

    /**
     * Get the set of registered brokers.
     */
    public Set<NodeId> getRegisteredBrokers() {
        return Collections.unmodifiableSet(new HashSet<>(brokers));
    }

    /**
     * Create a topic with the given configuration.
     * Assigns partitions round-robin across registered brokers.
     *
     * @return the list of partition assignments
     * @throws IllegalStateException if not enough brokers for the replication factor
     * @throws IllegalArgumentException if topic already exists
     */
    public List<PartitionAssignment> createTopic(TopicConfig config) {
        if (topicAssignments.containsKey(config.name())) {
            throw new IllegalArgumentException("Topic '" + config.name() + "' already exists");
        }

        List<NodeId> sortedBrokers = new ArrayList<>(brokers);
        Collections.sort(sortedBrokers);

        if (sortedBrokers.isEmpty()) {
            throw new IllegalStateException("No brokers registered");
        }
        if (sortedBrokers.size() < config.replicationFactor()) {
            throw new IllegalStateException(
                    "Not enough brokers for replication factor " + config.replicationFactor()
                            + " (have " + sortedBrokers.size() + ")");
        }

        List<PartitionAssignment> assignments = new ArrayList<>();
        for (int p = 0; p < config.partitionCount(); p++) {
            List<NodeId> replicas = new ArrayList<>();
            for (int r = 0; r < config.replicationFactor(); r++) {
                int brokerIndex = (p + r) % sortedBrokers.size();
                replicas.add(sortedBrokers.get(brokerIndex));
            }
            assignments.add(new PartitionAssignment(
                    PartitionId.of(p), config.name(), List.copyOf(replicas)));
        }

        topicAssignments.put(config.name(), new ArrayList<>(assignments));
        log.info("Created topic '{}' with {} partitions, rf={}",
                config.name(), config.partitionCount(), config.replicationFactor());
        return List.copyOf(assignments);
    }

    /**
     * Get current assignments for a topic.
     */
    public List<PartitionAssignment> getAssignments(String topic) {
        return List.copyOf(topicAssignments.getOrDefault(topic, List.of()));
    }

    /**
     * Get all partition IDs assigned to a specific broker (as preferred leader, rf=1).
     */
    public Set<PartitionId> getPartitionsForBroker(NodeId brokerId) {
        Set<PartitionId> result = new HashSet<>();
        for (List<PartitionAssignment> assignments : topicAssignments.values()) {
            for (PartitionAssignment a : assignments) {
                if (a.replicas().contains(brokerId)) {
                    result.add(a.partitionId());
                }
            }
        }
        return result;
    }

    /**
     * Update the assignment for a single partition within a topic.
     * Used by partition migration to swap a replica after migration completes.
     */
    public void updateAssignment(String topic, PartitionId partitionId, List<NodeId> newReplicas) {
        List<PartitionAssignment> assignments = topicAssignments.get(topic);
        if (assignments == null) {
            throw new IllegalArgumentException("Topic '" + topic + "' does not exist");
        }
        for (int i = 0; i < assignments.size(); i++) {
            if (assignments.get(i).partitionId().equals(partitionId)) {
                assignments.set(i, new PartitionAssignment(partitionId, topic, List.copyOf(newReplicas)));
                return;
            }
        }
        throw new IllegalArgumentException("Partition " + partitionId + " not found in topic '" + topic + "'");
    }

    /**
     * Rebalance partitions for a topic across registered brokers.
     * Uses greedy algorithm: move partitions from overloaded brokers to underloaded ones.
     *
     * @return map of broker → set of partition IDs that were moved TO that broker
     */
    public Map<NodeId, Set<PartitionId>> rebalance(String topic) {
        List<PartitionAssignment> assignments = topicAssignments.get(topic);
        if (assignments == null || assignments.isEmpty()) {
            return Map.of();
        }

        List<NodeId> sortedBrokers = new ArrayList<>(brokers);
        Collections.sort(sortedBrokers);

        if (sortedBrokers.isEmpty()) {
            throw new IllegalStateException("No brokers registered for rebalance");
        }

        int totalPartitions = assignments.size();
        int brokersCount = sortedBrokers.size();
        int baseCount = totalPartitions / brokersCount;
        int extra = totalPartitions % brokersCount;

        // Target: first 'extra' brokers get baseCount+1, rest get baseCount
        Map<NodeId, Integer> targetCounts = new LinkedHashMap<>();
        for (int i = 0; i < sortedBrokers.size(); i++) {
            targetCounts.put(sortedBrokers.get(i), baseCount + (i < extra ? 1 : 0));
        }

        // Current assignment: broker → list of partition indices
        Map<NodeId, List<Integer>> currentAssign = new LinkedHashMap<>();
        for (NodeId b : sortedBrokers) {
            currentAssign.put(b, new ArrayList<>());
        }

        // Collect orphaned partitions (assigned to deregistered brokers)
        List<Integer> orphaned = new ArrayList<>();
        for (int i = 0; i < assignments.size(); i++) {
            NodeId leader = assignments.get(i).preferredLeader();
            if (currentAssign.containsKey(leader)) {
                currentAssign.get(leader).add(i);
            } else {
                orphaned.add(i);
            }
        }

        // Take excess from overloaded brokers
        for (NodeId broker : sortedBrokers) {
            int target = targetCounts.get(broker);
            List<Integer> partitions = currentAssign.get(broker);
            while (partitions.size() > target) {
                orphaned.add(partitions.removeLast());
            }
        }

        // Distribute orphaned to underloaded brokers
        Map<NodeId, Set<PartitionId>> moves = new HashMap<>();
        Iterator<Integer> orphanIt = orphaned.iterator();
        for (NodeId broker : sortedBrokers) {
            int target = targetCounts.get(broker);
            List<Integer> partitions = currentAssign.get(broker);
            while (partitions.size() < target && orphanIt.hasNext()) {
                int idx = orphanIt.next();
                partitions.add(idx);
                moves.computeIfAbsent(broker, k -> new HashSet<>())
                        .add(assignments.get(idx).partitionId());
            }
        }

        // Rebuild assignments
        List<PartitionAssignment> newAssignments = new ArrayList<>(assignments);
        for (NodeId broker : sortedBrokers) {
            for (int idx : currentAssign.get(broker)) {
                PartitionAssignment old = assignments.get(idx);
                newAssignments.set(idx, new PartitionAssignment(
                        old.partitionId(), old.topic(), List.of(broker)));
            }
        }
        topicAssignments.put(topic, newAssignments);

        log.info("Rebalanced topic '{}': {} partition moves", topic,
                moves.values().stream().mapToInt(Set::size).sum());
        return moves;
    }
}
