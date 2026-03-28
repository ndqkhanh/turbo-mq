package turbomq.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import turbomq.common.PartitionId;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinates consumer group membership, partition assignment, and rebalancing.
 *
 * Each consumer group tracks:
 * - Member set (consumer IDs)
 * - Last heartbeat per consumer
 * - Current partition assignment
 * - Generation counter (incremented on every rebalance)
 *
 * The coordinator runs a configurable {@link AssignmentStrategy} to distribute
 * partitions across consumers on every join, leave, or session expiry.
 */
public final class ConsumerGroupCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupCoordinator.class);

    private final AssignmentStrategy strategy;
    private final Duration sessionTimeout;
    private final Map<String, GroupState> groups = new ConcurrentHashMap<>();

    public ConsumerGroupCoordinator(AssignmentStrategy strategy, Duration sessionTimeout) {
        this.strategy = strategy;
        this.sessionTimeout = sessionTimeout;
    }

    /**
     * A consumer joins (or re-joins) a group. Triggers rebalance.
     *
     * @return the new partition assignment for all consumers in the group
     */
    public Map<String, Set<PartitionId>> joinGroup(String groupId, String consumerId,
                                                    List<PartitionId> availablePartitions) {
        GroupState state = groups.computeIfAbsent(groupId, k -> new GroupState());
        state.members.add(consumerId);
        state.heartbeats.put(consumerId, Instant.now());
        state.generation++;

        Map<String, Set<PartitionId>> assignment = strategy.assign(
                availablePartitions, new ArrayList<>(state.members));
        state.currentAssignment = assignment;

        log.info("Consumer '{}' joined group '{}' (gen={}), {} members",
                consumerId, groupId, state.generation, state.members.size());
        return Map.copyOf(assignment);
    }

    /**
     * A consumer leaves the group. Triggers rebalance.
     *
     * @return the new partition assignment (without the leaving consumer)
     */
    public Map<String, Set<PartitionId>> leaveGroup(String groupId, String consumerId,
                                                     List<PartitionId> availablePartitions) {
        GroupState state = groups.get(groupId);
        if (state == null) return Map.of();

        boolean removed = state.members.remove(consumerId);
        state.heartbeats.remove(consumerId);

        if (state.members.isEmpty()) {
            groups.remove(groupId);
            return Map.of();
        }

        if (removed) {
            state.generation++;
            Map<String, Set<PartitionId>> assignment = strategy.assign(
                    availablePartitions, new ArrayList<>(state.members));
            state.currentAssignment = assignment;

            log.info("Consumer '{}' left group '{}' (gen={}), {} remaining",
                    consumerId, groupId, state.generation, state.members.size());
            return Map.copyOf(assignment);
        }

        // Consumer wasn't in the group — return current assignment
        return Map.copyOf(state.currentAssignment);
    }

    /**
     * Record a heartbeat for a consumer.
     */
    public void heartbeat(String groupId, String consumerId) {
        heartbeat(groupId, consumerId, Instant.now());
    }

    /**
     * Record a heartbeat for a consumer at a specific time (for testability).
     */
    public void heartbeat(String groupId, String consumerId, Instant now) {
        GroupState state = groups.get(groupId);
        if (state != null && state.members.contains(consumerId)) {
            state.heartbeats.put(consumerId, now);
        }
    }

    /**
     * Expire consumers whose last heartbeat is older than sessionTimeout.
     * Triggers rebalance if any consumers are removed.
     *
     * @param now the current time (externalized for testability)
     * @return the new assignment after expired consumers are removed
     */
    public Map<String, Set<PartitionId>> expireConsumers(String groupId,
                                                          List<PartitionId> availablePartitions,
                                                          Instant now) {
        GroupState state = groups.get(groupId);
        if (state == null) return Map.of();

        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, Instant> e : state.heartbeats.entrySet()) {
            if (e.getValue().plus(sessionTimeout).isBefore(now)) {
                expired.add(e.getKey());
            }
        }

        if (expired.isEmpty()) {
            return Map.copyOf(state.currentAssignment);
        }

        for (String consumerId : expired) {
            state.members.remove(consumerId);
            state.heartbeats.remove(consumerId);
            log.info("Consumer '{}' expired from group '{}' (session timeout)", consumerId, groupId);
        }

        if (state.members.isEmpty()) {
            groups.remove(groupId);
            return Map.of();
        }

        state.generation++;
        Map<String, Set<PartitionId>> assignment = strategy.assign(
                availablePartitions, new ArrayList<>(state.members));
        state.currentAssignment = assignment;

        return Map.copyOf(assignment);
    }

    /**
     * Get current members of a group.
     */
    public Set<String> getMembers(String groupId) {
        GroupState state = groups.get(groupId);
        return state != null ? Set.copyOf(state.members) : Set.of();
    }

    /**
     * Get the current generation (rebalance counter) for a group.
     */
    public int getGeneration(String groupId) {
        GroupState state = groups.get(groupId);
        return state != null ? state.generation : 0;
    }

    private static final class GroupState {
        final Set<String> members = new LinkedHashSet<>();
        final Map<String, Instant> heartbeats = new ConcurrentHashMap<>();
        Map<String, Set<PartitionId>> currentAssignment = Map.of();
        int generation = 0;
    }
}
