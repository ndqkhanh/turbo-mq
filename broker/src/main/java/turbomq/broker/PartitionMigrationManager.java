package turbomq.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import turbomq.common.NodeId;
import turbomq.common.PartitionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages zero-downtime partition migrations between brokers.
 *
 * Migration protocol:
 * 1. ADDING_LEARNER — target broker added as a non-voting learner
 * 2. CATCHING_UP    — learner replicates log from leader (or receives snapshot)
 * 3. PROMOTING      — learner promoted to full voter
 * 4. REMOVING_OLD   — source broker removed from replica set
 * 5. COMPLETED      — assignment updated, migration done
 *
 * Each partition can have at most one active migration at a time.
 */
public final class PartitionMigrationManager {

    private static final Logger log = LoggerFactory.getLogger(PartitionMigrationManager.class);

    private final PartitionManager partitionManager;
    private final Map<PartitionId, MigrationPlan> migrations = new ConcurrentHashMap<>();

    public PartitionMigrationManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    /**
     * Create a new migration plan for a partition.
     *
     * @throws IllegalArgumentException if source == target
     * @throws IllegalStateException    if partition already has an active migration
     */
    public MigrationPlan createMigration(PartitionId partitionId, String topic,
                                         NodeId sourceBroker, NodeId targetBroker) {
        if (sourceBroker.equals(targetBroker)) {
            throw new IllegalArgumentException("Source and target broker cannot be the same");
        }

        MigrationPlan existing = migrations.get(partitionId);
        if (existing != null && existing.state() != MigrationState.COMPLETED
                && existing.state() != MigrationState.FAILED) {
            throw new IllegalStateException(
                    "Partition " + partitionId + " already has an active migration");
        }

        MigrationPlan plan = new MigrationPlan(partitionId, topic, sourceBroker, targetBroker);
        migrations.put(partitionId, plan);
        log.info("Created migration plan: partition={} {} → {}", partitionId, sourceBroker, targetBroker);
        return plan;
    }

    /**
     * Advance the migration for a partition to the next state.
     * When reaching COMPLETED, updates the partition assignment.
     */
    public void advanceMigration(PartitionId partitionId) {
        MigrationPlan plan = migrations.get(partitionId);
        if (plan == null) {
            throw new IllegalArgumentException("No migration for partition " + partitionId);
        }

        MigrationState next = switch (plan.state()) {
            case ADDING_LEARNER -> MigrationState.CATCHING_UP;
            case CATCHING_UP -> MigrationState.PROMOTING;
            case PROMOTING -> MigrationState.REMOVING_OLD;
            case REMOVING_OLD -> {
                // On completion, update the partition assignment
                partitionManager.updateAssignment(
                        plan.topic(), plan.partitionId(), List.of(plan.targetBroker()));
                yield MigrationState.COMPLETED;
            }
            case COMPLETED, FAILED -> plan.state(); // no-op
        };

        plan.advanceTo(next);
        log.info("Migration advanced: partition={} → {}", partitionId, next);
    }

    /**
     * Mark a migration as failed.
     */
    public void failMigration(PartitionId partitionId, String reason) {
        MigrationPlan plan = migrations.get(partitionId);
        if (plan == null) {
            throw new IllegalArgumentException("No migration for partition " + partitionId);
        }
        plan.advanceTo(MigrationState.FAILED);
        log.warn("Migration failed: partition={} reason={}", partitionId, reason);
    }

    /**
     * Get the migration plan for a partition, if any.
     */
    public MigrationPlan getMigration(PartitionId partitionId) {
        return migrations.get(partitionId);
    }

    /**
     * List all active (non-completed, non-failed) migrations.
     */
    public List<MigrationPlan> listActiveMigrations() {
        return migrations.values().stream()
                .filter(p -> p.state() != MigrationState.COMPLETED
                        && p.state() != MigrationState.FAILED)
                .collect(Collectors.toList());
    }

    /**
     * List all migrations (active + completed + failed).
     */
    public List<MigrationPlan> listAllMigrations() {
        return List.copyOf(migrations.values());
    }

    /**
     * Generate migration plans to balance partitions for a topic
     * across all registered brokers. Compares current assignment
     * with ideal distribution and creates plans for partitions
     * that need to move.
     *
     * @return list of newly created migration plans
     */
    public List<MigrationPlan> generateMigrationPlans(String topic) {
        List<PartitionAssignment> assignments = partitionManager.getAssignments(topic);
        Set<NodeId> brokers = partitionManager.getRegisteredBrokers();

        if (assignments.isEmpty() || brokers.isEmpty()) return List.of();

        List<NodeId> sortedBrokers = new ArrayList<>(brokers);
        Collections.sort(sortedBrokers);

        int totalPartitions = assignments.size();
        int brokerCount = sortedBrokers.size();
        int baseCount = totalPartitions / brokerCount;
        int extra = totalPartitions % brokerCount;

        // Target counts per broker
        Map<NodeId, Integer> targetCounts = new LinkedHashMap<>();
        for (int i = 0; i < sortedBrokers.size(); i++) {
            targetCounts.put(sortedBrokers.get(i), baseCount + (i < extra ? 1 : 0));
        }

        // Current counts per broker
        Map<NodeId, Integer> currentCounts = new LinkedHashMap<>();
        for (NodeId b : sortedBrokers) currentCounts.put(b, 0);
        for (PartitionAssignment a : assignments) {
            NodeId leader = a.preferredLeader();
            currentCounts.merge(leader, 1, Integer::sum);
        }

        // Find brokers that need partitions (underloaded)
        List<NodeId> underloaded = new ArrayList<>();
        for (NodeId broker : sortedBrokers) {
            int deficit = targetCounts.getOrDefault(broker, 0) - currentCounts.getOrDefault(broker, 0);
            for (int i = 0; i < deficit; i++) {
                underloaded.add(broker);
            }
        }

        if (underloaded.isEmpty()) return List.of();

        // Find partitions from overloaded brokers to move
        List<MigrationPlan> plans = new ArrayList<>();
        Iterator<NodeId> targetIt = underloaded.iterator();

        for (NodeId broker : sortedBrokers) {
            int surplus = currentCounts.getOrDefault(broker, 0) - targetCounts.getOrDefault(broker, 0);
            if (surplus <= 0) continue;

            // Find partitions assigned to this overloaded broker
            for (PartitionAssignment a : assignments) {
                if (surplus <= 0 || !targetIt.hasNext()) break;
                if (a.preferredLeader().equals(broker)) {
                    NodeId target = targetIt.next();
                    plans.add(createMigration(a.partitionId(), topic, broker, target));
                    surplus--;
                }
            }
        }

        return plans;
    }
}
