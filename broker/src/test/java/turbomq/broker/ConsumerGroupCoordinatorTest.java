package turbomq.broker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import turbomq.common.PartitionId;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * ConsumerGroupCoordinator tests: join/leave, rebalancing,
 * heartbeats, session timeout, and assignment strategies.
 */
class ConsumerGroupCoordinatorTest {

    private ConsumerGroupCoordinator coordinator;
    private final List<PartitionId> partitions = List.of(
            PartitionId.of(0), PartitionId.of(1),
            PartitionId.of(2), PartitionId.of(3));

    @BeforeEach
    void setUp() {
        coordinator = new ConsumerGroupCoordinator(
                new RangeAssignmentStrategy(),
                Duration.ofSeconds(30) // session timeout
        );
    }

    // --- Join group ---

    @Test
    void singleConsumerGetsAllPartitions() {
        Map<String, Set<PartitionId>> assignment =
                coordinator.joinGroup("group1", "consumer-1", partitions);

        assertThat(assignment.get("consumer-1")).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    void twoConsumersSplitPartitions() {
        coordinator.joinGroup("group1", "consumer-1", partitions);
        Map<String, Set<PartitionId>> assignment =
                coordinator.joinGroup("group1", "consumer-2", partitions);

        Set<PartitionId> c1 = assignment.get("consumer-1");
        Set<PartitionId> c2 = assignment.get("consumer-2");

        assertThat(c1).hasSize(2);
        assertThat(c2).hasSize(2);
        // No overlap
        assertThat(c1).doesNotContainAnyElementsOf(c2);
        // All partitions covered
        assertThat(c1).containsAnyElementsOf(partitions);
        assertThat(c2).containsAnyElementsOf(partitions);
    }

    @Test
    void threeConsumersBalanced() {
        coordinator.joinGroup("group1", "consumer-1", partitions);
        coordinator.joinGroup("group1", "consumer-2", partitions);
        Map<String, Set<PartitionId>> assignment =
                coordinator.joinGroup("group1", "consumer-3", partitions);

        // 4 partitions / 3 consumers: one gets 2, two get 1
        int total = assignment.values().stream().mapToInt(Set::size).sum();
        assertThat(total).isEqualTo(4);

        int maxSize = assignment.values().stream().mapToInt(Set::size).max().orElse(0);
        int minSize = assignment.values().stream().mapToInt(Set::size).min().orElse(0);
        assertThat(maxSize - minSize).isLessThanOrEqualTo(1);
    }

    @Test
    void duplicateJoinIsIdempotent() {
        coordinator.joinGroup("group1", "consumer-1", partitions);
        Map<String, Set<PartitionId>> assignment =
                coordinator.joinGroup("group1", "consumer-1", partitions);

        assertThat(assignment).hasSize(1);
        assertThat(assignment.get("consumer-1")).hasSize(4);
    }

    // --- Leave group ---

    @Test
    void leaveGroupTriggersRebalance() {
        coordinator.joinGroup("group1", "consumer-1", partitions);
        coordinator.joinGroup("group1", "consumer-2", partitions);

        Map<String, Set<PartitionId>> assignment =
                coordinator.leaveGroup("group1", "consumer-2", partitions);

        // consumer-1 should now have all partitions
        assertThat(assignment.get("consumer-1")).containsExactlyInAnyOrderElementsOf(partitions);
        assertThat(assignment).doesNotContainKey("consumer-2");
    }

    @Test
    void leaveLastConsumerEmptiesGroup() {
        coordinator.joinGroup("group1", "consumer-1", partitions);
        Map<String, Set<PartitionId>> assignment =
                coordinator.leaveGroup("group1", "consumer-1", partitions);

        assertThat(assignment).isEmpty();
    }

    @Test
    void leaveNonexistentConsumerIsNoOp() {
        coordinator.joinGroup("group1", "consumer-1", partitions);
        Map<String, Set<PartitionId>> assignment =
                coordinator.leaveGroup("group1", "consumer-99", partitions);

        assertThat(assignment).hasSize(1);
        assertThat(assignment.get("consumer-1")).hasSize(4);
    }

    // --- Heartbeat and session timeout ---

    @Test
    void heartbeatKeepsConsumerAlive() {
        coordinator.joinGroup("group1", "consumer-1", partitions);
        coordinator.heartbeat("group1", "consumer-1");

        assertThat(coordinator.getMembers("group1")).contains("consumer-1");
    }

    @Test
    void expiredConsumerIsRemoved() {
        coordinator.joinGroup("group1", "consumer-1", partitions);
        coordinator.joinGroup("group1", "consumer-2", partitions);

        // Simulate time beyond session timeout for consumer-2
        Instant future = Instant.now().plus(Duration.ofSeconds(60));
        // Only heartbeat for consumer-1 at a recent time (within session timeout of future)
        coordinator.heartbeat("group1", "consumer-1", future.minus(Duration.ofSeconds(10)));

        Map<String, Set<PartitionId>> assignment =
                coordinator.expireConsumers("group1", partitions, future);

        assertThat(assignment).hasSize(1);
        assertThat(assignment).containsKey("consumer-1");
        assertThat(assignment.get("consumer-1")).hasSize(4);
    }

    @Test
    void heartbeatPreventsExpiry() {
        coordinator.joinGroup("group1", "consumer-1", partitions);

        Instant checkTime = Instant.now().plus(Duration.ofSeconds(60));
        // Heartbeat at a time within session timeout of checkTime
        coordinator.heartbeat("group1", "consumer-1", checkTime.minus(Duration.ofSeconds(10)));

        Map<String, Set<PartitionId>> assignment =
                coordinator.expireConsumers("group1", partitions, checkTime);

        assertThat(assignment).hasSize(1);
        assertThat(assignment.get("consumer-1")).hasSize(4);
    }

    // --- Multiple groups ---

    @Test
    void groupsAreIndependent() {
        coordinator.joinGroup("groupA", "consumer-1", partitions);
        coordinator.joinGroup("groupB", "consumer-2", partitions);

        assertThat(coordinator.getMembers("groupA")).containsExactly("consumer-1");
        assertThat(coordinator.getMembers("groupB")).containsExactly("consumer-2");
    }

    @Test
    void sameConsumerInDifferentGroups() {
        coordinator.joinGroup("groupA", "consumer-1", partitions);
        coordinator.joinGroup("groupB", "consumer-1", partitions);

        assertThat(coordinator.getMembers("groupA")).contains("consumer-1");
        assertThat(coordinator.getMembers("groupB")).contains("consumer-1");
    }

    // --- Round-robin strategy ---

    @Test
    void roundRobinStrategyDistributesEvenly() {
        ConsumerGroupCoordinator rrCoordinator = new ConsumerGroupCoordinator(
                new RoundRobinAssignmentStrategy(),
                Duration.ofSeconds(30)
        );

        rrCoordinator.joinGroup("group1", "consumer-1", partitions);
        Map<String, Set<PartitionId>> assignment =
                rrCoordinator.joinGroup("group1", "consumer-2", partitions);

        Set<PartitionId> c1 = assignment.get("consumer-1");
        Set<PartitionId> c2 = assignment.get("consumer-2");

        // Round-robin: 0→c1, 1→c2, 2→c1, 3→c2
        assertThat(c1).containsExactlyInAnyOrder(PartitionId.of(0), PartitionId.of(2));
        assertThat(c2).containsExactlyInAnyOrder(PartitionId.of(1), PartitionId.of(3));
    }

    // --- Generation tracking ---

    @Test
    void generationIncrementsOnRebalance() {
        coordinator.joinGroup("group1", "consumer-1", partitions);
        int gen1 = coordinator.getGeneration("group1");

        coordinator.joinGroup("group1", "consumer-2", partitions);
        int gen2 = coordinator.getGeneration("group1");

        assertThat(gen2).isGreaterThan(gen1);
    }

    @Test
    void leaveIncrementsGeneration() {
        coordinator.joinGroup("group1", "consumer-1", partitions);
        coordinator.joinGroup("group1", "consumer-2", partitions);
        int genBefore = coordinator.getGeneration("group1");

        coordinator.leaveGroup("group1", "consumer-2", partitions);
        int genAfter = coordinator.getGeneration("group1");

        assertThat(genAfter).isGreaterThan(genBefore);
    }

    // --- Edge cases ---

    @Test
    void getNonexistentGroupReturnsEmptyMembers() {
        assertThat(coordinator.getMembers("nope")).isEmpty();
    }

    @Test
    void getNonexistentGroupGenerationIsZero() {
        assertThat(coordinator.getGeneration("nope")).isEqualTo(0);
    }
}
