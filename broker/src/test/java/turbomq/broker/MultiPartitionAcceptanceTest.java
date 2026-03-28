package turbomq.broker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import turbomq.common.NodeId;
import turbomq.common.PartitionId;

import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Acceptance test: multi-partition topic creation, round-robin assignment,
 * consumer group rebalancing, and partition isolation.
 */
class MultiPartitionAcceptanceTest {

    private PartitionManager partitionManager;
    private ConsumerGroupCoordinator groupCoordinator;

    @BeforeEach
    void setUp() {
        partitionManager = new PartitionManager();
        groupCoordinator = new ConsumerGroupCoordinator(
                new RangeAssignmentStrategy(), Duration.ofSeconds(30));
    }

    @Test
    void createTopicAcrossThreeBrokersAndVerifyDistribution() {
        partitionManager.registerBroker(NodeId.of(1));
        partitionManager.registerBroker(NodeId.of(2));
        partitionManager.registerBroker(NodeId.of(3));

        List<PartitionAssignment> assignments = partitionManager.createTopic(
                new TopicConfig("events", 6, 1));

        // 6 partitions across 3 brokers = 2 each
        assertThat(assignments).hasSize(6);
        assertThat(partitionManager.getPartitionsForBroker(NodeId.of(1))).hasSize(2);
        assertThat(partitionManager.getPartitionsForBroker(NodeId.of(2))).hasSize(2);
        assertThat(partitionManager.getPartitionsForBroker(NodeId.of(3))).hasSize(2);
    }

    @Test
    void consumerGroupAssignsPartitionsToConsumers() {
        partitionManager.registerBroker(NodeId.of(1));
        partitionManager.registerBroker(NodeId.of(2));
        partitionManager.createTopic(new TopicConfig("orders", 4, 1));

        List<PartitionId> topicPartitions = partitionManager.getAssignments("orders")
                .stream().map(PartitionAssignment::partitionId).toList();

        // Consumer 1 joins → gets all 4 partitions
        Map<String, Set<PartitionId>> assignment1 =
                groupCoordinator.joinGroup("order-processors", "consumer-1", topicPartitions);
        assertThat(assignment1.get("consumer-1")).hasSize(4);

        // Consumer 2 joins → rebalance splits partitions
        Map<String, Set<PartitionId>> assignment2 =
                groupCoordinator.joinGroup("order-processors", "consumer-2", topicPartitions);
        assertThat(assignment2.get("consumer-1")).hasSize(2);
        assertThat(assignment2.get("consumer-2")).hasSize(2);

        // Verify all partitions covered, no overlap
        Set<PartitionId> all = new HashSet<>();
        all.addAll(assignment2.get("consumer-1"));
        all.addAll(assignment2.get("consumer-2"));
        assertThat(all).containsExactlyInAnyOrderElementsOf(topicPartitions);
    }

    @Test
    void consumerLeaveTriggersRebalanceAndCoversAllPartitions() {
        partitionManager.registerBroker(NodeId.of(1));
        partitionManager.createTopic(new TopicConfig("logs", 4, 1));

        List<PartitionId> topicPartitions = partitionManager.getAssignments("logs")
                .stream().map(PartitionAssignment::partitionId).toList();

        groupCoordinator.joinGroup("log-consumers", "c1", topicPartitions);
        groupCoordinator.joinGroup("log-consumers", "c2", topicPartitions);
        groupCoordinator.joinGroup("log-consumers", "c3", topicPartitions);

        // c2 leaves
        Map<String, Set<PartitionId>> afterLeave =
                groupCoordinator.leaveGroup("log-consumers", "c2", topicPartitions);

        // Only c1 and c3 remain
        assertThat(afterLeave).hasSize(2);
        assertThat(afterLeave).containsOnlyKeys("c1", "c3");

        // All partitions still covered
        Set<PartitionId> covered = new HashSet<>();
        afterLeave.values().forEach(covered::addAll);
        assertThat(covered).containsExactlyInAnyOrderElementsOf(topicPartitions);
    }

    @Test
    void brokerJoinTriggersPartitionRebalance() {
        partitionManager.registerBroker(NodeId.of(1));
        partitionManager.createTopic(new TopicConfig("metrics", 6, 1));

        // All 6 on broker 1
        assertThat(partitionManager.getPartitionsForBroker(NodeId.of(1))).hasSize(6);

        // New broker joins and rebalance
        partitionManager.registerBroker(NodeId.of(2));
        partitionManager.rebalance("metrics");

        // Each broker should have 3
        assertThat(partitionManager.getPartitionsForBroker(NodeId.of(1))).hasSize(3);
        assertThat(partitionManager.getPartitionsForBroker(NodeId.of(2))).hasSize(3);
    }

    @Test
    void brokerLeaveTriggersPartitionReassignment() {
        partitionManager.registerBroker(NodeId.of(1));
        partitionManager.registerBroker(NodeId.of(2));
        partitionManager.registerBroker(NodeId.of(3));
        partitionManager.createTopic(new TopicConfig("alerts", 6, 1));

        // Remove broker 3
        partitionManager.deregisterBroker(NodeId.of(3));
        partitionManager.rebalance("alerts");

        // Broker 3 has no partitions, broker 1 and 2 each have 3
        assertThat(partitionManager.getPartitionsForBroker(NodeId.of(3))).isEmpty();
        assertThat(partitionManager.getPartitionsForBroker(NodeId.of(1))).hasSize(3);
        assertThat(partitionManager.getPartitionsForBroker(NodeId.of(2))).hasSize(3);
    }

    @Test
    void replicatedTopicHasAllReplicasAssigned() {
        partitionManager.registerBroker(NodeId.of(1));
        partitionManager.registerBroker(NodeId.of(2));
        partitionManager.registerBroker(NodeId.of(3));

        List<PartitionAssignment> assignments = partitionManager.createTopic(
                new TopicConfig("critical", 3, 3));

        // Each partition has 3 replicas
        for (PartitionAssignment a : assignments) {
            assertThat(a.replicationFactor()).isEqualTo(3);
            assertThat(a.replicas()).containsExactlyInAnyOrder(
                    NodeId.of(1), NodeId.of(2), NodeId.of(3));
        }
    }

    @Test
    void endToEndTopicCreationAndConsumerGroupAssignment() {
        // Cluster setup: 3 brokers
        partitionManager.registerBroker(NodeId.of(1));
        partitionManager.registerBroker(NodeId.of(2));
        partitionManager.registerBroker(NodeId.of(3));

        // Create topic with 4 partitions
        partitionManager.createTopic(new TopicConfig("events", 4, 1));
        List<PartitionId> topicPartitions = partitionManager.getAssignments("events")
                .stream().map(PartitionAssignment::partitionId).toList();

        // 2 consumer groups consuming the same topic
        Map<String, Set<PartitionId>> groupA =
                groupCoordinator.joinGroup("analytics", "a1", topicPartitions);
        groupCoordinator.joinGroup("analytics", "a2", topicPartitions);
        Map<String, Set<PartitionId>> groupAFinal =
                groupCoordinator.joinGroup("analytics", "a2", topicPartitions);

        Map<String, Set<PartitionId>> groupB =
                groupCoordinator.joinGroup("billing", "b1", topicPartitions);

        // Group A: 2 consumers, each gets 2 partitions
        assertThat(groupAFinal.get("a1")).hasSize(2);
        assertThat(groupAFinal.get("a2")).hasSize(2);

        // Group B: 1 consumer, gets all 4 partitions
        assertThat(groupB.get("b1")).hasSize(4);

        // Groups are independent — different assignments
        assertThat(groupCoordinator.getMembers("analytics")).hasSize(2);
        assertThat(groupCoordinator.getMembers("billing")).hasSize(1);

        // Generation tracking
        assertThat(groupCoordinator.getGeneration("analytics")).isGreaterThan(0);
        assertThat(groupCoordinator.getGeneration("billing")).isGreaterThan(0);
    }
}
