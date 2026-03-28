package turbomq.broker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import turbomq.common.NodeId;
import turbomq.common.PartitionId;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * PartitionManager tests: topic creation, partition assignment,
 * round-robin distribution, and rebalancing on broker join/leave.
 */
class PartitionManagerTest {

    private PartitionManager manager;

    @BeforeEach
    void setUp() {
        manager = new PartitionManager();
    }

    // --- Topic creation ---

    @Test
    void createTopicWithSingleBroker() {
        manager.registerBroker(NodeId.of(1));

        List<PartitionAssignment> assignments = manager.createTopic(
                new TopicConfig("events", 3, 1));

        assertThat(assignments).hasSize(3);
        assertThat(assignments.get(0).topic()).isEqualTo("events");
        assertThat(assignments.get(0).partitionId()).isEqualTo(PartitionId.of(0));
        assertThat(assignments.get(1).partitionId()).isEqualTo(PartitionId.of(1));
        assertThat(assignments.get(2).partitionId()).isEqualTo(PartitionId.of(2));
        // All assigned to the only broker
        for (PartitionAssignment a : assignments) {
            assertThat(a.replicas()).containsExactly(NodeId.of(1));
        }
    }

    @Test
    void createTopicDistributesRoundRobin() {
        manager.registerBroker(NodeId.of(1));
        manager.registerBroker(NodeId.of(2));
        manager.registerBroker(NodeId.of(3));

        List<PartitionAssignment> assignments = manager.createTopic(
                new TopicConfig("orders", 6, 1));

        // Round-robin: 0→1, 1→2, 2→3, 3→1, 4→2, 5→3
        assertThat(assignments.get(0).preferredLeader()).isEqualTo(NodeId.of(1));
        assertThat(assignments.get(1).preferredLeader()).isEqualTo(NodeId.of(2));
        assertThat(assignments.get(2).preferredLeader()).isEqualTo(NodeId.of(3));
        assertThat(assignments.get(3).preferredLeader()).isEqualTo(NodeId.of(1));
        assertThat(assignments.get(4).preferredLeader()).isEqualTo(NodeId.of(2));
        assertThat(assignments.get(5).preferredLeader()).isEqualTo(NodeId.of(3));
    }

    @Test
    void createTopicWithReplicationFactor() {
        manager.registerBroker(NodeId.of(1));
        manager.registerBroker(NodeId.of(2));
        manager.registerBroker(NodeId.of(3));

        List<PartitionAssignment> assignments = manager.createTopic(
                new TopicConfig("replicated", 2, 3));

        for (PartitionAssignment a : assignments) {
            assertThat(a.replicas()).hasSize(3);
            // All 3 brokers should be in replicas (different order per partition)
            assertThat(a.replicas()).containsExactlyInAnyOrder(
                    NodeId.of(1), NodeId.of(2), NodeId.of(3));
        }
    }

    @Test
    void createTopicFailsWithInsufficientBrokers() {
        manager.registerBroker(NodeId.of(1));

        assertThatThrownBy(() -> manager.createTopic(
                new TopicConfig("bad", 2, 3)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("replication factor");
    }

    @Test
    void createTopicFailsWithNoBrokers() {
        assertThatThrownBy(() -> manager.createTopic(
                new TopicConfig("bad", 1, 1)))
                .isInstanceOf(IllegalStateException.class);
    }

    // --- Partition queries ---

    @Test
    void getAssignmentsForTopic() {
        manager.registerBroker(NodeId.of(1));
        manager.createTopic(new TopicConfig("events", 3, 1));

        List<PartitionAssignment> assignments = manager.getAssignments("events");
        assertThat(assignments).hasSize(3);
    }

    @Test
    void getAssignmentsForNonexistentTopicReturnsEmpty() {
        assertThat(manager.getAssignments("nope")).isEmpty();
    }

    @Test
    void getPartitionsForBroker() {
        manager.registerBroker(NodeId.of(1));
        manager.registerBroker(NodeId.of(2));
        manager.createTopic(new TopicConfig("t", 4, 1));

        Set<PartitionId> broker1 = manager.getPartitionsForBroker(NodeId.of(1));
        Set<PartitionId> broker2 = manager.getPartitionsForBroker(NodeId.of(2));

        assertThat(broker1).hasSize(2);
        assertThat(broker2).hasSize(2);
        // No overlap
        assertThat(broker1).doesNotContainAnyElementsOf(broker2);
    }

    // --- Broker registration ---

    @Test
    void registerBrokerIsIdempotent() {
        manager.registerBroker(NodeId.of(1));
        manager.registerBroker(NodeId.of(1));

        assertThat(manager.getRegisteredBrokers()).hasSize(1);
    }

    @Test
    void listRegisteredBrokers() {
        manager.registerBroker(NodeId.of(1));
        manager.registerBroker(NodeId.of(2));
        manager.registerBroker(NodeId.of(3));

        assertThat(manager.getRegisteredBrokers())
                .containsExactlyInAnyOrder(NodeId.of(1), NodeId.of(2), NodeId.of(3));
    }

    // --- Rebalancing ---

    @Test
    void rebalanceAfterBrokerJoin() {
        manager.registerBroker(NodeId.of(1));
        manager.createTopic(new TopicConfig("t", 4, 1));

        // All 4 partitions on broker 1
        assertThat(manager.getPartitionsForBroker(NodeId.of(1))).hasSize(4);

        // New broker joins
        manager.registerBroker(NodeId.of(2));
        Map<NodeId, Set<PartitionId>> moves = manager.rebalance("t");

        // After rebalance: each broker should have ~2 partitions
        Set<PartitionId> b1 = manager.getPartitionsForBroker(NodeId.of(1));
        Set<PartitionId> b2 = manager.getPartitionsForBroker(NodeId.of(2));
        assertThat(b1).hasSize(2);
        assertThat(b2).hasSize(2);

        // Moves should indicate what changed
        assertThat(moves).isNotEmpty();
    }

    @Test
    void rebalanceAfterBrokerLeave() {
        manager.registerBroker(NodeId.of(1));
        manager.registerBroker(NodeId.of(2));
        manager.createTopic(new TopicConfig("t", 4, 1));

        // Broker 2 leaves
        manager.deregisterBroker(NodeId.of(2));
        manager.rebalance("t");

        // All partitions should now be on broker 1
        assertThat(manager.getPartitionsForBroker(NodeId.of(1))).hasSize(4);
        assertThat(manager.getPartitionsForBroker(NodeId.of(2))).isEmpty();
    }

    @Test
    void rebalanceIsBalanced() {
        manager.registerBroker(NodeId.of(1));
        manager.registerBroker(NodeId.of(2));
        manager.registerBroker(NodeId.of(3));
        manager.createTopic(new TopicConfig("t", 9, 1));

        // Should already be balanced (round-robin creation)
        Set<PartitionId> b1 = manager.getPartitionsForBroker(NodeId.of(1));
        Set<PartitionId> b2 = manager.getPartitionsForBroker(NodeId.of(2));
        Set<PartitionId> b3 = manager.getPartitionsForBroker(NodeId.of(3));

        assertThat(b1).hasSize(3);
        assertThat(b2).hasSize(3);
        assertThat(b3).hasSize(3);
    }

    // --- Multiple topics ---

    @Test
    void multipleTopicsAreIndependent() {
        manager.registerBroker(NodeId.of(1));
        manager.registerBroker(NodeId.of(2));

        manager.createTopic(new TopicConfig("topicA", 2, 1));
        manager.createTopic(new TopicConfig("topicB", 3, 1));

        assertThat(manager.getAssignments("topicA")).hasSize(2);
        assertThat(manager.getAssignments("topicB")).hasSize(3);
    }

    @Test
    void duplicateTopicCreationFails() {
        manager.registerBroker(NodeId.of(1));
        manager.createTopic(new TopicConfig("events", 2, 1));

        assertThatThrownBy(() -> manager.createTopic(
                new TopicConfig("events", 3, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already exists");
    }
}
