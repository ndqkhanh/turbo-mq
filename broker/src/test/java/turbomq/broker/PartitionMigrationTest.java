package turbomq.broker;

import org.junit.jupiter.api.*;
import turbomq.common.NodeId;
import turbomq.common.PartitionId;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD tests for PartitionMigrationManager — zero-downtime partition migration.
 *
 * Tests cover:
 * - Creating migration plans (source → target)
 * - Migration state machine transitions (ADDING_LEARNER → CATCHING_UP → PROMOTING → REMOVING_OLD → COMPLETED)
 * - Updating partition assignments after migration
 * - Multiple concurrent migrations
 * - Rejecting invalid migrations (same source/target, unknown partition)
 * - Migration failure handling
 * - Listing active/completed migrations
 */
class PartitionMigrationTest {

    private PartitionManager partitionManager;
    private PartitionMigrationManager migrationManager;

    @BeforeEach
    void setUp() {
        partitionManager = new PartitionManager();
        migrationManager = new PartitionMigrationManager(partitionManager);

        // Register 3 brokers and create a topic
        partitionManager.registerBroker(NodeId.of(1));
        partitionManager.registerBroker(NodeId.of(2));
        partitionManager.registerBroker(NodeId.of(3));
        partitionManager.createTopic(new TopicConfig("orders", 6, 1));
    }

    // ========== Migration Plan Creation ==========

    @Test
    @DisplayName("createMigration creates a plan in ADDING_LEARNER state")
    void createMigrationPlanInAddingLearnerState() {
        MigrationPlan plan = migrationManager.createMigration(
                PartitionId.of(0), "orders", NodeId.of(1), NodeId.of(3));

        assertThat(plan.partitionId()).isEqualTo(PartitionId.of(0));
        assertThat(plan.topic()).isEqualTo("orders");
        assertThat(plan.sourceBroker()).isEqualTo(NodeId.of(1));
        assertThat(plan.targetBroker()).isEqualTo(NodeId.of(3));
        assertThat(plan.state()).isEqualTo(MigrationState.ADDING_LEARNER);
    }

    @Test
    @DisplayName("createMigration rejects same source and target")
    void rejectsSameSourceAndTarget() {
        assertThatThrownBy(() -> migrationManager.createMigration(
                PartitionId.of(0), "orders", NodeId.of(1), NodeId.of(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same");
    }

    @Test
    @DisplayName("createMigration rejects duplicate migration for same partition")
    void rejectsDuplicateMigration() {
        migrationManager.createMigration(PartitionId.of(0), "orders", NodeId.of(1), NodeId.of(3));

        assertThatThrownBy(() -> migrationManager.createMigration(
                PartitionId.of(0), "orders", NodeId.of(1), NodeId.of(2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already");
    }

    // ========== State Machine Transitions ==========

    @Test
    @DisplayName("advanceMigration transitions through all states to COMPLETED")
    void advanceMigrationThroughAllStates() {
        MigrationPlan plan = migrationManager.createMigration(
                PartitionId.of(0), "orders", NodeId.of(1), NodeId.of(3));

        assertThat(plan.state()).isEqualTo(MigrationState.ADDING_LEARNER);

        migrationManager.advanceMigration(PartitionId.of(0));
        assertThat(plan.state()).isEqualTo(MigrationState.CATCHING_UP);

        migrationManager.advanceMigration(PartitionId.of(0));
        assertThat(plan.state()).isEqualTo(MigrationState.PROMOTING);

        migrationManager.advanceMigration(PartitionId.of(0));
        assertThat(plan.state()).isEqualTo(MigrationState.REMOVING_OLD);

        migrationManager.advanceMigration(PartitionId.of(0));
        assertThat(plan.state()).isEqualTo(MigrationState.COMPLETED);
    }

    @Test
    @DisplayName("completed migration updates partition assignment")
    void completedMigrationUpdatesAssignment() {
        // Partition 0 initially assigned to broker 1
        List<PartitionAssignment> before = partitionManager.getAssignments("orders");
        NodeId originalLeader = before.get(0).preferredLeader();

        migrationManager.createMigration(
                PartitionId.of(0), "orders", originalLeader, NodeId.of(3));

        // Advance to COMPLETED
        migrationManager.advanceMigration(PartitionId.of(0)); // → CATCHING_UP
        migrationManager.advanceMigration(PartitionId.of(0)); // → PROMOTING
        migrationManager.advanceMigration(PartitionId.of(0)); // → REMOVING_OLD
        migrationManager.advanceMigration(PartitionId.of(0)); // → COMPLETED

        // Verify assignment updated
        List<PartitionAssignment> after = partitionManager.getAssignments("orders");
        assertThat(after.get(0).preferredLeader()).isEqualTo(NodeId.of(3));
    }

    @Test
    @DisplayName("advance past COMPLETED is a no-op")
    void advancePastCompletedIsNoop() {
        MigrationPlan plan = migrationManager.createMigration(
                PartitionId.of(0), "orders", NodeId.of(1), NodeId.of(3));

        // Advance to COMPLETED
        for (int i = 0; i < 4; i++) {
            migrationManager.advanceMigration(PartitionId.of(0));
        }
        assertThat(plan.state()).isEqualTo(MigrationState.COMPLETED);

        // Further advance is no-op
        migrationManager.advanceMigration(PartitionId.of(0));
        assertThat(plan.state()).isEqualTo(MigrationState.COMPLETED);
    }

    // ========== Failure Handling ==========

    @Test
    @DisplayName("failMigration sets state to FAILED")
    void failMigrationSetsStateFailed() {
        MigrationPlan plan = migrationManager.createMigration(
                PartitionId.of(0), "orders", NodeId.of(1), NodeId.of(3));
        migrationManager.advanceMigration(PartitionId.of(0)); // → CATCHING_UP

        migrationManager.failMigration(PartitionId.of(0), "Replication timeout");

        assertThat(plan.state()).isEqualTo(MigrationState.FAILED);
    }

    @Test
    @DisplayName("failed migration does not update partition assignment")
    void failedMigrationDoesNotUpdateAssignment() {
        List<PartitionAssignment> before = partitionManager.getAssignments("orders");
        NodeId originalLeader = before.get(0).preferredLeader();

        migrationManager.createMigration(
                PartitionId.of(0), "orders", originalLeader, NodeId.of(3));
        migrationManager.advanceMigration(PartitionId.of(0)); // → CATCHING_UP
        migrationManager.failMigration(PartitionId.of(0), "Timeout");

        List<PartitionAssignment> after = partitionManager.getAssignments("orders");
        assertThat(after.get(0).preferredLeader()).isEqualTo(originalLeader);
    }

    // ========== Listing ==========

    @Test
    @DisplayName("listActiveMigrations returns only in-progress migrations")
    void listActiveMigrationsReturnsOnlyInProgress() {
        migrationManager.createMigration(PartitionId.of(0), "orders", NodeId.of(1), NodeId.of(3));
        migrationManager.createMigration(PartitionId.of(1), "orders", NodeId.of(1), NodeId.of(2));

        // Complete first migration
        for (int i = 0; i < 4; i++) {
            migrationManager.advanceMigration(PartitionId.of(0));
        }

        List<MigrationPlan> active = migrationManager.listActiveMigrations();
        assertThat(active).hasSize(1);
        assertThat(active.getFirst().partitionId()).isEqualTo(PartitionId.of(1));
    }

    @Test
    @DisplayName("listAllMigrations returns all including completed")
    void listAllMigrationsReturnsAll() {
        migrationManager.createMigration(PartitionId.of(0), "orders", NodeId.of(1), NodeId.of(3));
        migrationManager.createMigration(PartitionId.of(1), "orders", NodeId.of(1), NodeId.of(2));

        // Complete first migration
        for (int i = 0; i < 4; i++) {
            migrationManager.advanceMigration(PartitionId.of(0));
        }

        assertThat(migrationManager.listAllMigrations()).hasSize(2);
    }

    // ========== Concurrent Migrations ==========

    @Test
    @DisplayName("multiple partitions can migrate concurrently")
    void multiplePartitionsMigrateConcurrently() {
        migrationManager.createMigration(PartitionId.of(0), "orders", NodeId.of(1), NodeId.of(3));
        migrationManager.createMigration(PartitionId.of(2), "orders", NodeId.of(1), NodeId.of(2));

        // Advance both independently
        migrationManager.advanceMigration(PartitionId.of(0)); // → CATCHING_UP
        migrationManager.advanceMigration(PartitionId.of(2)); // → CATCHING_UP
        migrationManager.advanceMigration(PartitionId.of(0)); // → PROMOTING

        MigrationPlan plan0 = migrationManager.getMigration(PartitionId.of(0));
        MigrationPlan plan2 = migrationManager.getMigration(PartitionId.of(2));

        assertThat(plan0.state()).isEqualTo(MigrationState.PROMOTING);
        assertThat(plan2.state()).isEqualTo(MigrationState.CATCHING_UP);
    }

    // ========== New broker triggers migration plan generation ==========

    @Test
    @DisplayName("generateMigrationPlan produces plans to balance partitions onto new broker")
    void generateMigrationPlanForNewBroker() {
        // Add broker 4 and ask for migration plans
        partitionManager.registerBroker(NodeId.of(4));
        List<MigrationPlan> plans = migrationManager.generateMigrationPlans("orders");

        // Some partitions should move to broker 4
        assertThat(plans).isNotEmpty();
        assertThat(plans).allSatisfy(plan -> {
            assertThat(plan.targetBroker()).isEqualTo(NodeId.of(4));
            assertThat(plan.state()).isEqualTo(MigrationState.ADDING_LEARNER);
        });
    }
}
