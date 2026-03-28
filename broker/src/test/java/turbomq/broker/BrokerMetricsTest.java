package turbomq.broker;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import turbomq.common.NodeId;
import turbomq.common.PartitionId;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD tests for broker-level Prometheus metrics via Micrometer.
 *
 * Tests cover:
 * - Message produce/consume counters
 * - Active partition gauge
 * - Consumer group gauges
 * - Raft leader election counter
 * - Snapshot creation counter
 * - Migration tracking
 * - Idempotency duplicate counter
 */
class BrokerMetricsTest {

    private MeterRegistry registry;
    private BrokerMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new BrokerMetrics(registry);
    }

    // ========== Message Counters ==========

    @Test
    @DisplayName("recordMessageProduced increments produce counter")
    void recordMessageProducedIncrementsCounter() {
        metrics.recordMessageProduced("orders", PartitionId.of(0));
        metrics.recordMessageProduced("orders", PartitionId.of(0));
        metrics.recordMessageProduced("orders", PartitionId.of(1));

        double count = registry.counter("turbomq.messages.produced",
                "topic", "orders", "partition", "Partition-0").count();
        assertThat(count).isEqualTo(2.0);

        double count1 = registry.counter("turbomq.messages.produced",
                "topic", "orders", "partition", "Partition-1").count();
        assertThat(count1).isEqualTo(1.0);
    }

    @Test
    @DisplayName("recordMessageConsumed increments consume counter")
    void recordMessageConsumedIncrementsCounter() {
        metrics.recordMessageConsumed("orders", "group-1");
        metrics.recordMessageConsumed("orders", "group-1");

        double count = registry.counter("turbomq.messages.consumed",
                "topic", "orders", "group", "group-1").count();
        assertThat(count).isEqualTo(2.0);
    }

    // ========== Partition Gauge ==========

    @Test
    @DisplayName("activePartitions gauge reflects current count")
    void activePartitionsGauge() {
        metrics.setActivePartitions(5);

        double gauge = registry.get("turbomq.partitions.active").gauge().value();
        assertThat(gauge).isEqualTo(5.0);

        metrics.setActivePartitions(3);
        gauge = registry.get("turbomq.partitions.active").gauge().value();
        assertThat(gauge).isEqualTo(3.0);
    }

    // ========== Consumer Group Gauge ==========

    @Test
    @DisplayName("activeConsumerGroups gauge reflects current count")
    void activeConsumerGroupsGauge() {
        metrics.setActiveConsumerGroups(2);

        double gauge = registry.get("turbomq.consumer.groups.active").gauge().value();
        assertThat(gauge).isEqualTo(2.0);
    }

    // ========== Raft Metrics ==========

    @Test
    @DisplayName("recordLeaderElection increments election counter")
    void recordLeaderElectionIncrementsCounter() {
        metrics.recordLeaderElection(NodeId.of(1));
        metrics.recordLeaderElection(NodeId.of(1));

        double count = registry.counter("turbomq.raft.elections",
                "node", "Node-1").count();
        assertThat(count).isEqualTo(2.0);
    }

    // ========== Snapshot Metrics ==========

    @Test
    @DisplayName("recordSnapshotCreated increments snapshot counter")
    void recordSnapshotCreatedIncrementsCounter() {
        metrics.recordSnapshotCreated();
        metrics.recordSnapshotCreated();

        double count = registry.counter("turbomq.snapshots.created").count();
        assertThat(count).isEqualTo(2.0);
    }

    // ========== Migration Metrics ==========

    @Test
    @DisplayName("recordMigrationStarted and completed increment counters")
    void migrationCounters() {
        metrics.recordMigrationStarted();
        metrics.recordMigrationStarted();
        metrics.recordMigrationCompleted();

        double started = registry.counter("turbomq.migrations.started").count();
        double completed = registry.counter("turbomq.migrations.completed").count();
        assertThat(started).isEqualTo(2.0);
        assertThat(completed).isEqualTo(1.0);
    }

    // ========== Idempotency Metrics ==========

    @Test
    @DisplayName("recordDuplicateRejected increments duplicate counter")
    void duplicateRejectedCounter() {
        metrics.recordDuplicateRejected();
        metrics.recordDuplicateRejected();
        metrics.recordDuplicateRejected();

        double count = registry.counter("turbomq.idempotency.duplicates").count();
        assertThat(count).isEqualTo(3.0);
    }

    // ========== Active Producers Gauge ==========

    @Test
    @DisplayName("activeProducers gauge reflects current count")
    void activeProducersGauge() {
        metrics.setActiveProducers(10);

        double gauge = registry.get("turbomq.producers.active").gauge().value();
        assertThat(gauge).isEqualTo(10.0);
    }
}
