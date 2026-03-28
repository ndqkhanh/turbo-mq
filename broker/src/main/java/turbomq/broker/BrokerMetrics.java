package turbomq.broker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import turbomq.common.NodeId;
import turbomq.common.PartitionId;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Centralized metrics instrumentation for the TurboMQ broker.
 * Uses Micrometer for Prometheus-compatible metric emission.
 *
 * Metrics:
 * - turbomq.messages.produced (counter, tags: topic, partition)
 * - turbomq.messages.consumed (counter, tags: topic, group)
 * - turbomq.partitions.active (gauge)
 * - turbomq.consumer.groups.active (gauge)
 * - turbomq.raft.elections (counter, tags: node)
 * - turbomq.snapshots.created (counter)
 * - turbomq.migrations.started (counter)
 * - turbomq.migrations.completed (counter)
 * - turbomq.idempotency.duplicates (counter)
 * - turbomq.producers.active (gauge)
 */
public final class BrokerMetrics {

    private final MeterRegistry registry;

    private final AtomicInteger activePartitions = new AtomicInteger(0);
    private final AtomicInteger activeConsumerGroups = new AtomicInteger(0);
    private final AtomicInteger activeProducers = new AtomicInteger(0);

    public BrokerMetrics(MeterRegistry registry) {
        this.registry = registry;

        // Register gauges backed by AtomicIntegers
        registry.gauge("turbomq.partitions.active", activePartitions);
        registry.gauge("turbomq.consumer.groups.active", activeConsumerGroups);
        registry.gauge("turbomq.producers.active", activeProducers);
    }

    // ========== Message Counters ==========

    public void recordMessageProduced(String topic, PartitionId partition) {
        registry.counter("turbomq.messages.produced",
                "topic", topic, "partition", partition.toString()).increment();
    }

    public void recordMessageConsumed(String topic, String group) {
        registry.counter("turbomq.messages.consumed",
                "topic", topic, "group", group).increment();
    }

    // ========== Partition Gauge ==========

    public void setActivePartitions(int count) {
        activePartitions.set(count);
    }

    // ========== Consumer Group Gauge ==========

    public void setActiveConsumerGroups(int count) {
        activeConsumerGroups.set(count);
    }

    // ========== Raft Metrics ==========

    public void recordLeaderElection(NodeId node) {
        registry.counter("turbomq.raft.elections", "node", node.toString()).increment();
    }

    // ========== Snapshot Metrics ==========

    public void recordSnapshotCreated() {
        registry.counter("turbomq.snapshots.created").increment();
    }

    // ========== Migration Metrics ==========

    public void recordMigrationStarted() {
        registry.counter("turbomq.migrations.started").increment();
    }

    public void recordMigrationCompleted() {
        registry.counter("turbomq.migrations.completed").increment();
    }

    // ========== Idempotency Metrics ==========

    public void recordDuplicateRejected() {
        registry.counter("turbomq.idempotency.duplicates").increment();
    }

    // ========== Producer Gauge ==========

    public void setActiveProducers(int count) {
        activeProducers.set(count);
    }
}
