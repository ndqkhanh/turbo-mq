package turbomq.storage;

import turbomq.common.PartitionId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for per-partition storage using RocksDB column families.
 *
 * Validates that each partition gets its own isolated keyspace,
 * message append/read operations, offset tracking, and cleanup.
 */
class PartitionStoreTest {

    @TempDir
    Path tempDir;

    private PartitionStoreManager manager;

    @BeforeEach
    void setUp() {
        manager = new PartitionStoreManager(tempDir);
        manager.open();
    }

    @AfterEach
    void tearDown() {
        manager.close();
    }

    // ========== Partition Lifecycle ==========

    @Test
    void createAndGetPartition() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        assertThat(store).isNotNull();
    }

    @Test
    void getExistingPartitionReturnsSameInstance() {
        PartitionStore store1 = manager.getOrCreatePartition(PartitionId.of(0));
        PartitionStore store2 = manager.getOrCreatePartition(PartitionId.of(0));
        assertThat(store1).isSameAs(store2);
    }

    @Test
    void multiplePartitionsAreIsolated() {
        PartitionStore p0 = manager.getOrCreatePartition(PartitionId.of(0));
        PartitionStore p1 = manager.getOrCreatePartition(PartitionId.of(1));

        p0.append(1, "msg-for-p0".getBytes());
        p1.append(1, "msg-for-p1".getBytes());

        assertThat(p0.read(1)).isEqualTo("msg-for-p0".getBytes());
        assertThat(p1.read(1)).isEqualTo("msg-for-p1".getBytes());
    }

    // ========== Message Operations ==========

    @Test
    void appendAndReadMessage() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        store.append(1, "hello".getBytes());

        assertThat(store.read(1)).isEqualTo("hello".getBytes());
    }

    @Test
    void readNonExistentOffsetReturnsNull() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        assertThat(store.read(999)).isNull();
    }

    @Test
    void appendMultipleMessagesInSequence() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        store.append(1, "msg1".getBytes());
        store.append(2, "msg2".getBytes());
        store.append(3, "msg3".getBytes());

        assertThat(store.read(1)).isEqualTo("msg1".getBytes());
        assertThat(store.read(2)).isEqualTo("msg2".getBytes());
        assertThat(store.read(3)).isEqualTo("msg3".getBytes());
    }

    @Test
    void readRangeReturnsMessagesInOrder() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        store.append(1, "a".getBytes());
        store.append(2, "b".getBytes());
        store.append(3, "c".getBytes());
        store.append(4, "d".getBytes());

        List<byte[]> messages = store.readRange(2, 4); // offsets 2, 3
        assertThat(messages).hasSize(2);
        assertThat(messages.get(0)).isEqualTo("b".getBytes());
        assertThat(messages.get(1)).isEqualTo("c".getBytes());
    }

    @Test
    void readRangeWithNoMessagesReturnsEmpty() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        assertThat(store.readRange(1, 10)).isEmpty();
    }

    // ========== Offset Tracking ==========

    @Test
    void latestOffsetStartsAtZero() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        assertThat(store.latestOffset()).isEqualTo(0);
    }

    @Test
    void latestOffsetAdvancesAfterAppend() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        store.append(1, "msg".getBytes());
        assertThat(store.latestOffset()).isEqualTo(1);

        store.append(2, "msg2".getBytes());
        assertThat(store.latestOffset()).isEqualTo(2);
    }

    // ========== Consumer Offset ==========

    @Test
    void consumerOffsetDefaultsToZero() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        assertThat(store.getConsumerOffset("group-1")).isEqualTo(0);
    }

    @Test
    void commitAndReadConsumerOffset() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        store.commitConsumerOffset("group-1", 5);
        assertThat(store.getConsumerOffset("group-1")).isEqualTo(5);
    }

    @Test
    void multipleConsumerGroupsHaveIndependentOffsets() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        store.commitConsumerOffset("group-A", 3);
        store.commitConsumerOffset("group-B", 7);

        assertThat(store.getConsumerOffset("group-A")).isEqualTo(3);
        assertThat(store.getConsumerOffset("group-B")).isEqualTo(7);
    }

    // ========== Persistence ==========

    @Test
    void partitionDataPersistedAcrossReopen() {
        PartitionStore store = manager.getOrCreatePartition(PartitionId.of(0));
        store.append(1, "persistent".getBytes());
        store.commitConsumerOffset("g1", 1);
        manager.close();

        manager = new PartitionStoreManager(tempDir);
        manager.open();
        PartitionStore reopened = manager.getOrCreatePartition(PartitionId.of(0));

        assertThat(reopened.read(1)).isEqualTo("persistent".getBytes());
        assertThat(reopened.getConsumerOffset("g1")).isEqualTo(1);
    }

    // ========== Helpers ==========

    @Test
    void listPartitionsReturnsCreatedPartitions() {
        manager.getOrCreatePartition(PartitionId.of(0));
        manager.getOrCreatePartition(PartitionId.of(3));

        assertThat(manager.listPartitions()).containsExactlyInAnyOrder(
                PartitionId.of(0), PartitionId.of(3)
        );
    }
}
