package turbomq.broker;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD tests for exactly-once delivery semantics.
 *
 * Tests cover:
 * - Producer ID allocation (monotonically increasing)
 * - Idempotent writes (duplicate sequence numbers rejected)
 * - Sequence gap detection (out-of-order rejected)
 * - Per-partition, per-producer sequence tracking
 * - Sequence window (sliding window of accepted sequences)
 * - Multiple producers on same partition
 * - Producer session expiry
 */
class ExactlyOnceTest {

    private IdempotencyManager idempotencyManager;

    @BeforeEach
    void setUp() {
        idempotencyManager = new IdempotencyManager();
    }

    // ========== Producer ID Allocation ==========

    @Test
    @DisplayName("allocateProducerId returns monotonically increasing IDs")
    void allocateProducerIdMonotonicallyIncreasing() {
        long id1 = idempotencyManager.allocateProducerId();
        long id2 = idempotencyManager.allocateProducerId();
        long id3 = idempotencyManager.allocateProducerId();

        assertThat(id1).isLessThan(id2);
        assertThat(id2).isLessThan(id3);
    }

    // ========== Idempotent Writes ==========

    @Test
    @DisplayName("first write with sequence 0 is accepted")
    void firstWriteAccepted() {
        long pid = idempotencyManager.allocateProducerId();

        var result = idempotencyManager.checkAndRecord(pid, "partition-0", 0);

        assertThat(result).isEqualTo(IdempotencyManager.Result.ACCEPTED);
    }

    @Test
    @DisplayName("sequential writes are accepted")
    void sequentialWritesAccepted() {
        long pid = idempotencyManager.allocateProducerId();

        assertThat(idempotencyManager.checkAndRecord(pid, "partition-0", 0))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);
        assertThat(idempotencyManager.checkAndRecord(pid, "partition-0", 1))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);
        assertThat(idempotencyManager.checkAndRecord(pid, "partition-0", 2))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);
    }

    @Test
    @DisplayName("duplicate sequence number is rejected as DUPLICATE")
    void duplicateSequenceRejected() {
        long pid = idempotencyManager.allocateProducerId();
        idempotencyManager.checkAndRecord(pid, "partition-0", 0);

        var result = idempotencyManager.checkAndRecord(pid, "partition-0", 0);

        assertThat(result).isEqualTo(IdempotencyManager.Result.DUPLICATE);
    }

    @Test
    @DisplayName("re-sending last accepted sequence is DUPLICATE")
    void resendLastSequenceIsDuplicate() {
        long pid = idempotencyManager.allocateProducerId();
        idempotencyManager.checkAndRecord(pid, "partition-0", 0);
        idempotencyManager.checkAndRecord(pid, "partition-0", 1);
        idempotencyManager.checkAndRecord(pid, "partition-0", 2);

        // Re-send sequence 2
        var result = idempotencyManager.checkAndRecord(pid, "partition-0", 2);
        assertThat(result).isEqualTo(IdempotencyManager.Result.DUPLICATE);
    }

    @Test
    @DisplayName("sequence gap is rejected as OUT_OF_ORDER")
    void sequenceGapRejected() {
        long pid = idempotencyManager.allocateProducerId();
        idempotencyManager.checkAndRecord(pid, "partition-0", 0);

        // Skip sequence 1, try sequence 2
        var result = idempotencyManager.checkAndRecord(pid, "partition-0", 2);

        assertThat(result).isEqualTo(IdempotencyManager.Result.OUT_OF_ORDER);
    }

    @Test
    @DisplayName("sequence going backwards is rejected as DUPLICATE")
    void sequenceBackwardsIsDuplicate() {
        long pid = idempotencyManager.allocateProducerId();
        idempotencyManager.checkAndRecord(pid, "partition-0", 0);
        idempotencyManager.checkAndRecord(pid, "partition-0", 1);
        idempotencyManager.checkAndRecord(pid, "partition-0", 2);

        // Go back to 1
        var result = idempotencyManager.checkAndRecord(pid, "partition-0", 1);
        assertThat(result).isEqualTo(IdempotencyManager.Result.DUPLICATE);
    }

    // ========== Per-Partition Isolation ==========

    @Test
    @DisplayName("same producer can write independently to different partitions")
    void sameProducerDifferentPartitions() {
        long pid = idempotencyManager.allocateProducerId();

        assertThat(idempotencyManager.checkAndRecord(pid, "partition-0", 0))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);
        assertThat(idempotencyManager.checkAndRecord(pid, "partition-1", 0))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);

        // Sequence 1 on partition-0
        assertThat(idempotencyManager.checkAndRecord(pid, "partition-0", 1))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);
        // Sequence 1 on partition-1 (independent)
        assertThat(idempotencyManager.checkAndRecord(pid, "partition-1", 1))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);
    }

    // ========== Multiple Producers ==========

    @Test
    @DisplayName("different producers have independent sequence tracking")
    void differentProducersIndependent() {
        long pid1 = idempotencyManager.allocateProducerId();
        long pid2 = idempotencyManager.allocateProducerId();

        assertThat(idempotencyManager.checkAndRecord(pid1, "partition-0", 0))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);
        assertThat(idempotencyManager.checkAndRecord(pid2, "partition-0", 0))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);

        // Sequence 1 for both
        assertThat(idempotencyManager.checkAndRecord(pid1, "partition-0", 1))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);
        assertThat(idempotencyManager.checkAndRecord(pid2, "partition-0", 1))
                .isEqualTo(IdempotencyManager.Result.ACCEPTED);
    }

    // ========== Unknown Producer ==========

    @Test
    @DisplayName("unknown producer ID is rejected")
    void unknownProducerRejected() {
        var result = idempotencyManager.checkAndRecord(999, "partition-0", 0);

        assertThat(result).isEqualTo(IdempotencyManager.Result.UNKNOWN_PRODUCER);
    }

    // ========== Sequence Query ==========

    @Test
    @DisplayName("getLastSequence returns the last accepted sequence")
    void getLastSequenceReturnsLastAccepted() {
        long pid = idempotencyManager.allocateProducerId();
        idempotencyManager.checkAndRecord(pid, "partition-0", 0);
        idempotencyManager.checkAndRecord(pid, "partition-0", 1);
        idempotencyManager.checkAndRecord(pid, "partition-0", 2);

        assertThat(idempotencyManager.getLastSequence(pid, "partition-0")).isEqualTo(2);
    }

    @Test
    @DisplayName("getLastSequence returns -1 for unknown producer/partition")
    void getLastSequenceUnknownReturnsNegOne() {
        assertThat(idempotencyManager.getLastSequence(999, "partition-0")).isEqualTo(-1);

        long pid = idempotencyManager.allocateProducerId();
        assertThat(idempotencyManager.getLastSequence(pid, "partition-0")).isEqualTo(-1);
    }

    // ========== Producer Expiry ==========

    @Test
    @DisplayName("expireProducer removes all state for a producer")
    void expireProducerRemovesState() {
        long pid = idempotencyManager.allocateProducerId();
        idempotencyManager.checkAndRecord(pid, "partition-0", 0);
        idempotencyManager.checkAndRecord(pid, "partition-0", 1);

        idempotencyManager.expireProducer(pid);

        // Producer is now unknown
        assertThat(idempotencyManager.checkAndRecord(pid, "partition-0", 0))
                .isEqualTo(IdempotencyManager.Result.UNKNOWN_PRODUCER);
        assertThat(idempotencyManager.getLastSequence(pid, "partition-0")).isEqualTo(-1);
    }

    // ========== Active Producer Count ==========

    @Test
    @DisplayName("activeProducerCount tracks allocated producers")
    void activeProducerCountTracksAllocated() {
        assertThat(idempotencyManager.activeProducerCount()).isEqualTo(0);

        long pid1 = idempotencyManager.allocateProducerId();
        long pid2 = idempotencyManager.allocateProducerId();
        assertThat(idempotencyManager.activeProducerCount()).isEqualTo(2);

        idempotencyManager.expireProducer(pid1);
        assertThat(idempotencyManager.activeProducerCount()).isEqualTo(1);
    }
}
