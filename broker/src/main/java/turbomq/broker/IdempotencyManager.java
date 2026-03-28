package turbomq.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages idempotent producer writes for exactly-once delivery semantics.
 *
 * Each producer is assigned a unique ID. For each (producerId, partitionKey) pair,
 * the manager tracks the last accepted sequence number. Writes are checked against
 * this state to detect duplicates and out-of-order deliveries.
 *
 * Thread-safe: uses ConcurrentHashMap and AtomicLong for lock-free operation.
 */
public final class IdempotencyManager {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyManager.class);

    /** Result of an idempotency check. */
    public enum Result {
        /** Write accepted — sequence is the expected next value. */
        ACCEPTED,
        /** Write rejected — this sequence was already written. */
        DUPLICATE,
        /** Write rejected — sequence is ahead of expected (gap). */
        OUT_OF_ORDER,
        /** Write rejected — producer ID is not registered. */
        UNKNOWN_PRODUCER
    }

    /** Monotonically increasing producer ID counter. */
    private final AtomicLong nextProducerId = new AtomicLong(1);

    /** Set of active producer IDs. */
    private final Set<Long> activeProducers = ConcurrentHashMap.newKeySet();

    /**
     * Per-producer, per-partition last accepted sequence number.
     * Key: "producerId:partitionKey", Value: last accepted sequence.
     */
    private final Map<String, Long> sequenceState = new ConcurrentHashMap<>();

    /**
     * Allocate a new unique producer ID.
     */
    public long allocateProducerId() {
        long id = nextProducerId.getAndIncrement();
        activeProducers.add(id);
        log.debug("Allocated producer ID: {}", id);
        return id;
    }

    /**
     * Check if a write is idempotent and record it if accepted.
     *
     * @param producerId   the producer's unique ID
     * @param partitionKey the partition being written to
     * @param sequence     the producer-assigned sequence number
     * @return the result of the idempotency check
     */
    public Result checkAndRecord(long producerId, String partitionKey, long sequence) {
        if (!activeProducers.contains(producerId)) {
            return Result.UNKNOWN_PRODUCER;
        }

        String key = producerId + ":" + partitionKey;
        Long lastSeq = sequenceState.get(key);

        if (lastSeq == null) {
            // First write for this producer+partition
            if (sequence == 0) {
                sequenceState.put(key, 0L);
                return Result.ACCEPTED;
            }
            // First write must start at 0
            return sequence < 0 ? Result.DUPLICATE : Result.OUT_OF_ORDER;
        }

        if (sequence <= lastSeq) {
            return Result.DUPLICATE;
        }

        if (sequence == lastSeq + 1) {
            sequenceState.put(key, sequence);
            return Result.ACCEPTED;
        }

        // sequence > lastSeq + 1 → gap
        return Result.OUT_OF_ORDER;
    }

    /**
     * Get the last accepted sequence number for a producer on a partition.
     *
     * @return the last sequence, or -1 if unknown
     */
    public long getLastSequence(long producerId, String partitionKey) {
        String key = producerId + ":" + partitionKey;
        Long lastSeq = sequenceState.get(key);
        return lastSeq != null ? lastSeq : -1;
    }

    /**
     * Expire a producer, removing all its state.
     * Called when a producer session times out.
     */
    public void expireProducer(long producerId) {
        activeProducers.remove(producerId);
        String prefix = producerId + ":";
        sequenceState.keySet().removeIf(k -> k.startsWith(prefix));
        log.info("Expired producer {}", producerId);
    }

    /**
     * Get the number of currently active producers.
     */
    public int activeProducerCount() {
        return activeProducers.size();
    }
}
