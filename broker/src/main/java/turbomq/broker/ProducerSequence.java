package turbomq.broker;

/**
 * Tracks the last acknowledged sequence number for a producer on a partition.
 *
 * @param producerId unique producer identifier
 * @param lastSequence last sequence number successfully written
 */
public record ProducerSequence(long producerId, long lastSequence) {

    public ProducerSequence {
        if (producerId < 0) throw new IllegalArgumentException("producerId must be >= 0");
        if (lastSequence < 0) throw new IllegalArgumentException("lastSequence must be >= 0");
    }
}
