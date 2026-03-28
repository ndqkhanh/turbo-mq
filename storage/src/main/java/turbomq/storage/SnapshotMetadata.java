package turbomq.storage;

import java.nio.file.Path;
import java.time.Instant;

/**
 * Metadata describing a RocksDB snapshot checkpoint.
 *
 * @param lastIncludedIndex the Raft log index up to which this snapshot covers
 * @param lastIncludedTerm  the Raft term of the lastIncludedIndex entry
 * @param createdAt         when the snapshot was taken
 * @param path              filesystem path to the checkpoint directory
 */
public record SnapshotMetadata(
        long lastIncludedIndex,
        long lastIncludedTerm,
        Instant createdAt,
        Path path) {

    public SnapshotMetadata {
        if (lastIncludedIndex < 0) throw new IllegalArgumentException("lastIncludedIndex must be >= 0");
        if (lastIncludedTerm < 0) throw new IllegalArgumentException("lastIncludedTerm must be >= 0");
    }
}
