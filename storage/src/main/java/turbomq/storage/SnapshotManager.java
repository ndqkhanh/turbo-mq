package turbomq.storage;

import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manages RocksDB checkpoint-based snapshots for Raft log compaction.
 *
 * Each snapshot is a RocksDB checkpoint (hard-linked SST files) stored
 * in a subdirectory named by the Raft index. Snapshots are point-in-time
 * consistent and created with minimal I/O overhead via hard links.
 *
 * Thread-safe: snapshot creation and cleanup are synchronized.
 */
public final class SnapshotManager {

    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);

    private final RocksDBEngine engine;
    private final Path snapshotsDir;

    /** In-memory index of known snapshots, ordered by lastIncludedIndex descending. */
    private final List<SnapshotMetadata> snapshots = new ArrayList<>();

    public SnapshotManager(RocksDBEngine engine, Path snapshotsDir) {
        this.engine = engine;
        this.snapshotsDir = snapshotsDir;
        ensureDirectoryExists(snapshotsDir);
    }

    /**
     * Create a RocksDB checkpoint snapshot at the given Raft log position.
     *
     * @param lastIncludedIndex Raft log index this snapshot covers up to
     * @param lastIncludedTerm  Raft term of the lastIncludedIndex entry
     * @return metadata describing the created snapshot
     */
    public synchronized SnapshotMetadata createSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        if (lastIncludedIndex < 0) throw new IllegalArgumentException("lastIncludedIndex must be >= 0");
        if (lastIncludedTerm < 0) throw new IllegalArgumentException("lastIncludedTerm must be >= 0");

        Path checkpointDir = snapshotsDir.resolve("snapshot-" + lastIncludedIndex);
        try {
            Checkpoint checkpoint = Checkpoint.create(engine.rawDb());
            checkpoint.createCheckpoint(checkpointDir.toString());
        } catch (RocksDBException e) {
            throw new StorageException("Failed to create snapshot at index " + lastIncludedIndex, e);
        }

        SnapshotMetadata meta = new SnapshotMetadata(
                lastIncludedIndex, lastIncludedTerm, Instant.now(), checkpointDir);
        snapshots.add(meta);
        snapshots.sort(Comparator.comparingLong(SnapshotMetadata::lastIncludedIndex).reversed());

        log.info("Created snapshot at index={} term={} path={}", lastIncludedIndex, lastIncludedTerm, checkpointDir);
        return meta;
    }

    /**
     * List all known snapshots, ordered by lastIncludedIndex descending.
     */
    public synchronized List<SnapshotMetadata> listSnapshots() {
        return List.copyOf(snapshots);
    }

    /**
     * Get the latest (highest-index) snapshot, if any.
     */
    public synchronized Optional<SnapshotMetadata> latestSnapshot() {
        return snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.getFirst());
    }

    /**
     * Remove old snapshots, keeping only the {@code retainCount} most recent.
     *
     * @param retainCount number of most recent snapshots to keep
     * @return number of snapshots removed
     */
    public synchronized int cleanupOldSnapshots(int retainCount) {
        if (snapshots.size() <= retainCount) return 0;

        List<SnapshotMetadata> toRemove = new ArrayList<>(
                snapshots.subList(retainCount, snapshots.size()));
        int removed = 0;

        for (SnapshotMetadata meta : toRemove) {
            try {
                deleteDirectory(meta.path());
                snapshots.remove(meta);
                removed++;
                log.info("Deleted old snapshot at index={}", meta.lastIncludedIndex());
            } catch (IOException e) {
                log.warn("Failed to delete snapshot at {}", meta.path(), e);
            }
        }
        return removed;
    }

    /**
     * Get the total size in bytes of a snapshot's checkpoint directory.
     */
    public long snapshotSize(SnapshotMetadata meta) throws IOException {
        long size = 0;
        try (Stream<Path> files = Files.walk(meta.path())) {
            for (Path file : files.collect(Collectors.toList())) {
                if (Files.isRegularFile(file)) {
                    size += Files.size(file);
                }
            }
        }
        return size;
    }

    /**
     * Read the entire snapshot as a TAR-like byte stream: for each file,
     * concatenate all file bytes. Used for InstallSnapshot RPC transfer.
     *
     * For production, this should be chunked/streamed. This simple version
     * reads everything into memory for correctness testing.
     */
    public byte[] readSnapshotData(SnapshotMetadata meta) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (Stream<Path> files = Files.walk(meta.path())) {
            for (Path file : files.sorted().collect(Collectors.toList())) {
                if (Files.isRegularFile(file)) {
                    baos.write(Files.readAllBytes(file));
                }
            }
        }
        return baos.toByteArray();
    }

    // ========== Internal ==========

    private static void ensureDirectoryExists(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create snapshots directory: " + dir, e);
        }
    }

    private static void deleteDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }
}
