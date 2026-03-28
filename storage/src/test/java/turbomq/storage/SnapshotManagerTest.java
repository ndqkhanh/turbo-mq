package turbomq.storage;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD tests for SnapshotManager — RocksDB checkpoint-based snapshots.
 *
 * Tests cover:
 * - Creating checkpoints at a given Raft index/term
 * - Listing snapshots ordered by index (descending)
 * - Getting the latest snapshot
 * - Cleaning up old snapshots (retention policy)
 * - Snapshot directory contains actual RocksDB files
 * - Error handling for invalid state
 */
class SnapshotManagerTest {

    @TempDir
    Path tempDir;

    private RocksDBEngine engine;
    private SnapshotManager snapshotManager;

    @BeforeEach
    void setUp() {
        engine = new RocksDBEngine(tempDir.resolve("db"));
        engine.open();
        snapshotManager = new SnapshotManager(engine, tempDir.resolve("snapshots"));
    }

    @AfterEach
    void tearDown() {
        engine.close();
    }

    // ========== Snapshot Creation ==========

    @Test
    @DisplayName("createSnapshot produces a checkpoint directory with RocksDB files")
    void createSnapshotProducesCheckpointDirectory() throws IOException {
        // Given: some data in the engine
        engine.put("key1".getBytes(), "value1".getBytes());
        engine.put("key2".getBytes(), "value2".getBytes());

        // When: create a snapshot at index 10, term 2
        SnapshotMetadata meta = snapshotManager.createSnapshot(10, 2);

        // Then: metadata is correct
        assertThat(meta.lastIncludedIndex()).isEqualTo(10);
        assertThat(meta.lastIncludedTerm()).isEqualTo(2);
        assertThat(meta.createdAt()).isNotNull();
        assertThat(meta.path()).exists();

        // And: the checkpoint directory contains files (SST, MANIFEST, CURRENT, etc.)
        try (var files = Files.list(meta.path())) {
            assertThat(files.count()).isGreaterThan(0);
        }
    }

    @Test
    @DisplayName("snapshot checkpoint contains consistent data from time of creation")
    void snapshotContainsConsistentData() {
        // Given: data written before snapshot
        engine.put("before".getBytes(), "yes".getBytes());
        SnapshotMetadata meta = snapshotManager.createSnapshot(5, 1);

        // When: more data written after snapshot
        engine.put("after".getBytes(), "no".getBytes());

        // Then: opening the checkpoint should show only pre-snapshot data
        try (RocksDBEngine checkpointEngine = new RocksDBEngine(meta.path())) {
            checkpointEngine.open();
            assertThat(checkpointEngine.get("before".getBytes())).isEqualTo("yes".getBytes());
            assertThat(checkpointEngine.get("after".getBytes())).isNull();
        }
    }

    @Test
    @DisplayName("multiple snapshots at different indices create separate directories")
    void multipleSnapshotsCreateSeparateDirectories() {
        engine.put("k1".getBytes(), "v1".getBytes());
        SnapshotMetadata snap1 = snapshotManager.createSnapshot(5, 1);

        engine.put("k2".getBytes(), "v2".getBytes());
        SnapshotMetadata snap2 = snapshotManager.createSnapshot(10, 2);

        assertThat(snap1.path()).isNotEqualTo(snap2.path());
        assertThat(snap1.path()).exists();
        assertThat(snap2.path()).exists();
    }

    // ========== Snapshot Listing ==========

    @Test
    @DisplayName("listSnapshots returns snapshots ordered by index descending")
    void listSnapshotsOrderedByIndexDescending() {
        snapshotManager.createSnapshot(5, 1);
        snapshotManager.createSnapshot(10, 2);
        snapshotManager.createSnapshot(15, 3);

        List<SnapshotMetadata> snapshots = snapshotManager.listSnapshots();

        assertThat(snapshots).hasSize(3);
        assertThat(snapshots.get(0).lastIncludedIndex()).isEqualTo(15);
        assertThat(snapshots.get(1).lastIncludedIndex()).isEqualTo(10);
        assertThat(snapshots.get(2).lastIncludedIndex()).isEqualTo(5);
    }

    @Test
    @DisplayName("listSnapshots returns empty list when no snapshots exist")
    void listSnapshotsEmptyWhenNone() {
        assertThat(snapshotManager.listSnapshots()).isEmpty();
    }

    // ========== Latest Snapshot ==========

    @Test
    @DisplayName("latestSnapshot returns the highest-index snapshot")
    void latestSnapshotReturnsHighestIndex() {
        snapshotManager.createSnapshot(5, 1);
        snapshotManager.createSnapshot(15, 3);
        snapshotManager.createSnapshot(10, 2);

        var latest = snapshotManager.latestSnapshot();

        assertThat(latest).isPresent();
        assertThat(latest.get().lastIncludedIndex()).isEqualTo(15);
        assertThat(latest.get().lastIncludedTerm()).isEqualTo(3);
    }

    @Test
    @DisplayName("latestSnapshot returns empty when no snapshots exist")
    void latestSnapshotEmptyWhenNone() {
        assertThat(snapshotManager.latestSnapshot()).isEmpty();
    }

    // ========== Snapshot Cleanup ==========

    @Test
    @DisplayName("cleanupOldSnapshots retains only the N most recent snapshots")
    void cleanupRetainsOnlyNMostRecent() {
        snapshotManager.createSnapshot(5, 1);
        snapshotManager.createSnapshot(10, 2);
        snapshotManager.createSnapshot(15, 3);
        snapshotManager.createSnapshot(20, 4);

        int removed = snapshotManager.cleanupOldSnapshots(2);

        assertThat(removed).isEqualTo(2);
        List<SnapshotMetadata> remaining = snapshotManager.listSnapshots();
        assertThat(remaining).hasSize(2);
        assertThat(remaining.get(0).lastIncludedIndex()).isEqualTo(20);
        assertThat(remaining.get(1).lastIncludedIndex()).isEqualTo(15);
    }

    @Test
    @DisplayName("cleanupOldSnapshots deletes checkpoint directories from disk")
    void cleanupDeletesDirectoriesFromDisk() {
        SnapshotMetadata old1 = snapshotManager.createSnapshot(5, 1);
        SnapshotMetadata old2 = snapshotManager.createSnapshot(10, 2);
        snapshotManager.createSnapshot(15, 3);

        snapshotManager.cleanupOldSnapshots(1);

        assertThat(old1.path()).doesNotExist();
        assertThat(old2.path()).doesNotExist();
    }

    @Test
    @DisplayName("cleanupOldSnapshots with retain >= count removes nothing")
    void cleanupWithHighRetainRemovesNothing() {
        snapshotManager.createSnapshot(5, 1);
        snapshotManager.createSnapshot(10, 2);

        int removed = snapshotManager.cleanupOldSnapshots(5);

        assertThat(removed).isEqualTo(0);
        assertThat(snapshotManager.listSnapshots()).hasSize(2);
    }

    // ========== Snapshot Byte Transfer ==========

    @Test
    @DisplayName("readSnapshotChunk reads bytes from the snapshot checkpoint files")
    void readSnapshotChunkReadsBytes() throws IOException {
        engine.put("data".getBytes(), "value".getBytes());
        SnapshotMetadata meta = snapshotManager.createSnapshot(10, 2);

        // Get total snapshot size
        long totalSize = snapshotManager.snapshotSize(meta);
        assertThat(totalSize).isGreaterThan(0);

        // Read all bytes in one chunk
        byte[] data = snapshotManager.readSnapshotData(meta);
        assertThat(data.length).isEqualTo((int) totalSize);
    }

    // ========== Error Cases ==========

    @Test
    @DisplayName("createSnapshot with negative index throws")
    void createSnapshotNegativeIndexThrows() {
        assertThatThrownBy(() -> snapshotManager.createSnapshot(-1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("createSnapshot with negative term throws")
    void createSnapshotNegativeTermThrows() {
        assertThatThrownBy(() -> snapshotManager.createSnapshot(5, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
