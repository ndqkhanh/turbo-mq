package turbomq.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the RocksDB storage engine wrapper.
 *
 * Validates basic put/get/delete, WriteBatch atomicity,
 * iterator-based range scans, and lifecycle management.
 */
class RocksDBEngineTest {

    @TempDir
    Path tempDir;

    private RocksDBEngine engine;

    @BeforeEach
    void setUp() {
        engine = new RocksDBEngine(tempDir);
        engine.open();
    }

    @AfterEach
    void tearDown() {
        engine.close();
    }

    // ========== Basic Operations ==========

    @Test
    void putAndGetValue() {
        engine.put(key("k1"), value("v1"));
        assertThat(engine.get(key("k1"))).isEqualTo(value("v1"));
    }

    @Test
    void getNonExistentKeyReturnsNull() {
        assertThat(engine.get(key("missing"))).isNull();
    }

    @Test
    void deleteRemovesKey() {
        engine.put(key("k1"), value("v1"));
        engine.delete(key("k1"));
        assertThat(engine.get(key("k1"))).isNull();
    }

    @Test
    void putOverwritesExistingValue() {
        engine.put(key("k1"), value("v1"));
        engine.put(key("k1"), value("v2"));
        assertThat(engine.get(key("k1"))).isEqualTo(value("v2"));
    }

    // ========== WriteBatch ==========

    @Test
    void writeBatchAppliesAtomically() {
        try (var batch = engine.newWriteBatch()) {
            batch.put(key("a"), value("1"));
            batch.put(key("b"), value("2"));
            batch.put(key("c"), value("3"));
            engine.write(batch);
        }

        assertThat(engine.get(key("a"))).isEqualTo(value("1"));
        assertThat(engine.get(key("b"))).isEqualTo(value("2"));
        assertThat(engine.get(key("c"))).isEqualTo(value("3"));
    }

    @Test
    void writeBatchWithDeleteAndPut() {
        engine.put(key("x"), value("old"));

        try (var batch = engine.newWriteBatch()) {
            batch.delete(key("x"));
            batch.put(key("y"), value("new"));
            engine.write(batch);
        }

        assertThat(engine.get(key("x"))).isNull();
        assertThat(engine.get(key("y"))).isEqualTo(value("new"));
    }

    // ========== Range Scan ==========

    @Test
    void scanReturnsEntriesInOrder() {
        engine.put(key("b"), value("2"));
        engine.put(key("a"), value("1"));
        engine.put(key("c"), value("3"));

        var entries = engine.scan(key("a"), key("d"));

        assertThat(entries).hasSize(3);
        assertThat(entries.get(0).key()).isEqualTo(key("a"));
        assertThat(entries.get(1).key()).isEqualTo(key("b"));
        assertThat(entries.get(2).key()).isEqualTo(key("c"));
    }

    @Test
    void scanRespectsStartAndEndBounds() {
        engine.put(key("a"), value("1"));
        engine.put(key("b"), value("2"));
        engine.put(key("c"), value("3"));
        engine.put(key("d"), value("4"));

        var entries = engine.scan(key("b"), key("d")); // [b, d)
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).key()).isEqualTo(key("b"));
        assertThat(entries.get(1).key()).isEqualTo(key("c"));
    }

    @Test
    void scanWithNoMatchingKeysReturnsEmpty() {
        engine.put(key("a"), value("1"));
        var entries = engine.scan(key("x"), key("z"));
        assertThat(entries).isEmpty();
    }

    // ========== Prefix Scan ==========

    @Test
    void prefixScanReturnsMatchingKeys() {
        engine.put(key("topic:orders:0001"), value("msg1"));
        engine.put(key("topic:orders:0002"), value("msg2"));
        engine.put(key("topic:users:0001"), value("msg3"));

        var entries = engine.scanPrefix(key("topic:orders:"));
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).value()).isEqualTo(value("msg1"));
        assertThat(entries.get(1).value()).isEqualTo(value("msg2"));
    }

    // ========== Persistence ==========

    @Test
    void dataPersistedAcrossReopen() {
        engine.put(key("persistent"), value("data"));
        engine.close();

        engine = new RocksDBEngine(tempDir);
        engine.open();

        assertThat(engine.get(key("persistent"))).isEqualTo(value("data"));
    }

    // ========== Lifecycle ==========

    @Test
    void doubleCloseIsIdempotent() {
        engine.close();
        assertThatNoException().isThrownBy(() -> engine.close());
    }

    // ========== Helpers ==========

    private byte[] key(String s) { return s.getBytes(); }
    private byte[] value(String s) { return s.getBytes(); }
}
