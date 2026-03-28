package turbomq.storage;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Low-level RocksDB wrapper providing basic key-value operations,
 * WriteBatch support, and range/prefix scans.
 *
 * Encapsulates RocksDB lifecycle and native resource management.
 * Thread-safe: RocksDB handles internal locking.
 */
public final class RocksDBEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RocksDBEngine.class);

    static {
        RocksDB.loadLibrary();
    }

    private final Path dataDir;
    private RocksDB db;
    private Options options;

    public RocksDBEngine(Path dataDir) {
        this.dataDir = dataDir;
    }

    public void open() {
        try {
            options = new Options()
                    .setCreateIfMissing(true)
                    .setWriteBufferSize(64 * 1024 * 1024)     // 64MB memtable
                    .setMaxWriteBufferNumber(3)
                    .setLevelCompactionDynamicLevelBytes(true);

            db = RocksDB.open(options, dataDir.toString());
            log.info("RocksDB opened at {}", dataDir);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to open RocksDB at " + dataDir, e);
        }
    }

    // ========== Single Key Operations ==========

    public void put(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to put key", e);
        }
    }

    public byte[] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to get key", e);
        }
    }

    public void delete(byte[] key) {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to delete key", e);
        }
    }

    // ========== WriteBatch ==========

    public StorageWriteBatch newWriteBatch() {
        return new StorageWriteBatch();
    }

    public void write(StorageWriteBatch batch) {
        try (var writeOptions = new WriteOptions()) {
            db.write(writeOptions, batch.batch);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to write batch", e);
        }
    }

    // ========== Range Scan ==========

    /**
     * Scan keys in range [startKey, endKey).
     */
    public List<KeyValue> scan(byte[] startKey, byte[] endKey) {
        List<KeyValue> results = new ArrayList<>();
        try (var readOptions = new ReadOptions();
             var iterator = db.newIterator(readOptions)) {

            iterator.seek(startKey);
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (compareBytes(key, endKey) >= 0) break;
                results.add(new KeyValue(
                        Arrays.copyOf(key, key.length),
                        Arrays.copyOf(iterator.value(), iterator.value().length)
                ));
                iterator.next();
            }
        }
        return results;
    }

    /**
     * Scan all keys sharing the given prefix.
     */
    public List<KeyValue> scanPrefix(byte[] prefix) {
        List<KeyValue> results = new ArrayList<>();
        try (var readOptions = new ReadOptions();
             var iterator = db.newIterator(readOptions)) {

            iterator.seek(prefix);
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (!startsWith(key, prefix)) break;
                results.add(new KeyValue(
                        Arrays.copyOf(key, key.length),
                        Arrays.copyOf(iterator.value(), iterator.value().length)
                ));
                iterator.next();
            }
        }
        return results;
    }

    // ========== Lifecycle ==========

    @Override
    public void close() {
        if (db != null) {
            db.close();
            db = null;
        }
        if (options != null) {
            options.close();
            options = null;
        }
    }

    // ========== Internal ==========

    RocksDB rawDb() {
        return db;
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.compareUnsigned(a[i], b[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.length, b.length);
    }

    private static boolean startsWith(byte[] data, byte[] prefix) {
        if (data.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) return false;
        }
        return true;
    }

    // ========== Inner Types ==========

    public record KeyValue(byte[] key, byte[] value) {}

    public static final class StorageWriteBatch implements AutoCloseable {
        final WriteBatch batch = new WriteBatch();

        public void put(byte[] key, byte[] value) {
            try {
                batch.put(key, value);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to add put to batch", e);
            }
        }

        public void delete(byte[] key) {
            try {
                batch.delete(key);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to add delete to batch", e);
            }
        }

        @Override
        public void close() {
            batch.close();
        }
    }
}
