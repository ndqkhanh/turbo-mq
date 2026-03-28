package turbomq.storage;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import turbomq.common.PartitionId;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Per-partition storage backed by a RocksDB column family.
 *
 * Each partition gets its own column family for isolation.
 * Supports message append/read by offset and consumer group offset tracking.
 *
 * Key encoding:
 * - Messages:        "msg:" + 8-byte big-endian offset
 * - Consumer offsets: "co:" + groupId
 * - Metadata:        "meta:" + key
 */
public final class PartitionStore {

    private static final Logger log = LoggerFactory.getLogger(PartitionStore.class);
    private static final byte[] MSG_PREFIX = "msg:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CO_PREFIX = "co:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LATEST_OFFSET_KEY = "meta:latest_offset".getBytes(StandardCharsets.UTF_8);

    private final PartitionId partitionId;
    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private long latestOffset;

    PartitionStore(PartitionId partitionId, RocksDB db, ColumnFamilyHandle cfHandle) {
        this.partitionId = partitionId;
        this.db = db;
        this.cfHandle = cfHandle;
        this.latestOffset = loadLatestOffset();
    }

    // ========== Message Operations ==========

    public void append(long offset, byte[] data) {
        try {
            db.put(cfHandle, msgKey(offset), data);
            latestOffset = Math.max(latestOffset, offset);
            db.put(cfHandle, LATEST_OFFSET_KEY, longToBytes(latestOffset));
        } catch (RocksDBException e) {
            throw new StorageException("Failed to append at offset " + offset, e);
        }
    }

    public byte[] read(long offset) {
        try {
            return db.get(cfHandle, msgKey(offset));
        } catch (RocksDBException e) {
            throw new StorageException("Failed to read offset " + offset, e);
        }
    }

    /**
     * Read messages in range [fromOffset, toOffset).
     */
    public List<byte[]> readRange(long fromOffset, long toOffset) {
        List<byte[]> results = new ArrayList<>();
        byte[] startKey = msgKey(fromOffset);
        byte[] endKey = msgKey(toOffset);

        try (var readOptions = new ReadOptions();
             var iterator = db.newIterator(cfHandle, readOptions)) {

            iterator.seek(startKey);
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (compareBytes(key, endKey) >= 0) break;
                results.add(Arrays.copyOf(iterator.value(), iterator.value().length));
                iterator.next();
            }
        }
        return results;
    }

    public long latestOffset() {
        return latestOffset;
    }

    // ========== Consumer Offset ==========

    public void commitConsumerOffset(String groupId, long offset) {
        try {
            byte[] key = consumerOffsetKey(groupId);
            db.put(cfHandle, key, longToBytes(offset));
        } catch (RocksDBException e) {
            throw new StorageException("Failed to commit consumer offset", e);
        }
    }

    public long getConsumerOffset(String groupId) {
        try {
            byte[] val = db.get(cfHandle, consumerOffsetKey(groupId));
            return val == null ? 0 : bytesToLong(val);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to get consumer offset", e);
        }
    }

    // ========== Internal ==========

    ColumnFamilyHandle handle() {
        return cfHandle;
    }

    private long loadLatestOffset() {
        try {
            byte[] val = db.get(cfHandle, LATEST_OFFSET_KEY);
            return val == null ? 0 : bytesToLong(val);
        } catch (RocksDBException e) {
            log.warn("Failed to load latest offset for {}, defaulting to 0", partitionId, e);
            return 0;
        }
    }

    private static byte[] msgKey(long offset) {
        byte[] key = new byte[MSG_PREFIX.length + 8];
        System.arraycopy(MSG_PREFIX, 0, key, 0, MSG_PREFIX.length);
        ByteBuffer.wrap(key, MSG_PREFIX.length, 8).putLong(offset);
        return key;
    }

    private static byte[] consumerOffsetKey(String groupId) {
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[CO_PREFIX.length + groupBytes.length];
        System.arraycopy(CO_PREFIX, 0, key, 0, CO_PREFIX.length);
        System.arraycopy(groupBytes, 0, key, CO_PREFIX.length, groupBytes.length);
        return key;
    }

    private static byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    private static long bytesToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.compareUnsigned(a[i], b[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.length, b.length);
    }
}
