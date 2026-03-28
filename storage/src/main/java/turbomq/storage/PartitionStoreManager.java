package turbomq.storage;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import turbomq.common.PartitionId;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages per-partition column families in a single RocksDB instance.
 *
 * Each partition is backed by its own column family for isolation.
 * Column family names follow the pattern "partition-{id}".
 */
public final class PartitionStoreManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PartitionStoreManager.class);

    static {
        RocksDB.loadLibrary();
    }

    private final Path dataDir;
    private RocksDB db;
    private DBOptions dbOptions;
    private ColumnFamilyHandle defaultCfHandle;
    private final Map<PartitionId, PartitionStore> stores = new ConcurrentHashMap<>();
    private final Map<PartitionId, ColumnFamilyHandle> cfHandles = new ConcurrentHashMap<>();

    public PartitionStoreManager(Path dataDir) {
        this.dataDir = dataDir;
    }

    public void open() {
        try {
            dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);

            // List existing column families
            List<byte[]> existingCfNames;
            try {
                existingCfNames = RocksDB.listColumnFamilies(new Options().setCreateIfMissing(true), dataDir.toString());
            } catch (RocksDBException e) {
                existingCfNames = List.of();
            }

            // Build column family descriptors
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            var cfOptions = new ColumnFamilyOptions();

            // Always include default CF
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));

            // Add existing partition CFs
            Set<String> seen = new HashSet<>();
            seen.add(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8));
            for (byte[] name : existingCfNames) {
                String cfName = new String(name, StandardCharsets.UTF_8);
                if (!seen.contains(cfName)) {
                    cfDescriptors.add(new ColumnFamilyDescriptor(name, cfOptions));
                    seen.add(cfName);
                }
            }

            // Open with all column families
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            db = RocksDB.open(dbOptions, dataDir.toString(), cfDescriptors, handles);

            // Map handles
            defaultCfHandle = handles.get(0);
            for (int i = 1; i < handles.size(); i++) {
                String cfName = new String(cfDescriptors.get(i).getName(), StandardCharsets.UTF_8);
                if (cfName.startsWith("partition-")) {
                    int id = Integer.parseInt(cfName.substring("partition-".length()));
                    PartitionId pid = PartitionId.of(id);
                    cfHandles.put(pid, handles.get(i));
                    stores.put(pid, new PartitionStore(pid, db, handles.get(i)));
                }
            }

            log.info("PartitionStoreManager opened at {} with {} partitions", dataDir, stores.size());
        } catch (RocksDBException e) {
            throw new StorageException("Failed to open PartitionStoreManager at " + dataDir, e);
        }
    }

    public PartitionStore getOrCreatePartition(PartitionId partitionId) {
        return stores.computeIfAbsent(partitionId, this::createPartition);
    }

    public List<PartitionId> listPartitions() {
        return List.copyOf(stores.keySet());
    }

    @Override
    public void close() {
        for (ColumnFamilyHandle handle : cfHandles.values()) {
            handle.close();
        }
        if (defaultCfHandle != null) {
            defaultCfHandle.close();
            defaultCfHandle = null;
        }
        if (db != null) {
            db.close();
            db = null;
        }
        if (dbOptions != null) {
            dbOptions.close();
            dbOptions = null;
        }
        stores.clear();
        cfHandles.clear();
    }

    private PartitionStore createPartition(PartitionId partitionId) {
        try {
            String cfName = "partition-" + partitionId.id();
            var cfOptions = new ColumnFamilyOptions();
            var descriptor = new ColumnFamilyDescriptor(cfName.getBytes(StandardCharsets.UTF_8), cfOptions);
            ColumnFamilyHandle handle = db.createColumnFamily(descriptor);

            cfHandles.put(partitionId, handle);
            PartitionStore store = new PartitionStore(partitionId, db, handle);
            log.info("Created partition column family: {}", cfName);
            return store;
        } catch (RocksDBException e) {
            throw new StorageException("Failed to create partition " + partitionId, e);
        }
    }
}
