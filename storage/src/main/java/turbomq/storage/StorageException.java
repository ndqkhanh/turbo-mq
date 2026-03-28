package turbomq.storage;

/**
 * Unchecked exception wrapping RocksDB errors.
 */
public final class StorageException extends RuntimeException {

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
