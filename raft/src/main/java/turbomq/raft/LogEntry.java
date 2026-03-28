package turbomq.raft;

/**
 * A single entry in the Raft replicated log.
 *
 * @param term  the leader's term when this entry was created
 * @param index the log position (1-based)
 * @param data  the command payload (opaque bytes applied to the state machine)
 */
public record LogEntry(long term, long index, byte[] data) {

    /** Sentinel for no-op entries used after leader election. */
    public static LogEntry noop(long term, long index) {
        return new LogEntry(term, index, new byte[0]);
    }
}
