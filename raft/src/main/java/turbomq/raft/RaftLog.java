package turbomq.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory Raft replicated log with snapshot support.
 *
 * Entries are 1-indexed (index 0 is a sentinel). After a snapshot is installed,
 * entries up to snapshotIndex are discarded and the log is rebased.
 *
 * Thread safety: not thread-safe. Each RaftNode owns one RaftLog
 * and processes messages on a single virtual thread.
 */
public final class RaftLog {

    /** Entries stored relative to snapshotIndex. Entry at position 0 has global index (snapshotIndex + 1). */
    private final List<LogEntry> entries = new ArrayList<>();

    /** Highest log index known to be committed. */
    private long commitIndex = 0;

    /** Last applied index (for state machine application). */
    private long lastApplied = 0;

    /** Snapshot state: index and term of the last entry included in the snapshot. */
    private long snapshotIndex = 0;
    private long snapshotTerm = 0;

    // ========== Snapshot ==========

    /** Index of the last entry included in the most recent snapshot. */
    public long snapshotIndex() {
        return snapshotIndex;
    }

    /** Term of the last entry included in the most recent snapshot. */
    public long snapshotTerm() {
        return snapshotTerm;
    }

    /**
     * Install a snapshot, discarding all log entries up to (and including) the given index.
     * Entries after the snapshot index are preserved.
     *
     * @param lastIncludedIndex the last log index included in the snapshot
     * @param lastIncludedTerm  the term of that entry
     */
    public void installSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        // Remove entries covered by the snapshot
        while (!entries.isEmpty()) {
            LogEntry first = entries.getFirst();
            if (first.index() <= lastIncludedIndex) {
                entries.removeFirst();
            } else {
                break;
            }
        }

        this.snapshotIndex = lastIncludedIndex;
        this.snapshotTerm = lastIncludedTerm;

        // Advance commit/applied to at least the snapshot point
        if (commitIndex < lastIncludedIndex) {
            commitIndex = lastIncludedIndex;
        }
        if (lastApplied < lastIncludedIndex) {
            lastApplied = lastIncludedIndex;
        }
    }

    // ========== Index / Term ==========

    /** Index of the last entry, or snapshotIndex if no entries after snapshot. */
    public long lastIndex() {
        return entries.isEmpty() ? snapshotIndex : entries.getLast().index();
    }

    /** Term of the last entry, or snapshotTerm if no entries after snapshot. */
    public long lastTerm() {
        return entries.isEmpty() ? snapshotTerm : entries.getLast().term();
    }

    /** Get the entry at the given 1-based global index, or null if out of bounds or compacted. */
    public LogEntry getEntry(long index) {
        if (index <= snapshotIndex || index > lastIndex()) return null;
        int pos = (int) (index - snapshotIndex - 1);
        if (pos < 0 || pos >= entries.size()) return null;
        return entries.get(pos);
    }

    /** Get the term at the given index. Returns snapshotTerm for snapshotIndex, 0 for sentinel or out of bounds. */
    public long termAt(long index) {
        if (index == 0) return 0;
        if (index == snapshotIndex) return snapshotTerm;
        LogEntry entry = getEntry(index);
        return entry != null ? entry.term() : 0;
    }

    // ========== Append / Truncate ==========

    /** Append a single entry to the end of the log. */
    public void append(LogEntry entry) {
        entries.add(entry);
    }

    /** Append multiple entries to the end of the log. */
    public void appendAll(List<LogEntry> newEntries) {
        entries.addAll(newEntries);
    }

    /**
     * Truncate the log from the given index (inclusive) onwards.
     * Used when a follower detects a conflict with the leader's log.
     * Will not truncate into the snapshot.
     */
    public void truncateFrom(long fromIndex) {
        if (fromIndex <= snapshotIndex) return;
        while (!entries.isEmpty() && entries.getLast().index() >= fromIndex) {
            entries.removeLast();
        }
    }

    /**
     * Get all entries from the given index (inclusive) to the end.
     * Used by the leader to build AppendEntries RPCs.
     */
    public List<LogEntry> getEntriesFrom(long fromIndex) {
        if (fromIndex > lastIndex()) return List.of();
        if (fromIndex <= snapshotIndex) fromIndex = snapshotIndex + 1;
        int startPos = (int) (fromIndex - snapshotIndex - 1);
        if (startPos < 0) startPos = 0;
        if (startPos >= entries.size()) return List.of();
        return List.copyOf(entries.subList(startPos, entries.size()));
    }

    // ========== Commit / Applied ==========

    /** Current commit index. */
    public long commitIndex() {
        return commitIndex;
    }

    /**
     * Advance the commit index. Never decreases.
     */
    public void advanceCommitIndex(long newCommitIndex) {
        if (newCommitIndex > commitIndex) {
            commitIndex = Math.min(newCommitIndex, lastIndex());
        }
    }

    /** Last applied index for the state machine. */
    public long lastApplied() {
        return lastApplied;
    }

    /** Advance lastApplied after applying entries to the state machine. */
    public void advanceLastApplied(long newLastApplied) {
        if (newLastApplied > lastApplied) {
            lastApplied = newLastApplied;
        }
    }

    /**
     * Check if the log contains an entry at the given index with the given term.
     * Handles the snapshot boundary: (snapshotIndex, snapshotTerm) is a valid match.
     * Index 0, term 0 is the sentinel — always matches (empty log base case).
     */
    public boolean hasMatchingEntry(long index, long term) {
        if (index == 0 && term == 0) return true;
        if (index == snapshotIndex && term == snapshotTerm && snapshotIndex > 0) return true;
        return termAt(index) == term && term != 0;
    }
}
