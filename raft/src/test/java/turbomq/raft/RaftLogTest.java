package turbomq.raft;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class RaftLogTest {

    private RaftLog log;

    @BeforeEach
    void setUp() {
        log = new RaftLog();
    }

    @Test
    void emptyLogHasZeroLastIndex() {
        assertThat(log.lastIndex()).isEqualTo(0);
        assertThat(log.lastTerm()).isEqualTo(0);
    }

    @Test
    void appendSingleEntry() {
        log.append(new LogEntry(1, 1, "cmd1".getBytes()));
        assertThat(log.lastIndex()).isEqualTo(1);
        assertThat(log.lastTerm()).isEqualTo(1);
    }

    @Test
    void appendMultipleEntries() {
        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(1, 2, "b".getBytes()));
        log.append(new LogEntry(2, 3, "c".getBytes()));

        assertThat(log.lastIndex()).isEqualTo(3);
        assertThat(log.lastTerm()).isEqualTo(2);
    }

    @Test
    void getEntryByIndex() {
        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(2, 2, "b".getBytes()));

        assertThat(log.getEntry(1).term()).isEqualTo(1);
        assertThat(log.getEntry(2).term()).isEqualTo(2);
    }

    @Test
    void getEntryOutOfBoundsReturnsNull() {
        assertThat(log.getEntry(1)).isNull();
        log.append(new LogEntry(1, 1, "a".getBytes()));
        assertThat(log.getEntry(2)).isNull();
        assertThat(log.getEntry(0)).isNull();
    }

    @Test
    void getTermAtIndex() {
        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(3, 2, "b".getBytes()));

        assertThat(log.termAt(1)).isEqualTo(1);
        assertThat(log.termAt(2)).isEqualTo(3);
        assertThat(log.termAt(0)).isEqualTo(0); // sentinel
        assertThat(log.termAt(99)).isEqualTo(0); // out of bounds
    }

    @Test
    void truncateFromIndex() {
        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(1, 2, "b".getBytes()));
        log.append(new LogEntry(2, 3, "c".getBytes()));

        log.truncateFrom(2); // remove entries at index 2 and beyond
        assertThat(log.lastIndex()).isEqualTo(1);
        assertThat(log.getEntry(2)).isNull();
    }

    @Test
    void truncateFromOneRemovesAll() {
        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(1, 2, "b".getBytes()));

        log.truncateFrom(1);
        assertThat(log.lastIndex()).isEqualTo(0);
        assertThat(log.lastTerm()).isEqualTo(0);
    }

    @Test
    void appendEntriesFromLeader() {
        // Simulate receiving entries from a leader
        List<LogEntry> entries = List.of(
                new LogEntry(1, 1, "a".getBytes()),
                new LogEntry(1, 2, "b".getBytes()),
                new LogEntry(2, 3, "c".getBytes())
        );
        log.appendAll(entries);

        assertThat(log.lastIndex()).isEqualTo(3);
        assertThat(log.getEntry(1).term()).isEqualTo(1);
        assertThat(log.getEntry(3).term()).isEqualTo(2);
    }

    @Test
    void getEntriesFromIndex() {
        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(1, 2, "b".getBytes()));
        log.append(new LogEntry(2, 3, "c".getBytes()));

        List<LogEntry> from2 = log.getEntriesFrom(2);
        assertThat(from2).hasSize(2);
        assertThat(from2.get(0).index()).isEqualTo(2);
        assertThat(from2.get(1).index()).isEqualTo(3);
    }

    @Test
    void getEntriesFromBeyondLastReturnsEmpty() {
        log.append(new LogEntry(1, 1, "a".getBytes()));
        assertThat(log.getEntriesFrom(2)).isEmpty();
    }

    @Test
    void commitIndexAdvances() {
        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(1, 2, "b".getBytes()));

        assertThat(log.commitIndex()).isEqualTo(0);

        log.advanceCommitIndex(1);
        assertThat(log.commitIndex()).isEqualTo(1);

        log.advanceCommitIndex(2);
        assertThat(log.commitIndex()).isEqualTo(2);
    }

    @Test
    void commitIndexNeverDecreases() {
        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(1, 2, "b".getBytes()));

        log.advanceCommitIndex(2);
        log.advanceCommitIndex(1); // should be ignored
        assertThat(log.commitIndex()).isEqualTo(2);
    }

    @Test
    void hasMatchingEntryAtPrevLogIndexAndTerm() {
        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(2, 2, "b".getBytes()));

        // Match at (index=1, term=1) — true
        assertThat(log.hasMatchingEntry(1, 1)).isTrue();
        // Match at (index=2, term=2) — true
        assertThat(log.hasMatchingEntry(2, 2)).isTrue();
        // Mismatch: wrong term
        assertThat(log.hasMatchingEntry(2, 1)).isFalse();
        // Sentinel (index=0, term=0) always matches
        assertThat(log.hasMatchingEntry(0, 0)).isTrue();
        // Beyond last index
        assertThat(log.hasMatchingEntry(3, 1)).isFalse();
    }
}
