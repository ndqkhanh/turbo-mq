package turbomq.raft;

import turbomq.common.NodeId;

import java.util.List;

/**
 * Sealed interface for all Raft protocol messages.
 * Each partition's Raft group exchanges these messages independently.
 *
 * Using Java 21 sealed interfaces + records for exhaustive pattern matching.
 */
public sealed interface RaftMessage {

    long term();
    NodeId from();
    NodeId to();

    // ========== Leader Election ==========

    /**
     * Sent by candidate to request votes during leader election.
     * Includes lastLogIndex/lastLogTerm for the up-to-date check (Raft §5.4.1).
     */
    record RequestVote(
            long term,
            NodeId from,
            NodeId to,
            long lastLogIndex,
            long lastLogTerm
    ) implements RaftMessage {}

    record RequestVoteResponse(
            long term,
            NodeId from,
            NodeId to,
            boolean voteGranted
    ) implements RaftMessage {}

    /**
     * Pre-vote: a candidate checks if it could win an election before
     * incrementing its term. Prevents disruption from partitioned nodes (Raft §9.6).
     */
    record PreVote(
            long term,
            NodeId from,
            NodeId to,
            long lastLogIndex,
            long lastLogTerm
    ) implements RaftMessage {}

    record PreVoteResponse(
            long term,
            NodeId from,
            NodeId to,
            boolean voteGranted
    ) implements RaftMessage {}

    // ========== Log Replication ==========

    /**
     * Sent by leader to replicate log entries and serve as heartbeat.
     * An empty entries list functions as a heartbeat.
     */
    record AppendEntries(
            long term,
            NodeId from,
            NodeId to,
            long prevLogIndex,
            long prevLogTerm,
            List<LogEntry> entries,
            long leaderCommit
    ) implements RaftMessage {}

    record AppendEntriesResponse(
            long term,
            NodeId from,
            NodeId to,
            boolean success,
            long matchIndex
    ) implements RaftMessage {}

    // ========== Snapshot Transfer ==========

    /**
     * Sent by leader to transfer a snapshot to a follower that is too far behind.
     * Raft §7: if nextIndex for a follower falls behind the leader's log start,
     * the leader sends its snapshot instead of log entries.
     */
    record InstallSnapshot(
            long term,
            NodeId from,
            NodeId to,
            long lastIncludedIndex,
            long lastIncludedTerm,
            long offset,
            byte[] data,
            boolean done
    ) implements RaftMessage {}

    record InstallSnapshotResponse(
            long term,
            NodeId from,
            NodeId to
    ) implements RaftMessage {}
}
