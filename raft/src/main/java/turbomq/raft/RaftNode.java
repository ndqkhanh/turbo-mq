package turbomq.raft;

import turbomq.common.NodeId;

import java.util.*;
import java.util.function.Consumer;

/**
 * Single Raft node state machine implementing leader election and log replication.
 *
 * Processes incoming RaftMessages and produces outgoing messages via a
 * configurable send function. Designed for deterministic testing: no threads,
 * no timers — the caller drives ticks and message delivery.
 *
 * Implements:
 * - Leader election with pre-vote (Raft §5.2, §9.6)
 * - Log replication with consistency checks (Raft §5.3)
 * - Commit index advancement via majority match (Raft §5.3, §5.4)
 * - Leader step-down on higher term discovery
 */
public final class RaftNode {

    private final NodeId id;
    private final List<NodeId> peers;
    private final RaftLog log;
    private final Consumer<RaftMessage> sendFn;

    // ========== Persistent state (would be persisted to disk in production) ==========
    private long currentTerm = 0;
    private NodeId votedFor = null;

    // ========== Volatile state ==========
    private RaftState state = RaftState.FOLLOWER;
    private NodeId leaderId = null;

    // ========== Election state ==========
    private int votesReceived = 0;
    private int preVotesReceived = 0;
    private boolean electionTimedOut = false;

    // ========== Leader state (reinitialized on election) ==========
    /** For each peer: index of the next log entry to send. */
    private final Map<NodeId, Long> nextIndex = new HashMap<>();
    /** For each peer: highest log entry known to be replicated. */
    private final Map<NodeId, Long> matchIndex = new HashMap<>();
    /** For each peer: pending snapshot lastIncludedIndex (set when InstallSnapshot sent). */
    private final Map<NodeId, Long> pendingSnapshotIndex = new HashMap<>();

    // ========== Applied entries callback ==========
    private final List<LogEntry> appliedEntries = new ArrayList<>();

    public RaftNode(NodeId id, List<NodeId> peers, Consumer<RaftMessage> sendFn) {
        this.id = id;
        this.peers = List.copyOf(peers);
        this.log = new RaftLog();
        this.sendFn = sendFn;
    }

    // ========== Public accessors ==========

    public NodeId id() { return id; }
    public long currentTerm() { return currentTerm; }
    public RaftState state() { return state; }
    public NodeId leaderId() { return leaderId; }
    public NodeId votedFor() { return votedFor; }
    public RaftLog log() { return log; }
    public List<LogEntry> appliedEntries() { return Collections.unmodifiableList(appliedEntries); }

    public int quorumSize() {
        return (peers.size() + 1) / 2 + 1;
    }

    // ========== Message handling ==========

    /**
     * Process an incoming Raft message. Dispatches to the appropriate handler
     * based on message type using pattern matching.
     */
    public void handleMessage(RaftMessage message) {
        // PreVote/PreVoteResponse use a prospective term — they must NOT
        // trigger stepDown. That's the whole point of pre-vote (Raft §9.6):
        // avoid disrupting the cluster with unnecessary term increments.
        boolean isPreVoteMessage = message instanceof RaftMessage.PreVote
                || message instanceof RaftMessage.PreVoteResponse;

        // Rule: if RPC request or response contains term > currentTerm,
        // set currentTerm = T, convert to follower (Raft §5.1)
        if (!isPreVoteMessage && message.term() > currentTerm) {
            stepDown(message.term());
        }

        switch (message) {
            case RaftMessage.PreVote pv -> handlePreVote(pv);
            case RaftMessage.PreVoteResponse pvr -> handlePreVoteResponse(pvr);
            case RaftMessage.RequestVote rv -> handleRequestVote(rv);
            case RaftMessage.RequestVoteResponse rvr -> handleRequestVoteResponse(rvr);
            case RaftMessage.AppendEntries ae -> handleAppendEntries(ae);
            case RaftMessage.AppendEntriesResponse aer -> handleAppendEntriesResponse(aer);
            case RaftMessage.InstallSnapshot is -> handleInstallSnapshot(is);
            case RaftMessage.InstallSnapshotResponse isr -> handleInstallSnapshotResponse(isr);
        }
    }

    // ========== Election ==========

    /**
     * Called when election timeout fires. Starts a pre-vote phase
     * to avoid disrupting the cluster with unnecessary term increments.
     */
    public void onElectionTimeout() {
        if (state == RaftState.LEADER) return; // leaders don't time out

        startPreVote();
    }

    private void startPreVote() {
        preVotesReceived = 1; // vote for self
        electionTimedOut = true;

        for (NodeId peer : peers) {
            sendFn.accept(new RaftMessage.PreVote(
                    currentTerm + 1, // prospective term
                    id, peer,
                    log.lastIndex(), log.lastTerm()
            ));
        }

        // Single-node cluster: immediately become leader
        if (peers.isEmpty()) {
            startElection();
        }
    }

    private void handlePreVote(RaftMessage.PreVote pv) {
        // Grant pre-vote if:
        // 1. The candidate's prospective term >= our current term
        // 2. The candidate's log is at least as up-to-date as ours
        // 3. We believe no leader is active (our election has timed out OR we haven't heard from leader)
        boolean logOk = isLogAtLeastAsUpToDate(pv.lastLogTerm(), pv.lastLogIndex());
        boolean termOk = pv.term() >= currentTerm;
        boolean granted = termOk && logOk;

        sendFn.accept(new RaftMessage.PreVoteResponse(
                currentTerm, id, pv.from(), granted
        ));
    }

    private void handlePreVoteResponse(RaftMessage.PreVoteResponse pvr) {
        if (!electionTimedOut) return; // stale response

        if (pvr.voteGranted()) {
            preVotesReceived++;
            if (preVotesReceived >= quorumSize()) {
                startElection();
            }
        }
    }

    private void startElection() {
        currentTerm++;
        state = RaftState.CANDIDATE;
        votedFor = id;
        votesReceived = 1; // vote for self
        leaderId = null;
        electionTimedOut = false;

        for (NodeId peer : peers) {
            sendFn.accept(new RaftMessage.RequestVote(
                    currentTerm, id, peer,
                    log.lastIndex(), log.lastTerm()
            ));
        }

        // Single-node cluster: immediately become leader
        if (peers.isEmpty()) {
            becomeLeader();
        }
    }

    private void handleRequestVote(RaftMessage.RequestVote rv) {
        boolean granted = false;

        if (rv.term() >= currentTerm) {
            boolean logOk = isLogAtLeastAsUpToDate(rv.lastLogTerm(), rv.lastLogIndex());
            boolean canVote = (votedFor == null || votedFor.equals(rv.from()));

            if (canVote && logOk) {
                votedFor = rv.from();
                granted = true;
                // Reset election timeout when granting a vote
                leaderId = null;
            }
        }

        sendFn.accept(new RaftMessage.RequestVoteResponse(
                currentTerm, id, rv.from(), granted
        ));
    }

    private void handleRequestVoteResponse(RaftMessage.RequestVoteResponse rvr) {
        if (state != RaftState.CANDIDATE) return;
        if (rvr.term() != currentTerm) return; // stale

        if (rvr.voteGranted()) {
            votesReceived++;
            if (votesReceived >= quorumSize()) {
                becomeLeader();
            }
        }
    }

    private void becomeLeader() {
        state = RaftState.LEADER;
        leaderId = id;

        // Reinitialize leader state (Raft §5.3)
        for (NodeId peer : peers) {
            nextIndex.put(peer, log.lastIndex() + 1);
            matchIndex.put(peer, 0L);
        }

        // Append a no-op entry to commit entries from previous terms (Raft §5.4.2)
        LogEntry noop = LogEntry.noop(currentTerm, log.lastIndex() + 1);
        log.append(noop);

        // Send initial heartbeats with the no-op
        sendHeartbeats();
    }

    // ========== Log Replication ==========

    /**
     * Propose a new entry to the replicated log. Only valid on the leader.
     *
     * @return true if the entry was proposed, false if not leader
     */
    public boolean propose(byte[] data) {
        if (state != RaftState.LEADER) return false;

        LogEntry entry = new LogEntry(currentTerm, log.lastIndex() + 1, data);
        log.append(entry);

        // Replicate to all peers
        for (NodeId peer : peers) {
            sendAppendEntries(peer);
        }
        return true;
    }

    /**
     * Send an InstallSnapshot RPC to a specific peer.
     * Used when a follower is too far behind to catch up via log replication.
     */
    public void sendInstallSnapshot(NodeId peer, long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
        if (state != RaftState.LEADER) return;

        pendingSnapshotIndex.put(peer, lastIncludedIndex);
        sendFn.accept(new RaftMessage.InstallSnapshot(
                currentTerm, id, peer,
                lastIncludedIndex, lastIncludedTerm,
                0, data, true
        ));
    }

    /** Send heartbeats (empty or with entries) to all peers. */
    public void sendHeartbeats() {
        if (state != RaftState.LEADER) return;

        for (NodeId peer : peers) {
            sendAppendEntries(peer);
        }
    }

    private void sendAppendEntries(NodeId peer) {
        long nextIdx = nextIndex.getOrDefault(peer, log.lastIndex() + 1);
        long prevLogIndex = nextIdx - 1;
        long prevLogTerm = log.termAt(prevLogIndex);
        List<LogEntry> entries = log.getEntriesFrom(nextIdx);

        sendFn.accept(new RaftMessage.AppendEntries(
                currentTerm, id, peer,
                prevLogIndex, prevLogTerm,
                entries, log.commitIndex()
        ));
    }

    private void handleAppendEntries(RaftMessage.AppendEntries ae) {
        // Reject if term < currentTerm (Raft §5.1)
        if (ae.term() < currentTerm) {
            sendFn.accept(new RaftMessage.AppendEntriesResponse(
                    currentTerm, id, ae.from(), false, 0
            ));
            return;
        }

        // Valid AppendEntries from current leader
        state = RaftState.FOLLOWER;
        leaderId = ae.from();
        electionTimedOut = false;

        // Consistency check: log must contain entry at prevLogIndex with prevLogTerm
        if (!log.hasMatchingEntry(ae.prevLogIndex(), ae.prevLogTerm())) {
            sendFn.accept(new RaftMessage.AppendEntriesResponse(
                    currentTerm, id, ae.from(), false, log.lastIndex()
            ));
            return;
        }

        // Append new entries, handling conflicts (Raft §5.3)
        if (!ae.entries().isEmpty()) {
            for (LogEntry entry : ae.entries()) {
                long existingTerm = log.termAt(entry.index());
                if (existingTerm != 0 && existingTerm != entry.term()) {
                    // Conflict: truncate from this point
                    log.truncateFrom(entry.index());
                }
                if (entry.index() > log.lastIndex()) {
                    log.append(entry);
                }
            }
        }

        // Advance commit index
        if (ae.leaderCommit() > log.commitIndex()) {
            log.advanceCommitIndex(Math.min(ae.leaderCommit(), log.lastIndex()));
            applyCommittedEntries();
        }

        sendFn.accept(new RaftMessage.AppendEntriesResponse(
                currentTerm, id, ae.from(), true, log.lastIndex()
        ));
    }

    private void handleAppendEntriesResponse(RaftMessage.AppendEntriesResponse aer) {
        if (state != RaftState.LEADER) return;
        if (aer.term() != currentTerm) return;

        if (aer.success()) {
            // Update nextIndex and matchIndex for this peer
            matchIndex.put(aer.from(), aer.matchIndex());
            nextIndex.put(aer.from(), aer.matchIndex() + 1);

            // Try to advance commit index
            advanceCommitIndex();
        } else {
            // Decrement nextIndex and retry (Raft §5.3)
            long next = nextIndex.getOrDefault(aer.from(), 1L);
            nextIndex.put(aer.from(), Math.max(1, next - 1));
            sendAppendEntries(aer.from());
        }
    }

    /**
     * Advance commit index if a majority of matchIndex values >= N
     * for some N > commitIndex, and log[N].term == currentTerm (Raft §5.4.2).
     */
    private void advanceCommitIndex() {
        long oldCommitIndex = log.commitIndex();

        for (long n = log.lastIndex(); n > log.commitIndex(); n--) {
            if (log.termAt(n) != currentTerm) continue;

            int replicatedCount = 1; // leader has it
            for (NodeId peer : peers) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    replicatedCount++;
                }
            }
            if (replicatedCount >= quorumSize()) {
                log.advanceCommitIndex(n);
                applyCommittedEntries();
                break;
            }
        }

        // If commit index advanced, send heartbeats to propagate leaderCommit to followers
        if (log.commitIndex() > oldCommitIndex) {
            sendHeartbeats();
        }
    }

    /** Apply entries between lastApplied and commitIndex to the state machine. */
    private void applyCommittedEntries() {
        while (log.lastApplied() < log.commitIndex()) {
            long nextApply = log.lastApplied() + 1;
            LogEntry entry = log.getEntry(nextApply);
            if (entry != null) {
                appliedEntries.add(entry);
            }
            log.advanceLastApplied(nextApply);
        }
    }

    // ========== Snapshot Transfer ==========

    /**
     * Handle InstallSnapshot RPC from leader (Raft §7).
     * If term is current, install the snapshot and reset log state.
     */
    private void handleInstallSnapshot(RaftMessage.InstallSnapshot is) {
        if (is.term() < currentTerm) {
            // Reply with current term so leader can step down
            sendFn.accept(new RaftMessage.InstallSnapshotResponse(
                    currentTerm, id, is.from()));
            return;
        }

        // Valid snapshot from current leader
        state = RaftState.FOLLOWER;
        leaderId = is.from();
        electionTimedOut = false;

        if (is.done()) {
            // Install the snapshot into the log
            log.installSnapshot(is.lastIncludedIndex(), is.lastIncludedTerm());
        }

        sendFn.accept(new RaftMessage.InstallSnapshotResponse(
                currentTerm, id, is.from()));
    }

    /**
     * Handle InstallSnapshotResponse from follower.
     * Advance nextIndex for the follower past the snapshot.
     */
    private void handleInstallSnapshotResponse(RaftMessage.InstallSnapshotResponse isr) {
        if (state != RaftState.LEADER) return;
        if (isr.term() != currentTerm) return;

        Long snapIdx = pendingSnapshotIndex.remove(isr.from());
        if (snapIdx != null) {
            nextIndex.put(isr.from(), snapIdx + 1);
            matchIndex.put(isr.from(), snapIdx);
        }
    }

    // ========== Helpers ==========

    /** Step down to follower at the given term. */
    private void stepDown(long newTerm) {
        currentTerm = newTerm;
        state = RaftState.FOLLOWER;
        votedFor = null;
        votesReceived = 0;
        preVotesReceived = 0;
        electionTimedOut = false;
        leaderId = null;
    }

    /**
     * Raft up-to-date check (§5.4.1):
     * A candidate's log is at least as up-to-date if its last entry
     * has a higher term, or same term with >= index.
     */
    private boolean isLogAtLeastAsUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long myLastTerm = log.lastTerm();
        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= log.lastIndex();
    }
}
