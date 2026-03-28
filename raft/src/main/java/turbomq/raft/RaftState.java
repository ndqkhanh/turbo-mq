package turbomq.raft;

/**
 * Raft node states per the Raft paper (Ongaro & Ousterhout, 2014).
 */
public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
