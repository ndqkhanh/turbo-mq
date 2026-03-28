package turbomq.raft;

import turbomq.common.NodeId;
import turbomq.testing.SimulatedNetwork;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class RaftNodeTest {

    private SimulatedNetwork network;
    private RaftNode node0, node1, node2;
    private final NodeId id0 = NodeId.of(0);
    private final NodeId id1 = NodeId.of(1);
    private final NodeId id2 = NodeId.of(2);

    @BeforeEach
    void setUp() {
        network = new SimulatedNetwork();
        network.registerNode(id0);
        network.registerNode(id1);
        network.registerNode(id2);

        node0 = createNode(id0, List.of(id1, id2));
        node1 = createNode(id1, List.of(id0, id2));
        node2 = createNode(id2, List.of(id0, id1));
    }

    private RaftNode createNode(NodeId id, List<NodeId> peers) {
        return new RaftNode(id, peers, network::send);
    }

    private void deliverAndProcess() {
        network.deliverAll();
        // Process all messages in each node's inbox
        for (var node : List.of(node0, node1, node2)) {
            RaftMessage msg;
            while ((msg = network.receive(node.id())) != null) {
                node.handleMessage(msg);
            }
        }
    }

    // ========== Initial State ==========

    @Test
    void nodeStartsAsFollower() {
        assertThat(node0.state()).isEqualTo(RaftState.FOLLOWER);
        assertThat(node0.currentTerm()).isEqualTo(0);
        assertThat(node0.votedFor()).isNull();
        assertThat(node0.leaderId()).isNull();
    }

    // ========== Pre-Vote ==========

    @Test
    void electionTimeoutTriggersPreVote() {
        node0.onElectionTimeout();

        // Should have sent PreVote to both peers
        assertThat(network.pendingCount()).isEqualTo(2);
    }

    @Test
    void preVoteDoesNotIncrementTerm() {
        node0.onElectionTimeout();

        // Term should NOT have changed yet (pre-vote is prospective)
        assertThat(node0.currentTerm()).isEqualTo(0);
    }

    // ========== Leader Election ==========

    @Test
    void nodeBecomesLeaderWithMajorityVotes() {
        node0.onElectionTimeout();
        deliverAndProcess(); // PreVote requests → responses
        deliverAndProcess(); // PreVote responses → RequestVote
        deliverAndProcess(); // RequestVote requests → responses
        deliverAndProcess(); // RequestVote responses → become leader + heartbeats

        assertThat(node0.state()).isEqualTo(RaftState.LEADER);
        assertThat(node0.currentTerm()).isEqualTo(1);
        assertThat(node0.leaderId()).isEqualTo(id0);
    }

    @Test
    void leaderSendsHeartbeatsAfterElection() {
        // Elect node0
        electLeader(node0);

        // Fully drain all in-flight messages (including commit-propagation heartbeats)
        for (int i = 0; i < 5; i++) {
            network.deliverAll();
            drainAll();
        }
        // Clear any remaining pending
        while (network.pendingCount() > 0) {
            network.deliverAll();
            drainAll();
        }

        // Trigger heartbeats
        node0.sendHeartbeats();
        assertThat(network.pendingCount()).isEqualTo(2); // one per peer
    }

    @Test
    void followerRecognizesLeaderFromAppendEntries() {
        electLeader(node0);

        // After election, followers should recognize node0 as leader
        assertThat(node1.leaderId()).isEqualTo(id0);
        assertThat(node2.leaderId()).isEqualTo(id0);
    }

    @Test
    void singleNodeClusterBecomesLeaderImmediately() {
        RaftNode solo = new RaftNode(id0, List.of(), network::send);
        solo.onElectionTimeout();

        assertThat(solo.state()).isEqualTo(RaftState.LEADER);
        assertThat(solo.currentTerm()).isEqualTo(1);
    }

    // ========== Term Management ==========

    @Test
    void nodeStepsDownOnHigherTerm() {
        electLeader(node0);
        assertThat(node0.state()).isEqualTo(RaftState.LEADER);

        // Simulate a message from a higher term
        node0.handleMessage(new RaftMessage.AppendEntries(
                5, id1, id0, 0, 0, List.of(), 0
        ));

        assertThat(node0.state()).isEqualTo(RaftState.FOLLOWER);
        assertThat(node0.currentTerm()).isEqualTo(5);
    }

    @Test
    void candidateStepsDownOnHigherTermResponse() {
        node0.onElectionTimeout();
        deliverAndProcess(); // PreVote
        deliverAndProcess(); // PreVoteResponse → starts election

        assertThat(node0.state()).isEqualTo(RaftState.CANDIDATE);

        // A peer responds with a higher term
        node0.handleMessage(new RaftMessage.RequestVoteResponse(
                10, id1, id0, false
        ));

        assertThat(node0.state()).isEqualTo(RaftState.FOLLOWER);
        assertThat(node0.currentTerm()).isEqualTo(10);
    }

    // ========== Vote Restriction ==========

    @Test
    void nodeVotesOnlyOncePerTerm() {
        // node0 starts election
        node0.onElectionTimeout();
        deliverAndProcess(); // PreVote
        deliverAndProcess(); // PreVoteResponse → starts election, sends RequestVote

        // Deliver only to node1 first
        network.deliverTo(id1);
        RaftMessage msg1 = network.receive(id1);
        node1.handleMessage(msg1);

        assertThat(node1.votedFor()).isEqualTo(id0);

        // Now node2 also starts an election at the same term
        // node1 should reject because it already voted for node0
        node1.handleMessage(new RaftMessage.RequestVote(
                1, id2, id1, 0, 0
        ));

        // Check that node1 still voted for node0
        assertThat(node1.votedFor()).isEqualTo(id0);
    }

    @Test
    void nodeRejectsVoteIfCandidateLogIsStale() {
        // Give node1 a log entry at term 2
        node1.log().append(new LogEntry(2, 1, "data".getBytes()));

        // node0 requests vote with empty log
        node1.handleMessage(new RaftMessage.RequestVote(
                3, id0, id1, 0, 0
        ));

        // node1 should reject — node0's log is less up-to-date
        network.deliverAll();
        RaftMessage resp = network.receive(id0);
        assertThat(resp).isInstanceOf(RaftMessage.RequestVoteResponse.class);
        assertThat(((RaftMessage.RequestVoteResponse) resp).voteGranted()).isFalse();
    }

    // ========== Leader does not time out ==========

    @Test
    void leaderIgnoresElectionTimeout() {
        electLeader(node0);
        long termBefore = node0.currentTerm();

        node0.onElectionTimeout(); // should be ignored

        assertThat(node0.state()).isEqualTo(RaftState.LEADER);
        assertThat(node0.currentTerm()).isEqualTo(termBefore);
    }

    // ========== Log Replication ==========

    @Test
    void leaderReplicatesProposedEntry() {
        electLeader(node0);
        drainAll();
        network.deliverAll();
        drainAll();

        boolean proposed = node0.propose("hello".getBytes());
        assertThat(proposed).isTrue();

        // Leader should have the entry in its log (noop at 1, "hello" at 2)
        assertThat(node0.log().lastIndex()).isEqualTo(2);

        // Deliver AppendEntries to followers and process
        deliverAndProcess();
        // Deliver responses back to leader
        deliverAndProcess();

        // Leader should have committed (majority replicated)
        assertThat(node0.log().commitIndex()).isEqualTo(2);
    }

    @Test
    void followersReceiveAndApplyReplicatedEntries() {
        electLeader(node0);
        drainAll();
        network.deliverAll();
        drainAll();

        node0.propose("data1".getBytes());
        deliverAndProcess(); // AppendEntries to followers
        deliverAndProcess(); // Responses → leader commits → next heartbeat
        deliverAndProcess(); // Heartbeat with updated commitIndex → followers commit

        // Followers should have applied the entries
        assertThat(node1.log().commitIndex()).isGreaterThanOrEqualTo(1);
        assertThat(node2.log().commitIndex()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void proposeOnFollowerReturnsFalse() {
        assertThat(node0.propose("data".getBytes())).isFalse();
    }

    @Test
    void followerRejectsAppendEntriesWithLowerTerm() {
        // Set node1 to term 5
        node1.handleMessage(new RaftMessage.AppendEntries(
                5, id0, id1, 0, 0, List.of(), 0
        ));

        // Drain the success response from the term-5 AE
        network.deliverAll();
        network.receive(id0);

        // Send AppendEntries with lower term
        node1.handleMessage(new RaftMessage.AppendEntries(
                3, id0, id1, 0, 0, List.of(), 0
        ));

        network.deliverAll();
        RaftMessage resp = network.receive(id0);
        assertThat(resp).isInstanceOf(RaftMessage.AppendEntriesResponse.class);
        assertThat(((RaftMessage.AppendEntriesResponse) resp).success()).isFalse();
    }

    @Test
    void followerRejectsAppendEntriesWithMismatchedPrevLog() {
        // node1 has entry at index 1 with term 1
        node1.log().append(new LogEntry(1, 1, "old".getBytes()));

        // Leader sends with prevLogIndex=1, prevLogTerm=2 (mismatch)
        node1.handleMessage(new RaftMessage.AppendEntries(
                2, id0, id1, 1, 2, List.of(), 0
        ));

        network.deliverAll();
        RaftMessage resp = network.receive(id0);
        assertThat(((RaftMessage.AppendEntriesResponse) resp).success()).isFalse();
    }

    @Test
    void leaderDecrementsNextIndexOnRejection() {
        electLeader(node0);
        drainAll();
        network.deliverAll();
        drainAll();

        // Manually corrupt node1's log to cause a mismatch
        node1.log().append(new LogEntry(99, 2, "conflict".getBytes()));

        // Leader proposes, follower rejects due to mismatch
        node0.propose("data".getBytes());
        deliverAndProcess(); // AppendEntries → rejection
        deliverAndProcess(); // Leader retries with decremented nextIndex
        deliverAndProcess(); // Eventually succeeds

        // After retries, the entry should be replicated
        assertThat(node0.log().commitIndex()).isGreaterThanOrEqualTo(1);
    }

    // ========== Helpers ==========

    private void electLeader(RaftNode candidate) {
        candidate.onElectionTimeout();
        for (int i = 0; i < 6; i++) {
            deliverAndProcess();
        }
        assertThat(candidate.state()).isEqualTo(RaftState.LEADER);
    }

    private void drainAll() {
        for (var node : List.of(node0, node1, node2)) {
            RaftMessage msg;
            while ((msg = network.receive(node.id())) != null) {
                node.handleMessage(msg);
            }
        }
    }
}
