package turbomq.raft;

import turbomq.common.NodeId;
import turbomq.testing.SimulatedNetwork;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Acceptance tests for Raft leader election.
 *
 * Validates end-to-end election scenarios using the deterministic
 * simulation harness: 3-node and 5-node clusters, split votes,
 * network partitions, and leader re-election after failure.
 */
class LeaderElectionAcceptanceTest {

    private SimulatedNetwork network;
    private List<RaftNode> nodes;

    @BeforeEach
    void setUp() {
        network = new SimulatedNetwork();
        nodes = new ArrayList<>();
    }

    private void createCluster(int size) {
        List<NodeId> allIds = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            allIds.add(NodeId.of(i));
            network.registerNode(NodeId.of(i));
        }

        for (int i = 0; i < size; i++) {
            NodeId id = allIds.get(i);
            List<NodeId> peers = allIds.stream()
                    .filter(p -> !p.equals(id))
                    .toList();
            nodes.add(new RaftNode(id, peers, network::send));
        }
    }

    private void deliverAndProcessAll() {
        network.deliverAll();
        for (RaftNode node : nodes) {
            RaftMessage msg;
            while ((msg = network.receive(node.id())) != null) {
                node.handleMessage(msg);
            }
        }
    }

    private void stabilize() {
        for (int i = 0; i < 10; i++) {
            deliverAndProcessAll();
        }
    }

    private RaftNode findLeader() {
        return nodes.stream()
                .filter(n -> n.state() == RaftState.LEADER)
                .findFirst()
                .orElse(null);
    }

    private long countLeaders() {
        return nodes.stream()
                .filter(n -> n.state() == RaftState.LEADER)
                .count();
    }

    // ========== Tests ==========

    @Test
    void threeNodeClusterElectsExactlyOneLeader() {
        createCluster(3);

        nodes.get(0).onElectionTimeout();
        stabilize();

        assertThat(countLeaders()).isEqualTo(1);
        RaftNode leader = findLeader();
        assertThat(leader).isNotNull();
        assertThat(leader.currentTerm()).isEqualTo(1);
    }

    @Test
    void fiveNodeClusterElectsExactlyOneLeader() {
        createCluster(5);

        nodes.get(2).onElectionTimeout();
        stabilize();

        assertThat(countLeaders()).isEqualTo(1);
        assertThat(findLeader().id()).isEqualTo(NodeId.of(2));
    }

    @Test
    void allFollowersAgreeOnLeader() {
        createCluster(3);

        nodes.get(1).onElectionTimeout();
        stabilize();

        RaftNode leader = findLeader();
        assertThat(leader).isNotNull();

        // All non-leader nodes should recognize the leader
        for (RaftNode node : nodes) {
            if (node.state() != RaftState.LEADER) {
                assertThat(node.leaderId()).isEqualTo(leader.id());
            }
        }
    }

    @Test
    void reElectionAfterLeaderIsolation() {
        createCluster(3);

        // Elect node0 as leader
        nodes.get(0).onElectionTimeout();
        stabilize();
        assertThat(findLeader().id()).isEqualTo(NodeId.of(0));

        // Partition node0 from the cluster
        network.isolateNode(NodeId.of(0));

        // node1 times out and starts election
        nodes.get(1).onElectionTimeout();
        stabilize();

        // A new leader should be elected among the remaining nodes
        long leaders = nodes.stream()
                .filter(n -> !n.id().equals(NodeId.of(0)))
                .filter(n -> n.state() == RaftState.LEADER)
                .count();
        assertThat(leaders).isEqualTo(1);

        // New leader should have a higher term
        RaftNode newLeader = nodes.stream()
                .filter(n -> !n.id().equals(NodeId.of(0)))
                .filter(n -> n.state() == RaftState.LEADER)
                .findFirst().orElseThrow();
        assertThat(newLeader.currentTerm()).isGreaterThan(1);
    }

    @Test
    void splitVoteResolvesOnRetry() {
        createCluster(3);

        // Both node0 and node1 timeout simultaneously (split vote scenario)
        nodes.get(0).onElectionTimeout();
        nodes.get(1).onElectionTimeout();
        stabilize();

        // At least one leader should emerge (possibly after multiple rounds)
        // In practice, one wins the pre-vote or vote; the other steps down
        long leaders = countLeaders();

        if (leaders == 0) {
            // Split vote: retry with node0 only
            nodes.get(0).onElectionTimeout();
            stabilize();
        }

        assertThat(countLeaders()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void leaderWithMostUpToDateLogWinsElection() {
        createCluster(3);

        // Give node2 a more up-to-date log
        nodes.get(2).log().append(new LogEntry(1, 1, "data".getBytes()));

        // node0 tries to become leader with empty log
        nodes.get(0).onElectionTimeout();
        stabilize();

        // node2 should NOT have voted for node0 (log up-to-date check)
        // Now let node2 try
        nodes.get(2).onElectionTimeout();
        stabilize();

        // node2 should win because it has the most up-to-date log
        assertThat(findLeader().id()).isEqualTo(NodeId.of(2));
    }

    @Test
    void atMostOneLeaderPerTerm() {
        createCluster(5);

        // Multiple nodes timeout
        nodes.get(0).onElectionTimeout();
        nodes.get(3).onElectionTimeout();
        stabilize();

        // Check: for any given term, at most one leader
        var leadersByTerm = new java.util.HashMap<Long, List<NodeId>>();
        for (RaftNode node : nodes) {
            if (node.state() == RaftState.LEADER) {
                leadersByTerm.computeIfAbsent(node.currentTerm(), k -> new ArrayList<>())
                        .add(node.id());
            }
        }

        for (var entry : leadersByTerm.entrySet()) {
            assertThat(entry.getValue())
                    .as("Term %d should have at most one leader", entry.getKey())
                    .hasSizeLessThanOrEqualTo(1);
        }
    }
}
