package turbomq.raft;

import turbomq.common.NodeId;
import turbomq.testing.SimulatedNetwork;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Acceptance tests for Raft log replication.
 *
 * Validates that proposed entries are replicated to a majority,
 * committed, and applied across the cluster. Tests include
 * normal replication, follower catch-up, and partition recovery.
 */
class LogReplicationAcceptanceTest {

    private SimulatedNetwork network;
    private List<RaftNode> nodes;

    @BeforeEach
    void setUp() {
        network = new SimulatedNetwork();
        nodes = new ArrayList<>();
        createCluster(3);
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

    private RaftNode electLeader(int nodeIndex) {
        nodes.get(nodeIndex).onElectionTimeout();
        stabilize();
        assertThat(nodes.get(nodeIndex).state()).isEqualTo(RaftState.LEADER);
        return nodes.get(nodeIndex);
    }

    // ========== Tests ==========

    @Test
    void proposedEntryIsCommittedAcrossCluster() {
        RaftNode leader = electLeader(0);

        leader.propose("message-1".getBytes());
        stabilize();

        // All nodes should have committed the entry
        for (RaftNode node : nodes) {
            assertThat(node.log().commitIndex())
                    .as("Node %s commit index", node.id())
                    .isGreaterThanOrEqualTo(2); // noop + message-1
        }
    }

    @Test
    void multipleProposalsReplicateInOrder() {
        RaftNode leader = electLeader(0);

        leader.propose("msg-A".getBytes());
        leader.propose("msg-B".getBytes());
        leader.propose("msg-C".getBytes());
        stabilize();

        // Leader log: noop(1) + A(2) + B(3) + C(4)
        assertThat(leader.log().lastIndex()).isEqualTo(4);
        assertThat(leader.log().commitIndex()).isEqualTo(4);

        // Followers should have all entries committed
        for (RaftNode node : nodes) {
            if (node.state() != RaftState.LEADER) {
                assertThat(node.log().commitIndex())
                        .as("Node %s commit index", node.id())
                        .isEqualTo(4);
            }
        }
    }

    @Test
    void committedEntriesAreAppliedToStateMachine() {
        RaftNode leader = electLeader(0);

        leader.propose("apply-me".getBytes());
        stabilize();

        // Leader should have applied entries (noop + apply-me)
        assertThat(leader.appliedEntries()).hasSizeGreaterThanOrEqualTo(2);

        // Followers should also apply
        for (RaftNode node : nodes) {
            assertThat(node.appliedEntries())
                    .as("Node %s applied entries", node.id())
                    .hasSizeGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void partitionedFollowerCatchesUpAfterHealing() {
        RaftNode leader = electLeader(0);

        // Propose an entry while all nodes are connected
        leader.propose("before-partition".getBytes());
        stabilize();

        // Partition node2
        network.isolateNode(NodeId.of(2));

        // Propose more entries (only leader + node1 see them)
        leader.propose("during-partition".getBytes());
        stabilize();

        // node2 should be behind
        assertThat(nodes.get(2).log().commitIndex()).isLessThan(leader.log().commitIndex());

        // Heal the partition
        network.healAll();

        // Leader sends heartbeats, node2 catches up
        leader.sendHeartbeats();
        stabilize();

        // node2 should now be caught up
        assertThat(nodes.get(2).log().lastIndex()).isEqualTo(leader.log().lastIndex());
        assertThat(nodes.get(2).log().commitIndex()).isEqualTo(leader.log().commitIndex());
    }

    @Test
    void entryNotCommittedWithoutMajority() {
        RaftNode leader = electLeader(0);
        stabilize();

        // Partition both followers
        network.isolateNode(NodeId.of(1));
        network.isolateNode(NodeId.of(2));

        // Propose — leader appends locally but cannot get majority
        leader.propose("lonely".getBytes());
        stabilize();

        // Leader has the entry in its log but should NOT have committed it
        // (only 1 out of 3 nodes have it — no majority)
        // The noop is committed from the election (when nodes were connected)
        // but the new entry should not be committed
        long noopCommit = 1; // noop was committed during election
        assertThat(leader.log().lastIndex()).isGreaterThan(noopCommit);
        // commitIndex should not advance beyond what was committed before partition
        assertThat(leader.log().commitIndex()).isEqualTo(noopCommit);
    }
}
