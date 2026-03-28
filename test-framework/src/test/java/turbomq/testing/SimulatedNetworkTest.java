package turbomq.testing;

import turbomq.common.NodeId;
import turbomq.raft.RaftMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class SimulatedNetworkTest {

    private SimulatedNetwork network;
    private final NodeId node0 = NodeId.of(0);
    private final NodeId node1 = NodeId.of(1);
    private final NodeId node2 = NodeId.of(2);

    @BeforeEach
    void setUp() {
        network = new SimulatedNetwork();
        network.registerNode(node0);
        network.registerNode(node1);
        network.registerNode(node2);
    }

    @Test
    void sentMessageIsNotDeliveredUntilExplicit() {
        network.send(voteRequest(node0, node1, 1));
        assertThat(network.inboxSize(node1)).isEqualTo(0);
        assertThat(network.pendingCount()).isEqualTo(1);
    }

    @Test
    void deliverAllPutsMessagesInCorrectInboxes() {
        network.send(voteRequest(node0, node1, 1));
        network.send(voteRequest(node0, node2, 1));

        int delivered = network.deliverAll();
        assertThat(delivered).isEqualTo(2);
        assertThat(network.inboxSize(node1)).isEqualTo(1);
        assertThat(network.inboxSize(node2)).isEqualTo(1);
        assertThat(network.inboxSize(node0)).isEqualTo(0);
    }

    @Test
    void receiveReturnsMessagesInOrder() {
        network.send(voteRequest(node0, node1, 1));
        network.send(heartbeat(node0, node1, 2));
        network.deliverAll();

        RaftMessage first = network.receive(node1);
        assertThat(first).isInstanceOf(RaftMessage.RequestVote.class);
        assertThat(first.term()).isEqualTo(1);

        RaftMessage second = network.receive(node1);
        assertThat(second).isInstanceOf(RaftMessage.AppendEntries.class);
        assertThat(second.term()).isEqualTo(2);

        assertThat(network.receive(node1)).isNull();
    }

    @Test
    void receiveAllDrainsInbox() {
        network.send(voteRequest(node0, node1, 1));
        network.send(voteRequest(node2, node1, 2));
        network.deliverAll();

        List<RaftMessage> all = network.receiveAll(node1);
        assertThat(all).hasSize(2);
        assertThat(network.inboxSize(node1)).isEqualTo(0);
    }

    @Test
    void deliverToOnlyDeliversToTargetNode() {
        network.send(voteRequest(node0, node1, 1));
        network.send(voteRequest(node0, node2, 1));

        int delivered = network.deliverTo(node1);
        assertThat(delivered).isEqualTo(1);
        assertThat(network.inboxSize(node1)).isEqualTo(1);
        assertThat(network.inboxSize(node2)).isEqualTo(0);
        assertThat(network.pendingCount()).isEqualTo(1); // node2's message still pending
    }

    @Test
    void isolateNodeDropsAllMessagesToAndFrom() {
        network.isolateNode(node1);

        network.send(voteRequest(node0, node1, 1)); // to isolated
        network.send(voteRequest(node1, node0, 1)); // from isolated
        network.send(voteRequest(node0, node2, 1)); // unaffected

        int delivered = network.deliverAll();
        assertThat(delivered).isEqualTo(1); // only node0→node2
        assertThat(network.inboxSize(node1)).isEqualTo(0);
        assertThat(network.inboxSize(node0)).isEqualTo(0);
        assertThat(network.inboxSize(node2)).isEqualTo(1);
    }

    @Test
    void partitionNodesBidirectional() {
        network.partitionNodes(node0, node1);

        network.send(voteRequest(node0, node1, 1));
        network.send(voteRequest(node1, node0, 1));
        network.send(voteRequest(node0, node2, 1)); // unaffected
        network.send(voteRequest(node1, node2, 1)); // unaffected

        int delivered = network.deliverAll();
        assertThat(delivered).isEqualTo(2);
        assertThat(network.inboxSize(node0)).isEqualTo(0);
        assertThat(network.inboxSize(node1)).isEqualTo(0);
        assertThat(network.inboxSize(node2)).isEqualTo(2);
    }

    @Test
    void dropMessagesOneWay() {
        network.dropMessages(node0, node1);

        network.send(voteRequest(node0, node1, 1)); // dropped
        network.send(voteRequest(node1, node0, 1)); // NOT dropped (reverse direction)

        int delivered = network.deliverAll();
        assertThat(delivered).isEqualTo(1);
        assertThat(network.inboxSize(node0)).isEqualTo(1);
        assertThat(network.inboxSize(node1)).isEqualTo(0);
    }

    @Test
    void healAllRestoresConnectivity() {
        network.isolateNode(node1);
        network.healAll();

        network.send(voteRequest(node0, node1, 1));
        network.deliverAll();
        assertThat(network.inboxSize(node1)).isEqualTo(1);
    }

    @Test
    void resetClearsEverything() {
        network.send(voteRequest(node0, node1, 1));
        network.deliverAll();
        network.isolateNode(node2);

        network.reset();
        assertThat(network.pendingCount()).isEqualTo(0);
        assertThat(network.inboxSize(node1)).isEqualTo(0);

        // Drop rules also cleared
        network.send(voteRequest(node0, node2, 1));
        network.deliverAll();
        assertThat(network.inboxSize(node2)).isEqualTo(1);
    }

    // ========== Helpers ==========

    private RaftMessage.RequestVote voteRequest(NodeId from, NodeId to, long term) {
        return new RaftMessage.RequestVote(term, from, to, 0, 0);
    }

    private RaftMessage.AppendEntries heartbeat(NodeId from, NodeId to, long term) {
        return new RaftMessage.AppendEntries(term, from, to, 0, 0, List.of(), 0);
    }
}
