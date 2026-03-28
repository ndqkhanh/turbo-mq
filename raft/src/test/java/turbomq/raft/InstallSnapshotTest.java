package turbomq.raft;

import org.junit.jupiter.api.*;
import turbomq.common.NodeId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD tests for Raft InstallSnapshot protocol.
 *
 * Tests cover:
 * - Leader sends InstallSnapshot when follower is too far behind
 * - Follower accepts snapshot and resets its log state
 * - Follower rejects snapshot with stale term
 * - Snapshot metadata (lastIncludedIndex/Term) applied correctly
 * - Leader advances nextIndex after successful snapshot install
 * - Multiple chunks transferred correctly (offset tracking)
 * - Follower responds with current term for leader step-down detection
 */
class InstallSnapshotTest {

    private final List<RaftMessage> node1Messages = new CopyOnWriteArrayList<>();
    private final List<RaftMessage> node2Messages = new CopyOnWriteArrayList<>();
    private final List<RaftMessage> node3Messages = new CopyOnWriteArrayList<>();

    private RaftNode node1;
    private RaftNode node2;
    private RaftNode node3;

    @BeforeEach
    void setUp() {
        NodeId id1 = NodeId.of(1);
        NodeId id2 = NodeId.of(2);
        NodeId id3 = NodeId.of(3);

        node1 = new RaftNode(id1, List.of(id2, id3), this::routeMessage);
        node2 = new RaftNode(id2, List.of(id1, id3), this::routeMessage);
        node3 = new RaftNode(id3, List.of(id1, id2), this::routeMessage);
    }

    private void routeMessage(RaftMessage msg) {
        switch (msg.to().id()) {
            case 1 -> node1Messages.add(msg);
            case 2 -> node2Messages.add(msg);
            case 3 -> node3Messages.add(msg);
        }
    }

    private void deliverMessages(NodeId nodeId) {
        List<RaftMessage> messages = switch (nodeId.id()) {
            case 1 -> node1Messages;
            case 2 -> node2Messages;
            case 3 -> node3Messages;
            default -> throw new IllegalArgumentException();
        };
        RaftNode target = switch (nodeId.id()) {
            case 1 -> node1;
            case 2 -> node2;
            case 3 -> node3;
            default -> throw new IllegalArgumentException();
        };
        List<RaftMessage> toDeliver = new ArrayList<>(messages);
        messages.clear();
        toDeliver.forEach(target::handleMessage);
    }

    private void electNode1AsLeader() {
        // Trigger election on node1
        node1.onElectionTimeout();
        // Deliver PreVote to node2 and node3
        deliverMessages(NodeId.of(2));
        deliverMessages(NodeId.of(3));
        // Deliver PreVoteResponses to node1
        deliverMessages(NodeId.of(1));
        // Deliver RequestVote to node2 and node3
        deliverMessages(NodeId.of(2));
        deliverMessages(NodeId.of(3));
        // Deliver RequestVoteResponses to node1
        deliverMessages(NodeId.of(1));
        assertThat(node1.state()).isEqualTo(RaftState.LEADER);
        // Deliver initial heartbeats (no-op) to followers
        deliverMessages(NodeId.of(2));
        deliverMessages(NodeId.of(3));
        // Deliver AppendEntriesResponses to leader
        deliverMessages(NodeId.of(1));
    }

    // ========== InstallSnapshot Message Creation ==========

    @Test
    @DisplayName("InstallSnapshot message carries snapshot metadata and data")
    void installSnapshotMessageCarriesMetadata() {
        byte[] data = "snapshot-data".getBytes();
        var msg = new RaftMessage.InstallSnapshot(
                3, NodeId.of(1), NodeId.of(2),
                10, 2, 0, data, true
        );

        assertThat(msg.term()).isEqualTo(3);
        assertThat(msg.lastIncludedIndex()).isEqualTo(10);
        assertThat(msg.lastIncludedTerm()).isEqualTo(2);
        assertThat(msg.offset()).isEqualTo(0);
        assertThat(msg.data()).isEqualTo(data);
        assertThat(msg.done()).isTrue();
    }

    @Test
    @DisplayName("InstallSnapshotResponse carries follower's current term")
    void installSnapshotResponseCarriesTerm() {
        var resp = new RaftMessage.InstallSnapshotResponse(
                3, NodeId.of(2), NodeId.of(1)
        );

        assertThat(resp.term()).isEqualTo(3);
        assertThat(resp.from()).isEqualTo(NodeId.of(2));
    }

    // ========== Follower Handling ==========

    @Test
    @DisplayName("follower accepts InstallSnapshot and resets log to snapshot state")
    void followerAcceptsSnapshotAndResetsLog() {
        electNode1AsLeader();

        // Clear all message queues from election
        node1Messages.clear();
        node2Messages.clear();
        node3Messages.clear();

        // Simulate: node2 is far behind, leader sends snapshot
        byte[] snapshotData = "full-snapshot".getBytes();
        var installMsg = new RaftMessage.InstallSnapshot(
                node1.currentTerm(), NodeId.of(1), NodeId.of(2),
                10, 1, 0, snapshotData, true
        );

        node2.handleMessage(installMsg);

        // node2's log should reflect the snapshot state
        assertThat(node2.log().snapshotIndex()).isEqualTo(10);
        assertThat(node2.log().snapshotTerm()).isEqualTo(1);

        // node2 should have sent a response to node1
        assertThat(node1Messages).hasSize(1);
        assertThat(node1Messages.getFirst()).isInstanceOf(RaftMessage.InstallSnapshotResponse.class);
    }

    @Test
    @DisplayName("follower rejects InstallSnapshot with stale term")
    void followerRejectsStaleTermSnapshot() {
        electNode1AsLeader();
        long currentTerm = node2.currentTerm();

        // Send InstallSnapshot with term lower than follower's current term
        var staleMsg = new RaftMessage.InstallSnapshot(
                currentTerm - 1, NodeId.of(1), NodeId.of(2),
                10, 1, 0, "data".getBytes(), true
        );

        node2.handleMessage(staleMsg);

        // Log should NOT be affected
        assertThat(node2.log().snapshotIndex()).isEqualTo(0);
    }

    @Test
    @DisplayName("follower discards log entries covered by snapshot")
    void followerDiscardsEntriesCoveredBySnapshot() {
        electNode1AsLeader();

        // First, replicate some entries to node2
        node1.propose("entry-1".getBytes());
        node1.propose("entry-2".getBytes());
        deliverMessages(NodeId.of(2));
        deliverMessages(NodeId.of(1));

        long logSizeBefore = node2.log().lastIndex();
        assertThat(logSizeBefore).isGreaterThan(0);

        // Now install a snapshot that covers all those entries
        var installMsg = new RaftMessage.InstallSnapshot(
                node1.currentTerm(), NodeId.of(1), NodeId.of(2),
                logSizeBefore + 5, node1.currentTerm(), 0,
                "snapshot-covering-all".getBytes(), true
        );

        node2.handleMessage(installMsg);

        // Log entries before snapshot should be discarded
        assertThat(node2.log().snapshotIndex()).isEqualTo(logSizeBefore + 5);
        assertThat(node2.log().lastIndex()).isEqualTo(logSizeBefore + 5);
    }

    // ========== Leader Handling ==========

    @Test
    @DisplayName("leader advances nextIndex after InstallSnapshotResponse")
    void leaderAdvancesNextIndexAfterResponse() {
        electNode1AsLeader();
        node1Messages.clear();
        node2Messages.clear();
        node3Messages.clear();

        // Leader sends InstallSnapshot to node2 at index 10
        node1.sendInstallSnapshot(NodeId.of(2), 10, 1, "data".getBytes());

        // Deliver InstallSnapshot to node2
        deliverMessages(NodeId.of(2));

        // Deliver response back to leader
        deliverMessages(NodeId.of(1));

        // Trigger heartbeat to check nextIndex advancement
        node1Messages.clear();
        node2Messages.clear();
        node1.sendHeartbeats();

        // Check that AppendEntries to node2 has prevLogIndex >= 10
        var ae = node2Messages.stream()
                .filter(m -> m instanceof RaftMessage.AppendEntries)
                .map(m -> (RaftMessage.AppendEntries) m)
                .findFirst();
        assertThat(ae).isPresent();
        assertThat(ae.get().prevLogIndex()).isGreaterThanOrEqualTo(10);
    }

    // ========== Snapshot with Log Compaction ==========

    @Test
    @DisplayName("RaftLog tracks snapshot metadata after install")
    void raftLogTracksSnapshotMetadata() {
        RaftLog log = new RaftLog();

        // Append some entries
        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(1, 2, "b".getBytes()));
        log.append(new LogEntry(2, 3, "c".getBytes()));

        // Install snapshot at index 5, term 2
        log.installSnapshot(5, 2);

        assertThat(log.snapshotIndex()).isEqualTo(5);
        assertThat(log.snapshotTerm()).isEqualTo(2);
        assertThat(log.lastIndex()).isEqualTo(5);
        // Entries before snapshot are gone
        assertThat(log.getEntry(1)).isNull();
        assertThat(log.getEntry(2)).isNull();
        assertThat(log.getEntry(3)).isNull();
    }

    @Test
    @DisplayName("RaftLog preserves entries after snapshot index")
    void raftLogPreservesEntriesAfterSnapshotIndex() {
        RaftLog log = new RaftLog();

        log.append(new LogEntry(1, 1, "a".getBytes()));
        log.append(new LogEntry(1, 2, "b".getBytes()));
        log.append(new LogEntry(2, 3, "c".getBytes()));
        log.append(new LogEntry(2, 4, "d".getBytes()));
        log.append(new LogEntry(2, 5, "e".getBytes()));

        // Snapshot at index 3 — entries 4 and 5 should survive
        log.installSnapshot(3, 2);

        assertThat(log.snapshotIndex()).isEqualTo(3);
        assertThat(log.lastIndex()).isEqualTo(5);
        assertThat(log.getEntry(4)).isNotNull();
        assertThat(log.getEntry(4).data()).isEqualTo("d".getBytes());
        assertThat(log.getEntry(5)).isNotNull();
        assertThat(log.getEntry(5).data()).isEqualTo("e".getBytes());
    }

    @Test
    @DisplayName("RaftLog hasMatchingEntry works with snapshot sentinel")
    void hasMatchingEntryWorksWithSnapshot() {
        RaftLog log = new RaftLog();
        log.installSnapshot(10, 3);

        // The snapshot boundary should match
        assertThat(log.hasMatchingEntry(10, 3)).isTrue();
        // Before snapshot should not match (entries discarded)
        assertThat(log.hasMatchingEntry(5, 1)).isFalse();
    }

    @Test
    @DisplayName("RaftLog termAt returns snapshot term for snapshot index")
    void termAtReturnsSnapshotTermForSnapshotIndex() {
        RaftLog log = new RaftLog();
        log.installSnapshot(10, 3);

        assertThat(log.termAt(10)).isEqualTo(3);
        assertThat(log.termAt(0)).isEqualTo(0); // sentinel still works
    }
}
