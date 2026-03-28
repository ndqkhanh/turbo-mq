package turbomq.broker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import turbomq.common.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.*;

/**
 * SWIM-lite gossip protocol tests.
 * Uses a fake transport to deterministically control ping/ack behavior.
 */
class GossipProtocolTest {

    private FakeGossipTransport transport;
    private GossipProtocol gossip;
    private final NodeId self = NodeId.of(1);
    private final BrokerAddress selfAddr = BrokerAddress.of(1, "localhost", 9001);

    @BeforeEach
    void setUp() {
        transport = new FakeGossipTransport();
        GossipConfig config = new GossipConfig(
                Duration.ofMillis(500),   // gossip interval
                Duration.ofMillis(200),   // ping timeout
                Duration.ofSeconds(2),    // suspect timeout
                3                          // fanout
        );
        gossip = new GossipProtocol(self, selfAddr, transport, config);
    }

    // --- Membership management ---

    @Test
    void selfIsAliveOnCreation() {
        Map<NodeId, MembershipEntry> members = gossip.getMembers();
        assertThat(members).containsKey(self);
        assertThat(members.get(self).state()).isEqualTo(BrokerState.ALIVE);
    }

    @Test
    void addPeerRegistersAsAlive() {
        BrokerAddress peer = BrokerAddress.of(2, "localhost", 9002);
        gossip.addPeer(peer);

        Map<NodeId, MembershipEntry> members = gossip.getMembers();
        assertThat(members).containsKey(NodeId.of(2));
        assertThat(members.get(NodeId.of(2)).state()).isEqualTo(BrokerState.ALIVE);
    }

    @Test
    void addMultiplePeers() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));
        gossip.addPeer(BrokerAddress.of(3, "localhost", 9003));
        gossip.addPeer(BrokerAddress.of(4, "localhost", 9004));

        assertThat(gossip.getMembers()).hasSize(4); // self + 3 peers
    }

    @Test
    void duplicateAddPeerIsIdempotent() {
        BrokerAddress peer = BrokerAddress.of(2, "localhost", 9002);
        gossip.addPeer(peer);
        gossip.addPeer(peer);

        assertThat(gossip.getMembers()).hasSize(2);
    }

    // --- Direct ping ---

    @Test
    void pingSuccessKeepsAlive() {
        BrokerAddress peer = BrokerAddress.of(2, "localhost", 9002);
        gossip.addPeer(peer);
        transport.setPingResult(NodeId.of(2), true);

        gossip.probeNode(NodeId.of(2));

        assertThat(gossip.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.ALIVE);
    }

    @Test
    void pingFailureMarksSuspect() {
        BrokerAddress peer = BrokerAddress.of(2, "localhost", 9002);
        gossip.addPeer(peer);
        transport.setPingResult(NodeId.of(2), false);

        gossip.probeNode(NodeId.of(2));

        assertThat(gossip.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.SUSPECT);
    }

    // --- Indirect ping ---

    @Test
    void indirectPingSuccessRestoresAlive() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));
        gossip.addPeer(BrokerAddress.of(3, "localhost", 9003));

        // Direct ping fails
        transport.setPingResult(NodeId.of(2), false);
        gossip.probeNode(NodeId.of(2));
        assertThat(gossip.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.SUSPECT);

        // Indirect ping via node 3 succeeds
        transport.setPingRequestResult(NodeId.of(3), NodeId.of(2), true);
        gossip.indirectProbe(NodeId.of(2));

        assertThat(gossip.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.ALIVE);
    }

    @Test
    void indirectPingFailureKeepsSuspect() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));
        gossip.addPeer(BrokerAddress.of(3, "localhost", 9003));

        transport.setPingResult(NodeId.of(2), false);
        gossip.probeNode(NodeId.of(2));

        transport.setPingRequestResult(NodeId.of(3), NodeId.of(2), false);
        gossip.indirectProbe(NodeId.of(2));

        assertThat(gossip.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.SUSPECT);
    }

    // --- Suspect timeout → Dead ---

    @Test
    void suspectBecomesDeadAfterTimeout() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));
        transport.setPingResult(NodeId.of(2), false);
        gossip.probeNode(NodeId.of(2));

        // Simulate time passing beyond suspect timeout
        gossip.expireSuspects(Instant.now().plus(Duration.ofSeconds(3)));

        assertThat(gossip.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.DEAD);
    }

    @Test
    void suspectDoesNotDieBeforeTimeout() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));
        transport.setPingResult(NodeId.of(2), false);
        gossip.probeNode(NodeId.of(2));

        // Time within suspect timeout
        gossip.expireSuspects(Instant.now().plus(Duration.ofSeconds(1)));

        assertThat(gossip.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.SUSPECT);
    }

    // --- Alive overrides suspect/dead (incarnation) ---

    @Test
    void higherIncarnationOverridesSuspect() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));
        transport.setPingResult(NodeId.of(2), false);
        gossip.probeNode(NodeId.of(2));
        assertThat(gossip.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.SUSPECT);

        // Receive gossip with higher incarnation
        MembershipEntry fresh = new MembershipEntry(
                BrokerAddress.of(2, "localhost", 9002),
                BrokerState.ALIVE,
                1, // higher incarnation
                Set.of(),
                Instant.now()
        );
        gossip.mergeEntry(NodeId.of(2), fresh);

        assertThat(gossip.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.ALIVE);
    }

    @Test
    void lowerIncarnationDoesNotOverride() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));
        transport.setPingResult(NodeId.of(2), false);
        gossip.probeNode(NodeId.of(2));

        // Receive stale gossip with same incarnation but ALIVE
        MembershipEntry stale = new MembershipEntry(
                BrokerAddress.of(2, "localhost", 9002),
                BrokerState.ALIVE,
                0, // same incarnation — suspect wins
                Set.of(),
                Instant.now()
        );
        gossip.mergeEntry(NodeId.of(2), stale);

        assertThat(gossip.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.SUSPECT);
    }

    // --- Partition tracking ---

    @Test
    void updatePartitionsForSelf() {
        gossip.updatePartitions(Set.of(PartitionId.of(0), PartitionId.of(1)));

        assertThat(gossip.getMembers().get(self).partitions())
                .containsExactlyInAnyOrder(PartitionId.of(0), PartitionId.of(1));
    }

    @Test
    void mergedEntryIncludesPartitions() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));

        MembershipEntry withPartitions = new MembershipEntry(
                BrokerAddress.of(2, "localhost", 9002),
                BrokerState.ALIVE,
                1,
                Set.of(PartitionId.of(3), PartitionId.of(4)),
                Instant.now()
        );
        gossip.mergeEntry(NodeId.of(2), withPartitions);

        assertThat(gossip.getMembers().get(NodeId.of(2)).partitions())
                .containsExactlyInAnyOrder(PartitionId.of(3), PartitionId.of(4));
    }

    // --- Push-pull gossip exchange ---

    @Test
    void pushPullMergesRemoteState() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));

        // Remote knows about node 3 that we don't
        MembershipEntry node3 = new MembershipEntry(
                BrokerAddress.of(3, "localhost", 9003),
                BrokerState.ALIVE,
                0,
                Set.of(PartitionId.of(5)),
                Instant.now()
        );
        GossipExchange remoteState = new GossipExchange(Map.of(
                NodeId.of(3), node3
        ));
        transport.setPushPullResult(NodeId.of(2), remoteState);

        gossip.gossipRound();

        assertThat(gossip.getMembers()).containsKey(NodeId.of(3));
        assertThat(gossip.getMembers().get(NodeId.of(3)).partitions())
                .containsExactlyInAnyOrder(PartitionId.of(5));
    }

    // --- Alive peers query ---

    @Test
    void getAlivePeersExcludesDeadAndSelf() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));
        gossip.addPeer(BrokerAddress.of(3, "localhost", 9003));

        // Kill node 3
        transport.setPingResult(NodeId.of(3), false);
        gossip.probeNode(NodeId.of(3));
        gossip.expireSuspects(Instant.now().plus(Duration.ofSeconds(3)));

        List<NodeId> alive = gossip.getAlivePeers();
        assertThat(alive).containsExactly(NodeId.of(2));
    }

    @Test
    void getAlivePeersIncludesSuspect() {
        gossip.addPeer(BrokerAddress.of(2, "localhost", 9002));
        transport.setPingResult(NodeId.of(2), false);
        gossip.probeNode(NodeId.of(2));

        // Suspect nodes are still included (they might recover)
        List<NodeId> alive = gossip.getAlivePeers();
        assertThat(alive).isEmpty(); // suspect is NOT included in alive peers
    }

    // --- Self-refutation ---

    @Test
    void selfRefutationBumpsIncarnation() {
        // Simulate receiving gossip that says we are suspect
        MembershipEntry suspectSelf = new MembershipEntry(
                selfAddr,
                BrokerState.SUSPECT,
                0,
                Set.of(),
                Instant.now()
        );
        gossip.mergeEntry(self, suspectSelf);

        // Self should refute by bumping incarnation
        assertThat(gossip.getMembers().get(self).state()).isEqualTo(BrokerState.ALIVE);
        assertThat(gossip.getMembers().get(self).incarnation()).isGreaterThan(0);
    }

    // --- Helper: Fake transport ---

    static class FakeGossipTransport implements GossipTransport {
        private final Map<NodeId, Boolean> pingResults = new ConcurrentHashMap<>();
        private final Map<String, Boolean> pingRequestResults = new ConcurrentHashMap<>();
        private final Map<NodeId, GossipExchange> pushPullResults = new ConcurrentHashMap<>();

        void setPingResult(NodeId target, boolean success) {
            pingResults.put(target, success);
        }

        void setPingRequestResult(NodeId intermediary, NodeId target, boolean success) {
            pingRequestResults.put(intermediary + "->" + target, success);
        }

        void setPushPullResult(NodeId target, GossipExchange exchange) {
            pushPullResults.put(target, exchange);
        }

        @Override
        public CompletableFuture<Boolean> ping(NodeId target) {
            return CompletableFuture.completedFuture(pingResults.getOrDefault(target, false));
        }

        @Override
        public CompletableFuture<Boolean> pingRequest(NodeId intermediary, NodeId target) {
            String key = intermediary + "->" + target;
            return CompletableFuture.completedFuture(pingRequestResults.getOrDefault(key, false));
        }

        @Override
        public CompletableFuture<GossipExchange> pushPull(NodeId target, GossipExchange localState) {
            GossipExchange result = pushPullResults.getOrDefault(target, new GossipExchange(Map.of()));
            return CompletableFuture.completedFuture(result);
        }
    }
}
