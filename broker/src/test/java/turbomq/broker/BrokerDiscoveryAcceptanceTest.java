package turbomq.broker;

import org.junit.jupiter.api.Test;
import turbomq.common.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.*;

/**
 * Acceptance test: 3 brokers discover each other via gossip push-pull rounds.
 * Verifies that membership and partition metadata propagates across the cluster.
 */
class BrokerDiscoveryAcceptanceTest {

    @Test
    void threeBrokersDiscoverEachOtherViaGossip() {
        // Set up 3 gossip instances with an interconnected fake transport
        InterconnectedTransport transport = new InterconnectedTransport();

        GossipConfig config = new GossipConfig(
                Duration.ofMillis(500), Duration.ofMillis(200),
                Duration.ofSeconds(5), 3);

        GossipProtocol broker1 = new GossipProtocol(
                NodeId.of(1), BrokerAddress.of(1, "host1", 9001), transport, config);
        GossipProtocol broker2 = new GossipProtocol(
                NodeId.of(2), BrokerAddress.of(2, "host2", 9002), transport, config);
        GossipProtocol broker3 = new GossipProtocol(
                NodeId.of(3), BrokerAddress.of(3, "host3", 9003), transport, config);

        transport.register(NodeId.of(1), broker1);
        transport.register(NodeId.of(2), broker2);
        transport.register(NodeId.of(3), broker3);

        // Broker 1 knows about broker 2, broker 2 knows about broker 3
        // Broker 1 does NOT initially know about broker 3
        broker1.addPeer(BrokerAddress.of(2, "host2", 9002));
        broker2.addPeer(BrokerAddress.of(1, "host1", 9001));
        broker2.addPeer(BrokerAddress.of(3, "host3", 9003));
        broker3.addPeer(BrokerAddress.of(2, "host2", 9002));

        // Set partition metadata
        broker1.updatePartitions(Set.of(PartitionId.of(0), PartitionId.of(1)));
        broker2.updatePartitions(Set.of(PartitionId.of(2)));
        broker3.updatePartitions(Set.of(PartitionId.of(3)));

        // Run gossip rounds — after a few rounds, all brokers should know about each other
        for (int i = 0; i < 3; i++) {
            broker1.gossipRound();
            broker2.gossipRound();
            broker3.gossipRound();
        }

        // Broker 1 should now know about broker 3 (transitive discovery)
        assertThat(broker1.getMembers()).containsKey(NodeId.of(3));
        assertThat(broker1.getMembers().get(NodeId.of(3)).address().host()).isEqualTo("host3");

        // Broker 3 should know about broker 1
        assertThat(broker3.getMembers()).containsKey(NodeId.of(1));

        // All brokers should have full cluster view
        for (GossipProtocol broker : new GossipProtocol[]{broker1, broker2, broker3}) {
            assertThat(broker.getMembers()).hasSize(3);
            assertThat(broker.getMembers().keySet())
                    .containsExactlyInAnyOrder(NodeId.of(1), NodeId.of(2), NodeId.of(3));
        }

        // Partition metadata should have propagated
        assertThat(broker3.getMembers().get(NodeId.of(1)).partitions())
                .containsExactlyInAnyOrder(PartitionId.of(0), PartitionId.of(1));
        assertThat(broker1.getMembers().get(NodeId.of(3)).partitions())
                .containsExactlyInAnyOrder(PartitionId.of(3));
    }

    @Test
    void brokerFailureDetectedViaGossip() {
        InterconnectedTransport transport = new InterconnectedTransport();
        GossipConfig config = new GossipConfig(
                Duration.ofMillis(500), Duration.ofMillis(200),
                Duration.ofSeconds(2), 3);

        GossipProtocol broker1 = new GossipProtocol(
                NodeId.of(1), BrokerAddress.of(1, "host1", 9001), transport, config);
        GossipProtocol broker2 = new GossipProtocol(
                NodeId.of(2), BrokerAddress.of(2, "host2", 9002), transport, config);

        transport.register(NodeId.of(1), broker1);
        transport.register(NodeId.of(2), broker2);

        broker1.addPeer(BrokerAddress.of(2, "host2", 9002));
        broker2.addPeer(BrokerAddress.of(1, "host1", 9001));

        // Simulate broker 2 going down
        transport.setReachable(NodeId.of(2), false);

        // Broker 1 probes broker 2 → SUSPECT
        broker1.probeNode(NodeId.of(2));
        assertThat(broker1.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.SUSPECT);

        // After suspect timeout → DEAD
        broker1.expireSuspects(Instant.now().plus(Duration.ofSeconds(5)));
        assertThat(broker1.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.DEAD);
    }

    @Test
    void brokerRecoveryViaIncarnationBump() {
        InterconnectedTransport transport = new InterconnectedTransport();
        GossipConfig config = new GossipConfig(
                Duration.ofMillis(500), Duration.ofMillis(200),
                Duration.ofSeconds(2), 3);

        GossipProtocol broker1 = new GossipProtocol(
                NodeId.of(1), BrokerAddress.of(1, "host1", 9001), transport, config);
        GossipProtocol broker2 = new GossipProtocol(
                NodeId.of(2), BrokerAddress.of(2, "host2", 9002), transport, config);

        transport.register(NodeId.of(1), broker1);
        transport.register(NodeId.of(2), broker2);

        broker1.addPeer(BrokerAddress.of(2, "host2", 9002));
        broker2.addPeer(BrokerAddress.of(1, "host1", 9001));

        // Broker 2 goes down, detected by broker 1
        transport.setReachable(NodeId.of(2), false);
        broker1.probeNode(NodeId.of(2));
        assertThat(broker1.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.SUSPECT);

        // Broker 2 comes back up
        transport.setReachable(NodeId.of(2), true);

        // Broker 2 self-refutes by merging the suspect info and bumping incarnation
        MembershipEntry suspectEntry = broker1.getMembers().get(NodeId.of(2));
        broker2.mergeEntry(NodeId.of(2), suspectEntry);

        // Broker 2 gossips its recovery to broker 1 (recovered broker initiates)
        broker2.gossipRound();

        // Broker 1 should see broker 2 as ALIVE with higher incarnation
        assertThat(broker1.getMembers().get(NodeId.of(2)).state()).isEqualTo(BrokerState.ALIVE);
        assertThat(broker1.getMembers().get(NodeId.of(2)).incarnation()).isGreaterThan(0);
    }

    /**
     * Interconnected transport that routes push-pull between in-memory gossip instances.
     */
    static class InterconnectedTransport implements GossipTransport {
        private final Map<NodeId, GossipProtocol> nodes = new ConcurrentHashMap<>();
        private final Map<NodeId, Boolean> reachable = new ConcurrentHashMap<>();

        void register(NodeId nodeId, GossipProtocol protocol) {
            nodes.put(nodeId, protocol);
            reachable.put(nodeId, true);
        }

        void setReachable(NodeId nodeId, boolean isReachable) {
            reachable.put(nodeId, isReachable);
        }

        @Override
        public CompletableFuture<Boolean> ping(NodeId target) {
            return CompletableFuture.completedFuture(
                    reachable.getOrDefault(target, false) && nodes.containsKey(target));
        }

        @Override
        public CompletableFuture<Boolean> pingRequest(NodeId intermediary, NodeId target) {
            if (!reachable.getOrDefault(intermediary, false)) {
                return CompletableFuture.completedFuture(false);
            }
            return ping(target);
        }

        @Override
        public CompletableFuture<GossipExchange> pushPull(NodeId target, GossipExchange localState) {
            GossipProtocol remote = nodes.get(target);
            if (remote == null || !reachable.getOrDefault(target, false)) {
                return CompletableFuture.completedFuture(new GossipExchange(Map.of()));
            }

            // Remote merges our state
            for (Map.Entry<NodeId, MembershipEntry> e : localState.members().entrySet()) {
                remote.mergeEntry(e.getKey(), e.getValue());
            }

            // Return remote's current state
            return CompletableFuture.completedFuture(
                    new GossipExchange(remote.getMembers()));
        }
    }
}
