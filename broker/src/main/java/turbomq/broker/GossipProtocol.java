package turbomq.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import turbomq.common.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SWIM-lite gossip protocol for cluster membership and metadata dissemination.
 *
 * Each broker runs a GossipProtocol instance that:
 * - Maintains a membership table of all known brokers
 * - Probes peers via direct ping and indirect ping (SWIM protocol)
 * - Transitions: ALIVE → SUSPECT (ping fail) → DEAD (timeout)
 * - Exchanges membership state via push-pull gossip rounds
 * - Tracks partition assignments per broker
 * - Supports self-refutation via incarnation numbers
 */
public final class GossipProtocol {

    private static final Logger log = LoggerFactory.getLogger(GossipProtocol.class);

    private final NodeId self;
    private final BrokerAddress selfAddress;
    private final GossipTransport transport;
    private final GossipConfig config;
    private final Map<NodeId, MembershipEntry> members = new ConcurrentHashMap<>();

    public GossipProtocol(NodeId self, BrokerAddress selfAddress,
                          GossipTransport transport, GossipConfig config) {
        this.self = self;
        this.selfAddress = selfAddress;
        this.transport = transport;
        this.config = config;

        // Register self as ALIVE
        members.put(self, new MembershipEntry(
                selfAddress, BrokerState.ALIVE, 0, Set.of(), Instant.now()));
    }

    /**
     * Returns a snapshot of the current membership table.
     */
    public Map<NodeId, MembershipEntry> getMembers() {
        return Collections.unmodifiableMap(new HashMap<>(members));
    }

    /**
     * Add a peer broker to the membership table as ALIVE.
     */
    public void addPeer(BrokerAddress address) {
        members.putIfAbsent(address.nodeId(), new MembershipEntry(
                address, BrokerState.ALIVE, 0, Set.of(), Instant.now()));
    }

    /**
     * Direct ping probe for a single node.
     * On success: keeps ALIVE. On failure: marks SUSPECT.
     */
    public void probeNode(NodeId target) {
        MembershipEntry entry = members.get(target);
        if (entry == null) return;

        boolean ack = transport.ping(target).join();
        if (ack) {
            if (entry.state() != BrokerState.ALIVE) {
                members.put(target, entry.withState(BrokerState.ALIVE, Instant.now()));
                log.info("Node {} restored to ALIVE via direct ping", target);
            }
        } else {
            if (entry.state() == BrokerState.ALIVE) {
                members.put(target, entry.withState(BrokerState.SUSPECT, Instant.now()));
                log.info("Node {} marked SUSPECT (direct ping failed)", target);
            }
        }
    }

    /**
     * Indirect probe: ask other alive peers to ping the suspect target.
     * If any intermediary confirms the target is alive, restore to ALIVE.
     */
    public void indirectProbe(NodeId target) {
        MembershipEntry entry = members.get(target);
        if (entry == null) return;

        List<NodeId> intermediaries = getAlivePeers();
        intermediaries.remove(target);

        boolean confirmed = false;
        for (NodeId intermediary : intermediaries) {
            boolean result = transport.pingRequest(intermediary, target).join();
            if (result) {
                confirmed = true;
                break;
            }
        }

        if (confirmed) {
            members.put(target, entry.withState(BrokerState.ALIVE, Instant.now()));
            log.info("Node {} restored to ALIVE via indirect ping", target);
        }
        // If not confirmed, stays SUSPECT — will eventually expire to DEAD
    }

    /**
     * Expire SUSPECT nodes that have exceeded the suspect timeout.
     *
     * @param now the current time (externalized for testability)
     */
    public void expireSuspects(Instant now) {
        for (Map.Entry<NodeId, MembershipEntry> e : members.entrySet()) {
            if (e.getKey().equals(self)) continue;
            MembershipEntry entry = e.getValue();
            if (entry.state() == BrokerState.SUSPECT) {
                if (entry.lastUpdated().plus(config.suspectTimeout()).isBefore(now)) {
                    members.put(e.getKey(), entry.withState(BrokerState.DEAD, now));
                    log.info("Node {} declared DEAD (suspect timeout expired)", e.getKey());
                }
            }
        }
    }

    /**
     * Merge a remote membership entry. Rules:
     * - Higher incarnation always wins
     * - Same incarnation: ALIVE < SUSPECT < DEAD
     * - If someone says WE are suspect/dead, refute by bumping incarnation
     */
    public void mergeEntry(NodeId nodeId, MembershipEntry remote) {
        // Self-refutation: if gossip says we're suspect or dead, bump incarnation
        if (nodeId.equals(self)) {
            if (remote.state() != BrokerState.ALIVE) {
                MembershipEntry current = members.get(self);
                long newIncarnation = Math.max(current.incarnation(), remote.incarnation()) + 1;
                members.put(self, current.withIncarnation(newIncarnation, Instant.now()));
                log.info("Self-refutation: bumped incarnation to {}", newIncarnation);
                return;
            }
        }

        MembershipEntry local = members.get(nodeId);
        if (local == null) {
            // New node we haven't seen
            members.put(nodeId, remote);
            return;
        }

        // Higher incarnation always wins
        if (remote.incarnation() > local.incarnation()) {
            members.put(nodeId, remote);
            return;
        }

        // Same incarnation: more severe state wins (ALIVE < SUSPECT < DEAD)
        // If same state, accept remote if it has newer timestamp (carries fresh partition data)
        if (remote.incarnation() == local.incarnation()) {
            if (stateOrdinal(remote.state()) > stateOrdinal(local.state())) {
                members.put(nodeId, remote);
            } else if (remote.state() == local.state()
                    && remote.lastUpdated().isAfter(local.lastUpdated())) {
                members.put(nodeId, remote);
            }
        }
        // Lower incarnation: ignore (stale)
    }

    /**
     * Run one gossip round: select up to fanout alive peers and push-pull with them.
     */
    public void gossipRound() {
        List<NodeId> peers = getAlivePeers();
        Collections.shuffle(peers);

        int count = Math.min(config.fanout(), peers.size());
        GossipExchange localState = new GossipExchange(new HashMap<>(members));

        for (int i = 0; i < count; i++) {
            NodeId target = peers.get(i);
            try {
                GossipExchange remoteState = transport.pushPull(target, localState).join();
                for (Map.Entry<NodeId, MembershipEntry> e : remoteState.members().entrySet()) {
                    mergeEntry(e.getKey(), e.getValue());
                }
            } catch (Exception e) {
                log.warn("Gossip push-pull to {} failed: {}", target, e.getMessage());
            }
        }
    }

    /**
     * Update the partition set for this broker (self).
     */
    public void updatePartitions(Set<PartitionId> partitions) {
        MembershipEntry current = members.get(self);
        members.put(self, current.withPartitions(partitions, Instant.now()));
    }

    /**
     * Get list of alive peer node IDs (excludes self and DEAD nodes).
     * SUSPECT nodes are excluded — only fully ALIVE peers returned.
     */
    public List<NodeId> getAlivePeers() {
        List<NodeId> result = new ArrayList<>();
        for (Map.Entry<NodeId, MembershipEntry> e : members.entrySet()) {
            if (e.getKey().equals(self)) continue;
            if (e.getValue().state() == BrokerState.ALIVE) {
                result.add(e.getKey());
            }
        }
        return result;
    }

    private static int stateOrdinal(BrokerState state) {
        return switch (state) {
            case ALIVE -> 0;
            case SUSPECT -> 1;
            case DEAD -> 2;
        };
    }
}
