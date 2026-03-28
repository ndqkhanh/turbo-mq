package turbomq.common;

import java.time.Instant;
import java.util.Set;

/**
 * Gossip membership entry for a single broker.
 * Tracks state, incarnation number (crdt-style lamport counter),
 * and the set of partitions hosted by this broker.
 */
public record MembershipEntry(
        BrokerAddress address,
        BrokerState state,
        long incarnation,
        Set<PartitionId> partitions,
        Instant lastUpdated
) {

    public MembershipEntry withState(BrokerState newState, Instant now) {
        return new MembershipEntry(address, newState, incarnation, partitions, now);
    }

    public MembershipEntry withIncarnation(long newIncarnation, Instant now) {
        return new MembershipEntry(address, BrokerState.ALIVE, newIncarnation, partitions, now);
    }

    public MembershipEntry withPartitions(Set<PartitionId> newPartitions, Instant now) {
        return new MembershipEntry(address, state, incarnation, newPartitions, now);
    }
}
