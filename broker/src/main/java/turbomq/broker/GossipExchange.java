package turbomq.broker;

import turbomq.common.MembershipEntry;
import turbomq.common.NodeId;

import java.util.Map;

/**
 * State exchanged during gossip push-pull rounds.
 */
public record GossipExchange(Map<NodeId, MembershipEntry> members) {
}
