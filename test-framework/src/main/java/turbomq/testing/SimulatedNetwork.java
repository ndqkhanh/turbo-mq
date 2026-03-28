package turbomq.testing;

import turbomq.common.NodeId;
import turbomq.raft.RaftMessage;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiPredicate;

/**
 * Deterministic simulated network for Raft testing.
 *
 * Inspired by FoundationDB's deterministic simulation: all message
 * delivery is controlled by the test, enabling reproducible scenarios
 * for split votes, network partitions, message reordering, and drops.
 *
 * Messages are queued and delivered only when the test explicitly
 * calls {@link #deliverAll()} or {@link #deliverTo(NodeId)}.
 */
public final class SimulatedNetwork {

    /** Queued messages awaiting delivery, in insertion order. */
    private final Deque<RaftMessage> pendingMessages = new ArrayDeque<>();

    /** Delivered messages per destination node. */
    private final Map<NodeId, Queue<RaftMessage>> inboxes = new HashMap<>();

    /** Active partition rules: if predicate returns true, message is dropped. */
    private final List<BiPredicate<NodeId, NodeId>> dropRules = new ArrayList<>();

    /** Register a node so it has an inbox. */
    public void registerNode(NodeId nodeId) {
        inboxes.putIfAbsent(nodeId, new ConcurrentLinkedQueue<>());
    }

    /**
     * Send a message into the simulated network.
     * The message is queued but NOT delivered until delivery is triggered.
     */
    public void send(RaftMessage message) {
        pendingMessages.add(message);
    }

    /**
     * Deliver all pending messages to their destination inboxes.
     * Messages matching any drop rule are silently discarded.
     *
     * @return the number of messages delivered (excluding dropped)
     */
    public int deliverAll() {
        int delivered = 0;
        while (!pendingMessages.isEmpty()) {
            RaftMessage msg = pendingMessages.poll();
            if (shouldDrop(msg.from(), msg.to())) {
                continue;
            }
            Queue<RaftMessage> inbox = inboxes.get(msg.to());
            if (inbox != null) {
                inbox.add(msg);
                delivered++;
            }
        }
        return delivered;
    }

    /**
     * Deliver only messages destined for a specific node.
     * Other messages remain in the pending queue.
     *
     * @return the number of messages delivered
     */
    public int deliverTo(NodeId nodeId) {
        int delivered = 0;
        int size = pendingMessages.size();
        for (int i = 0; i < size; i++) {
            RaftMessage msg = pendingMessages.poll();
            if (msg.to().equals(nodeId)) {
                if (!shouldDrop(msg.from(), msg.to())) {
                    Queue<RaftMessage> inbox = inboxes.get(nodeId);
                    if (inbox != null) {
                        inbox.add(msg);
                        delivered++;
                    }
                }
            } else {
                pendingMessages.add(msg); // re-queue for others
            }
        }
        return delivered;
    }

    /** Poll the next message from a node's inbox, or null if empty. */
    public RaftMessage receive(NodeId nodeId) {
        Queue<RaftMessage> inbox = inboxes.get(nodeId);
        return inbox != null ? inbox.poll() : null;
    }

    /** Drain all messages from a node's inbox. */
    public List<RaftMessage> receiveAll(NodeId nodeId) {
        Queue<RaftMessage> inbox = inboxes.get(nodeId);
        if (inbox == null || inbox.isEmpty()) return List.of();
        List<RaftMessage> messages = new ArrayList<>(inbox);
        inbox.clear();
        return messages;
    }

    /** Number of messages waiting in the pending queue (not yet delivered). */
    public int pendingCount() {
        return pendingMessages.size();
    }

    /** Number of messages in a node's inbox (delivered but not consumed). */
    public int inboxSize(NodeId nodeId) {
        Queue<RaftMessage> inbox = inboxes.get(nodeId);
        return inbox != null ? inbox.size() : 0;
    }

    // ========== Network Fault Injection ==========

    /**
     * Partition a node from the cluster: all messages to/from this node are dropped.
     */
    public void isolateNode(NodeId nodeId) {
        dropRules.add((from, to) -> from.equals(nodeId) || to.equals(nodeId));
    }

    /**
     * Create a one-way partition: messages from {@code from} to {@code to} are dropped.
     */
    public void dropMessages(NodeId from, NodeId to) {
        dropRules.add((f, t) -> f.equals(from) && t.equals(to));
    }

    /**
     * Create a bidirectional partition between two nodes.
     */
    public void partitionNodes(NodeId a, NodeId b) {
        dropRules.add((f, t) ->
                (f.equals(a) && t.equals(b)) || (f.equals(b) && t.equals(a)));
    }

    /** Remove all drop rules, healing all partitions. */
    public void healAll() {
        dropRules.clear();
    }

    /** Clear all pending messages and inboxes. */
    public void reset() {
        pendingMessages.clear();
        inboxes.values().forEach(Queue::clear);
        dropRules.clear();
    }

    private boolean shouldDrop(NodeId from, NodeId to) {
        for (var rule : dropRules) {
            if (rule.test(from, to)) return true;
        }
        return false;
    }
}
