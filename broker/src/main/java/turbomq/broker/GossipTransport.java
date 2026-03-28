package turbomq.broker;

import turbomq.common.NodeId;

import java.util.concurrent.CompletableFuture;

/**
 * Transport abstraction for gossip protocol communication.
 * Allows testing with simulated network and production use with gRPC.
 */
public interface GossipTransport {

    /**
     * Send a ping to the target node. Returns true if ACK received within timeout.
     */
    CompletableFuture<Boolean> ping(NodeId target);

    /**
     * Ask an intermediary node to ping the target on our behalf (indirect ping).
     * Returns true if the intermediary confirms the target is alive.
     */
    CompletableFuture<Boolean> pingRequest(NodeId intermediary, NodeId target);

    /**
     * Push local membership state to a peer and receive their state back (push-pull).
     */
    CompletableFuture<GossipExchange> pushPull(NodeId target, GossipExchange localState);
}
