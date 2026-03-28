package turbomq.common;

/**
 * SWIM-lite membership states for a broker in the cluster.
 */
public enum BrokerState {
    /** Broker is reachable and healthy. */
    ALIVE,
    /** Broker failed direct ping; indirect probing in progress. */
    SUSPECT,
    /** Broker confirmed unreachable after indirect probe timeout. */
    DEAD
}
