package turbomq.common;

/**
 * Network address of a broker in the cluster.
 */
public record BrokerAddress(NodeId nodeId, String host, int port) {

    public static BrokerAddress of(int nodeId, String host, int port) {
        return new BrokerAddress(NodeId.of(nodeId), host, port);
    }

    @Override
    public String toString() {
        return nodeId + "@" + host + ":" + port;
    }
}
