package turbomq.broker;

import java.time.Duration;

/**
 * Configuration for the SWIM-lite gossip protocol.
 *
 * @param gossipInterval  how often to run a gossip round (push-pull)
 * @param pingTimeout     timeout for direct ping ACK
 * @param suspectTimeout  how long a node stays SUSPECT before being declared DEAD
 * @param fanout          number of peers to gossip with per round
 */
public record GossipConfig(
        Duration gossipInterval,
        Duration pingTimeout,
        Duration suspectTimeout,
        int fanout
) {

    public static GossipConfig defaults() {
        return new GossipConfig(
                Duration.ofMillis(500),
                Duration.ofMillis(200),
                Duration.ofSeconds(5),
                3
        );
    }
}
