package turbomq.broker;

import turbomq.common.NodeId;
import turbomq.common.PartitionId;

import java.time.Instant;

/**
 * Describes a single partition migration from one broker to another.
 */
public final class MigrationPlan {

    private final PartitionId partitionId;
    private final String topic;
    private final NodeId sourceBroker;
    private final NodeId targetBroker;
    private final Instant createdAt;
    private volatile MigrationState state;

    public MigrationPlan(PartitionId partitionId, String topic,
                         NodeId sourceBroker, NodeId targetBroker) {
        this.partitionId = partitionId;
        this.topic = topic;
        this.sourceBroker = sourceBroker;
        this.targetBroker = targetBroker;
        this.createdAt = Instant.now();
        this.state = MigrationState.ADDING_LEARNER;
    }

    public PartitionId partitionId() { return partitionId; }
    public String topic() { return topic; }
    public NodeId sourceBroker() { return sourceBroker; }
    public NodeId targetBroker() { return targetBroker; }
    public Instant createdAt() { return createdAt; }
    public MigrationState state() { return state; }

    public void advanceTo(MigrationState newState) {
        this.state = newState;
    }
}
