package turbomq.broker;

/**
 * States of a partition migration lifecycle.
 */
public enum MigrationState {
    /** Learner added to target broker, replication starting. */
    ADDING_LEARNER,
    /** Learner is catching up with the leader's log. */
    CATCHING_UP,
    /** Learner has caught up, being promoted to voter. */
    PROMOTING,
    /** Old replica is being removed. */
    REMOVING_OLD,
    /** Migration completed successfully. */
    COMPLETED,
    /** Migration failed. */
    FAILED
}
