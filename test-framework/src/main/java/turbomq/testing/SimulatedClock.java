package turbomq.testing;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Deterministic virtual clock for simulation testing.
 *
 * All time advances are explicit — no wall-clock dependency.
 * Supports scheduling callbacks at future ticks, enabling
 * deterministic testing of election timeouts and heartbeat intervals.
 */
public final class SimulatedClock {

    private long currentTimeMs;
    private final PriorityQueue<ScheduledTask> taskQueue =
            new PriorityQueue<>(Comparator.comparingLong(ScheduledTask::triggerTimeMs));

    public SimulatedClock() {
        this(0);
    }

    public SimulatedClock(long initialTimeMs) {
        this.currentTimeMs = initialTimeMs;
    }

    /** Current virtual time in milliseconds. */
    public long nowMs() {
        return currentTimeMs;
    }

    /** Schedule a callback to fire at a specific absolute time. */
    public void scheduleAt(long timeMs, Runnable callback) {
        if (timeMs < currentTimeMs) {
            throw new IllegalArgumentException(
                    "Cannot schedule in the past: " + timeMs + " < " + currentTimeMs);
        }
        taskQueue.add(new ScheduledTask(timeMs, callback));
    }

    /** Schedule a callback to fire after a relative delay from now. */
    public void scheduleAfter(long delayMs, Runnable callback) {
        scheduleAt(currentTimeMs + delayMs, callback);
    }

    /**
     * Advance time by the given duration, firing all scheduled tasks
     * whose trigger time falls within [now, now + deltaMs].
     * Tasks are fired in chronological order.
     *
     * @return the number of tasks fired
     */
    public int advanceBy(long deltaMs) {
        long targetTime = currentTimeMs + deltaMs;
        int fired = 0;

        while (!taskQueue.isEmpty() && taskQueue.peek().triggerTimeMs() <= targetTime) {
            ScheduledTask task = taskQueue.poll();
            currentTimeMs = task.triggerTimeMs();
            task.callback().run();
            fired++;
        }

        currentTimeMs = targetTime;
        return fired;
    }

    /**
     * Advance time to the next scheduled task and fire it.
     *
     * @return true if a task was fired, false if no tasks are pending
     */
    public boolean advanceToNext() {
        if (taskQueue.isEmpty()) {
            return false;
        }
        ScheduledTask task = taskQueue.poll();
        currentTimeMs = task.triggerTimeMs();
        task.callback().run();
        return true;
    }

    /** Number of pending scheduled tasks. */
    public int pendingTasks() {
        return taskQueue.size();
    }

    private record ScheduledTask(long triggerTimeMs, Runnable callback) {}
}
