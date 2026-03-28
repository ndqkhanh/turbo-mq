package turbomq.testing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class SimulatedClockTest {

    private SimulatedClock clock;

    @BeforeEach
    void setUp() {
        clock = new SimulatedClock();
    }

    @Test
    void startsAtZero() {
        assertThat(clock.nowMs()).isEqualTo(0);
    }

    @Test
    void advanceByIncrementsTime() {
        clock.advanceBy(100);
        assertThat(clock.nowMs()).isEqualTo(100);

        clock.advanceBy(50);
        assertThat(clock.nowMs()).isEqualTo(150);
    }

    @Test
    void scheduledTaskFiresAtCorrectTime() {
        List<Long> firedAt = new ArrayList<>();
        clock.scheduleAt(50, () -> firedAt.add(clock.nowMs()));

        clock.advanceBy(49);
        assertThat(firedAt).isEmpty();

        clock.advanceBy(1);
        assertThat(firedAt).containsExactly(50L);
    }

    @Test
    void multipleTasksFireInChronologicalOrder() {
        List<String> order = new ArrayList<>();
        clock.scheduleAt(30, () -> order.add("B"));
        clock.scheduleAt(10, () -> order.add("A"));
        clock.scheduleAt(50, () -> order.add("C"));

        clock.advanceBy(50);
        assertThat(order).containsExactly("A", "B", "C");
    }

    @Test
    void scheduleAfterUsesRelativeDelay() {
        List<Long> firedAt = new ArrayList<>();
        clock.advanceBy(100);
        clock.scheduleAfter(25, () -> firedAt.add(clock.nowMs()));

        clock.advanceBy(25);
        assertThat(firedAt).containsExactly(125L);
    }

    @Test
    void advanceToNextFiresSingleTask() {
        List<String> order = new ArrayList<>();
        clock.scheduleAt(10, () -> order.add("first"));
        clock.scheduleAt(20, () -> order.add("second"));

        boolean fired = clock.advanceToNext();
        assertThat(fired).isTrue();
        assertThat(clock.nowMs()).isEqualTo(10);
        assertThat(order).containsExactly("first");
    }

    @Test
    void advanceToNextReturnsFalseWhenEmpty() {
        assertThat(clock.advanceToNext()).isFalse();
    }

    @Test
    void pendingTasksReportsCorrectCount() {
        clock.scheduleAt(10, () -> {});
        clock.scheduleAt(20, () -> {});
        assertThat(clock.pendingTasks()).isEqualTo(2);

        clock.advanceBy(15);
        assertThat(clock.pendingTasks()).isEqualTo(1);
    }

    @Test
    void taskScheduledDuringAdvanceFiresIfWithinWindow() {
        List<String> order = new ArrayList<>();
        clock.scheduleAt(10, () -> {
            order.add("first");
            clock.scheduleAt(15, () -> order.add("chained"));
        });

        clock.advanceBy(20);
        assertThat(order).containsExactly("first", "chained");
    }

    @Test
    void scheduleInPastThrows() {
        clock.advanceBy(100);
        assertThatThrownBy(() -> clock.scheduleAt(50, () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
