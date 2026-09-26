package stockcanyon.consumption;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import stockcanyon.consumption.SequenceTracker.Verdict;

class SequenceTrackerTest {

    @Test
    @DisplayName("a contiguous run reports no gaps")
    void contiguousRun() {
        SequenceTracker tracker = new SequenceTracker();
        assertThat(tracker.observe(10, at(10)).verdict()).isEqualTo(Verdict.FIRST);
        assertThat(tracker.observe(11, at(11)).verdict()).isEqualTo(Verdict.IN_ORDER);
        assertThat(tracker.observe(12, at(12)).verdict()).isEqualTo(Verdict.IN_ORDER);
        assertThat(tracker.safePosition().sequence()).isEqualTo(12);
        assertThat(tracker.oldestHole()).isEmpty();
    }

    @Test
    @DisplayName("a hole is reported with its exact bounds")
    void detectsGap() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(10, at(10));
        SequenceTracker.Observation observation = tracker.observe(15, at(15));

        assertThat(observation.verdict()).isEqualTo(Verdict.GAP);
        assertThat(observation.missingFrom()).isEqualTo(11);
        assertThat(observation.missingTo()).isEqualTo(14);
        assertThat(observation.missingCount()).isEqualTo(4);
        assertThat(tracker.oldestHole()).contains(new SequenceTracker.Hole(11, 14));
    }

    /** Replay re-delivers stored messages; those are not gaps. */
    @Test
    @DisplayName("replayed messages are duplicates, not gaps")
    void replayIsNotAGap() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(10, at(10));
        tracker.observe(11, at(11));
        tracker.observe(12, at(12));

        assertThat(tracker.observe(11, at(11)).verdict()).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.observe(12, at(12)).verdict()).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.observe(13, at(13)).verdict()).isEqualTo(Verdict.IN_ORDER);
    }

    /**
     * 40, 41, 44, then 43 late. 43 is a backfill, not a second gap: counting it as one would report
     * three missing where only 42 is.
     */
    @Test
    @DisplayName("40, 41, 44, 43: one hole, shrinking from two missing to one, closed by the replay")
    void lateArrivalShrinksTheHole() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(40, at(40));
        tracker.observe(41, at(41));

        SequenceTracker.Observation gap = tracker.observe(44, at(44));
        assertThat(gap.verdict()).isEqualTo(Verdict.GAP);
        assertThat(gap.missingCount()).isEqualTo(2);
        assertThat(tracker.outstanding()).isEqualTo(2);
        assertThat(tracker.safePosition().sequence()).isEqualTo(41);

        assertThat(tracker.observe(43, at(43)).verdict()).isEqualTo(Verdict.BACKFILL);
        assertThat(tracker.outstanding()).as("only 42 is still missing").isEqualTo(1);
        assertThat(tracker.oldestHole()).contains(new SequenceTracker.Hole(42, 42));
        assertThat(tracker.safePosition().sequence())
                .as("resuming past 42 would make it permanent")
                .isEqualTo(41);

        // The forced replay from 41 re-delivers 41..44; 42 closes the hole.
        assertThat(tracker.observe(41, at(41)).verdict()).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.observe(42, at(42)).verdict()).isEqualTo(Verdict.BACKFILL);
        assertThat(tracker.observe(43, at(43)).verdict()).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.observe(44, at(44)).verdict()).isEqualTo(Verdict.DUPLICATE);

        assertThat(tracker.safePosition().sequence()).isEqualTo(44);
        assertThat(tracker.outstanding()).isZero();
        assertThat(tracker.oldestHole()).isEmpty();
        assertThat(tracker.writtenOff()).isZero();
    }

    @Test
    @DisplayName("new messages above an open hole are in order, not further gaps")
    void inOrderAboveAnOpenHole() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(40, at(40));
        tracker.observe(42, at(42));

        assertThat(tracker.observe(43, at(43)).verdict()).isEqualTo(Verdict.IN_ORDER);
        assertThat(tracker.outstanding()).isEqualTo(1);
    }

    @Test
    @DisplayName("filling part of a gap does not advance the safe position past it")
    void partialFillDoesNotAdvance() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(10, at(10));
        tracker.observe(20, at(20));          // gap 11..19
        tracker.observe(12, at(12));          // fills one of the nine, not the first

        assertThat(tracker.safePosition().sequence())
                .as("11 is still missing, so 10 is the last safe position")
                .isEqualTo(10);
        assertThat(tracker.pendingAhead()).isEqualTo(2);
        assertThat(tracker.outstanding()).isEqualTo(8);
    }

    @Test
    @DisplayName("the prefix jumps over sequences already received once the hole fills")
    void prefixJumpsWhenTheHoleFills() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(40, at(40));
        tracker.observe(43, at(43));
        tracker.observe(44, at(44));
        assertThat(tracker.safePosition().sequence()).isEqualTo(40);

        tracker.observe(41, at(41));
        assertThat(tracker.safePosition().sequence()).isEqualTo(41);
        tracker.observe(42, at(42));

        assertThat(tracker.safePosition().sequence())
                .as("43 and 44 were already held, so the prefix should run straight to 44")
                .isEqualTo(44);
        assertThat(tracker.pendingAhead()).isZero();
    }

    @Test
    @DisplayName("the safe position carries the event time of its own quote")
    void positionCarriesItsEventTime() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(40, at(40));
        tracker.observe(41, at(41));
        tracker.observe(43, at(43));

        assertThat(tracker.safePosition().eventTime()).isEqualTo(at(41));
    }

    @Test
    @DisplayName("writing off a hole moves the prefix to the next message received, and counts the loss")
    void writeOff() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(40, at(40));
        tracker.observe(41, at(41));
        tracker.observe(44, at(44));
        tracker.observe(45, at(45));

        assertThat(tracker.writeOffOldestHole()).isEqualTo(2);
        assertThat(tracker.safePosition().sequence()).isEqualTo(45);
        assertThat(tracker.writtenOff()).isEqualTo(2);
        assertThat(tracker.outstanding()).isZero();
        assertThat(tracker.oldestHole()).isEmpty();
    }

    /** Seeded from the checkpoint, a hole that opened across a restart is still a hole. */
    @Test
    @DisplayName("a tracker seeded from the checkpoint detects a gap at the first message")
    void seededFromCheckpoint() {
        SequenceTracker tracker = new SequenceTracker(41, at(41));

        assertThat(tracker.observe(41, at(41)).verdict()).isEqualTo(Verdict.DUPLICATE);
        SequenceTracker.Observation observation = tracker.observe(45, at(45));
        assertThat(observation.verdict()).isEqualTo(Verdict.GAP);
        assertThat(observation.missingFrom()).isEqualTo(42);
        assertThat(tracker.safePosition().sequence()).isEqualTo(41);
    }

    @Test
    @DisplayName("a counter that restarts while time moves on is a reset, not endless duplicates")
    void sequenceReset() {
        SequenceTracker tracker = new SequenceTracker(5_000, at(5_000));

        assertThat(tracker.observe(0, at(9_000)).verdict()).isEqualTo(Verdict.RESET);
        assertThat(tracker.observe(1, at(9_001)).verdict()).isEqualTo(Verdict.IN_ORDER);
        assertThat(tracker.safePosition().sequence()).isEqualTo(1);
    }

    /** The checkpoint is stored at microseconds; the replayed message it names may carry nanoseconds. */
    @Test
    @DisplayName("the replayed checkpoint message is a duplicate, not a reset, despite sub-µs precision")
    void checkpointPrecisionIsNotAReset() {
        Instant exact = Instant.parse("2026-09-25T10:00:00.123456789Z");
        SequenceTracker tracker = new SequenceTracker(41, Instant.parse("2026-09-25T10:00:00.123456Z"));

        assertThat(tracker.observe(41, exact).verdict()).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.writtenOff()).isZero();
    }

    @Test
    @DisplayName("a reset while a hole is open writes the hole off, and says so")
    void resetWritesOffOpenHole() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(40, at(40));
        tracker.observe(43, at(43));   // 41, 42 missing

        assertThat(tracker.observe(0, at(9_000)).verdict()).isEqualTo(Verdict.RESET);
        assertThat(tracker.writtenOff()).isEqualTo(2);
    }

    private static Instant at(long sequence) {
        return Instant.parse("2026-09-25T10:00:00Z").plusMillis(sequence);
    }
}
