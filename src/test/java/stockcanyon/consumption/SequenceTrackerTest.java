package stockcanyon.consumption;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import java.time.Instant;

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

    /** A partial fill is still a hole: the safe position may not step over 11. */
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
    }


    @Test
    @DisplayName("the safe position is the contiguous prefix, not the newest sequence")
    void safePositionHoldsAtTheHole() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(40, at(40));
        tracker.observe(41, at(41));
        tracker.observe(43, at(43));   // 42 is missing

        assertThat(tracker.safePosition().sequence())
                .as("resuming from 43 would make the hole at 42 permanent")
                .isEqualTo(41);
        assertThat(tracker.pendingAhead()).isEqualTo(1);
    }

    @Test
    @DisplayName("the prefix jumps over sequences already received once the hole fills")
    void prefixJumpsWhenTheHoleFills() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(40, at(40));
        tracker.observe(43, at(43));
        tracker.observe(44, at(44));
        assertThat(tracker.safePosition().sequence()).isEqualTo(40);

        // replay delivers the missing 41 and 42
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

    private static Instant at(long sequence) {
        return Instant.parse("2026-09-25T10:00:00Z").plusMillis(sequence);
    }
}
