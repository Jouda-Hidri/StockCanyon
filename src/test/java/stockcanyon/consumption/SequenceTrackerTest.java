package stockcanyon.consumption;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import stockcanyon.consumption.SequenceTracker.Verdict;

class SequenceTrackerTest {

    @Test
    @DisplayName("a contiguous run reports no gaps")
    void contiguousRun() {
        SequenceTracker tracker = new SequenceTracker();
        assertThat(tracker.observe(10).verdict()).isEqualTo(Verdict.FIRST);
        assertThat(tracker.observe(11).verdict()).isEqualTo(Verdict.IN_ORDER);
        assertThat(tracker.observe(12).verdict()).isEqualTo(Verdict.IN_ORDER);
        assertThat(tracker.lastSequence()).isEqualTo(12);
    }

    @Test
    @DisplayName("a hole is reported with its exact bounds")
    void detectsGap() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(10);
        SequenceTracker.Observation observation = tracker.observe(15);

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
        tracker.observe(10);
        tracker.observe(11);
        tracker.observe(12);

        assertThat(tracker.observe(11).verdict()).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.observe(12).verdict()).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.observe(13).verdict()).isEqualTo(Verdict.IN_ORDER);
    }

    /** Otherwise the next real gap is measured from the rewound position and over-reported. */
    @Test
    @DisplayName("a duplicate does not rewind the high-water mark")
    void duplicateDoesNotRewind() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(10);
        tracker.observe(20);          // gap 11..19
        tracker.observe(12);          // late duplicate from the replayed window

        assertThat(tracker.lastSequence()).isEqualTo(20);
        assertThat(tracker.observe(21).verdict()).isEqualTo(Verdict.IN_ORDER);
    }

}
