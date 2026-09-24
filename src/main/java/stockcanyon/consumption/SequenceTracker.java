package stockcanyon.consumption;

/**
 * Detects holes in the exchange's sequence counter.
 *
 * <p>The only thing that can prove the no-gaps requirement: a healthy socket and a climbing quote
 * count are both consistent with having missed a thousand messages; 41, 42, 44 is not.
 *
 * <p>Not thread-safe by design — driven only from the writer thread, in arrival order.
 */
public class SequenceTracker {

    public enum Verdict {
        /** First message seen; nothing to compare against. */
        FIRST,
        /** Exactly one more than the last. */
        IN_ORDER,
        /** Already seen. Routine after a recovery, since replay rewinds to a timestamp. */
        DUPLICATE,
        /** Messages are missing between the last one and this one. */
        GAP
    }

    /**
     * @param missingFrom first sequence not received, valid only when the verdict is {@link
     *     Verdict#GAP}
     * @param missingTo last sequence not received
     */
    public record Observation(Verdict verdict, long missingFrom, long missingTo) {

        public long missingCount() {
            return verdict == Verdict.GAP ? missingTo - missingFrom + 1 : 0;
        }
    }

    private static final Observation FIRST = new Observation(Verdict.FIRST, 0, 0);
    private static final Observation IN_ORDER = new Observation(Verdict.IN_ORDER, 0, 0);
    private static final Observation DUPLICATE = new Observation(Verdict.DUPLICATE, 0, 0);

    private long lastSequence = -1;

    public Observation observe(long sequence) {
        if (lastSequence < 0) {
            lastSequence = sequence;
            return FIRST;
        }
        if (sequence == lastSequence + 1) {
            lastSequence = sequence;
            return IN_ORDER;
        }
        if (sequence <= lastSequence) {
            // Replay overlapping what is already stored. Expected, and not a gap: the high-water
            // mark must not move backwards, or the next genuine gap is measured from the wrong
            // place and reported as far larger than it is.
            return DUPLICATE;
        }
        Observation gap = new Observation(Verdict.GAP, lastSequence + 1, sequence - 1);
        lastSequence = sequence;
        return gap;
    }

    /** Highest sequence seen, or -1 before anything has arrived. */
    public long lastSequence() {
        return lastSequence;
    }

}
