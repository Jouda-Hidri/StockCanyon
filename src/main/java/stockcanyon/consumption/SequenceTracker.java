package stockcanyon.consumption;

/**
 * Watches a contiguous exchange sequence for holes.
 *
 * <p>This is the only thing in the service that can actually prove the "no gaps" requirement. A
 * healthy socket, a successful reconnect and a rising quote count are all consistent with having
 * missed a thousand messages; a counter that goes 41, 42, 44 is not. Everything else in the
 * ingestion path is designed to prevent gaps — this is what detects the ones that happen anyway.
 *
 * <p>Deliberately not thread-safe. It is driven only from the single flush thread, in arrival
 * order, and making it concurrent would invite it being called from somewhere that cannot
 * guarantee that order, at which point every result it produces is meaningless.
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

    /**
     * Forgets the position, so the next message is treated as a first.
     *
     * <p>Used when resuming a feed that cannot replay: the stream restarts at the live edge, so the
     * discontinuity there is already known and separately recorded, and comparing across it would
     * only report it a second time as an enormous spurious gap.
     */
    public void reset() {
        lastSequence = -1;
    }
}
