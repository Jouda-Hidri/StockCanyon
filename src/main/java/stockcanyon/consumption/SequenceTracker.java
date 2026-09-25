package stockcanyon.consumption;

import java.time.Instant;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Tracks the exchange's sequence counter: detects holes, and works out how far it is safe to
 * checkpoint.
 *
 * <p>Detection is the only thing that can prove the no-gaps requirement. A healthy socket and a
 * climbing quote count are both consistent with having missed a thousand messages; 41, 42, 44 is
 * not.
 *
 * <p>The checkpoint is the <b>contiguous prefix</b>, not the highest sequence seen. Given 40, 41,
 * 43 the safe position is 41: resuming there makes the exchange replay 42 and 43, and the primary
 * key discards 43 as a duplicate. Checkpointing 43 instead would detect the hole and then make it
 * permanent, because nothing would ever ask for 42 again. Quotes are still written as they arrive —
 * only the position lags.
 *
 * <p>Not thread-safe by design: driven only from the writer thread, in arrival order.
 */
public class SequenceTracker {

    /**
     * How many out-of-order sequences may pile up behind a hole before it is written off.
     *
     * <p>Without a bound, an exchange that legitimately skips a number stalls the checkpoint
     * forever: it never advances past the hole, every reconnect replays more, and this map grows
     * without limit. Writing the hole off keeps the service moving and leaves the gap recorded.
     */
    private static final int MAX_PENDING_AHEAD = 10_000;

    public enum Verdict {
        /** First message seen; nothing to compare against. */
        FIRST,
        /** Exactly one more than the last. */
        IN_ORDER,
        /** Already seen. Routine after a recovery, since replay rewinds to a timestamp. */
        DUPLICATE,
        /** Messages are missing between the contiguous prefix and this one. */
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

    /** The position it is safe to resume from: the end of the contiguous run. */
    public record Position(long sequence, Instant eventTime) {}

    private static final Observation FIRST = new Observation(Verdict.FIRST, 0, 0);
    private static final Observation IN_ORDER = new Observation(Verdict.IN_ORDER, 0, 0);
    private static final Observation DUPLICATE = new Observation(Verdict.DUPLICATE, 0, 0);

    /** Sequences seen while a hole is still open, held so the prefix can be extended when it fills. */
    private final NavigableMap<Long, Instant> ahead = new TreeMap<>();

    private long contiguousUpTo = -1;
    private Instant contiguousEventTime;

    public Observation observe(long sequence, Instant eventTime) {
        if (contiguousUpTo < 0) {
            contiguousUpTo = sequence;
            contiguousEventTime = eventTime;
            return FIRST;
        }
        if (sequence <= contiguousUpTo) {
            return DUPLICATE;
        }
        if (sequence == contiguousUpTo + 1) {
            contiguousUpTo = sequence;
            contiguousEventTime = eventTime;
            extendOverFilledHoles();
            return IN_ORDER;
        }
        // Ahead of the prefix: keep it, so the prefix can jump forward once the hole fills.
        if (ahead.putIfAbsent(sequence, eventTime) != null) {
            return DUPLICATE;
        }
        if (ahead.size() > MAX_PENDING_AHEAD) {
            writeOffTheHole();
        }
        return new Observation(Verdict.GAP, contiguousUpTo + 1, sequence - 1);
    }

    /** After the prefix advances, absorb anything already received that now continues it. */
    private void extendOverFilledHoles() {
        Instant next;
        while ((next = ahead.remove(contiguousUpTo + 1)) != null) {
            contiguousUpTo++;
            contiguousEventTime = next;
        }
    }

    /** Gives up on the oldest hole and moves the prefix to the next sequence actually received. */
    private void writeOffTheHole() {
        Map.Entry<Long, Instant> oldest = ahead.pollFirstEntry();
        contiguousUpTo = oldest.getKey();
        contiguousEventTime = oldest.getValue();
        extendOverFilledHoles();
    }

    /**
     * Where it is safe to resume from, or null before anything has arrived.
     *
     * <p>This, not the highest sequence seen, is what the checkpoint must record.
     */
    public Position safePosition() {
        return contiguousEventTime == null ? null : new Position(contiguousUpTo, contiguousEventTime);
    }

    /** Sequences received but not yet contiguous, i.e. sitting behind an open hole. */
    public int pendingAhead() {
        return ahead.size();
    }
}
