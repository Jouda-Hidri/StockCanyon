package stockcanyon.consumption;

import java.time.Instant;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Tracks the exchange's sequence counter: detects holes, knows which are still open, and works
 * out how far it is safe to checkpoint.
 *
 * <p>Detection is the only thing that can prove the no-gaps requirement. A healthy socket and a
 * climbing quote count are both consistent with having missed a thousand messages; 41, 42, 44 is
 * not.
 *
 * <p>The checkpoint is the <b>contiguous prefix</b>, not the highest sequence seen. Given 40, 41,
 * 44 the safe position is 41. Quotes are still written as they arrive — only the position lags —
 * so when a replay from 41 re-delivers 42..44, 42 and 43 fill the hole and 44 is discarded by the
 * primary key. Checkpointing 44 instead would detect the hole and then make it permanent.
 *
 * <p>This class only keeps the books. Deciding when an open hole warrants a replay, and when to
 * give up on it, is {@link QuoteConsumer}'s job.
 *
 * <p>Not thread-safe by design: driven only from the writer thread, in arrival order.
 */
public class SequenceTracker {

    /**
     * How many out-of-order sequences may pile up behind a hole before it is written off
     * regardless. A memory bound, not the normal path: holes are normally resolved by replay long
     * before this.
     */
    static final int MAX_PENDING_AHEAD = 100_000;

    public enum Verdict {
        /** First message seen and no checkpoint to compare against. */
        FIRST,
        /** Next in line, or next after the newest while an older hole is still open. */
        IN_ORDER,
        /** Already seen. Routine after a replay, since replay rewinds to a timestamp. */
        DUPLICATE,
        /** Opens a new hole: messages between the newest seen and this one are missing. */
        GAP,
        /** Arrived late, inside a hole that was already open. Fills it, fully or partly. */
        BACKFILL,
        /**
         * The counter went backwards while time went forwards: the exchange restarted its
         * numbering. Tracking starts over from this message, and any hole still open is written off.
         */
        RESET
    }

    /**
     * @param missingFrom first sequence of a new hole, valid only when the verdict is {@link
     *     Verdict#GAP}
     * @param missingTo last sequence of that hole
     */
    public record Observation(Verdict verdict, long missingFrom, long missingTo) {

        public long missingCount() {
            return verdict == Verdict.GAP ? missingTo - missingFrom + 1 : 0;
        }
    }

    /** The position it is safe to resume from: the end of the contiguous run. */
    public record Position(long sequence, Instant eventTime) {}

    /** The oldest missing range: everything from {@code from} to {@code to} inclusive. */
    public record Hole(long from, long to) {

        public long size() {
            return to - from + 1;
        }
    }

    private static final Observation FIRST = new Observation(Verdict.FIRST, 0, 0);
    private static final Observation IN_ORDER = new Observation(Verdict.IN_ORDER, 0, 0);
    private static final Observation DUPLICATE = new Observation(Verdict.DUPLICATE, 0, 0);
    private static final Observation BACKFILL = new Observation(Verdict.BACKFILL, 0, 0);
    private static final Observation RESET = new Observation(Verdict.RESET, 0, 0);

    /** Sequences received above an open hole, held so the prefix can jump when the hole fills. */
    private final NavigableMap<Long, Instant> ahead = new TreeMap<>();

    private long contiguousUpTo = -1;
    private Instant contiguousEventTime;
    private long highestSeen = -1;
    private Instant highestEventTime;
    private long writtenOff;

    public SequenceTracker() {
    }

    /**
     * Starts from a stored checkpoint, so a hole that opens across a restart is still detected:
     * with the checkpoint at 41, a replay whose first message is 45 is a gap of three, not a
     * {@link Verdict#FIRST}.
     */
    public SequenceTracker(long checkpointSequence, Instant checkpointEventTime) {
        this.contiguousUpTo = checkpointSequence;
        this.contiguousEventTime = checkpointEventTime;
        this.highestSeen = checkpointSequence;
        this.highestEventTime = checkpointEventTime;
    }

    public Observation observe(long sequence, Instant eventTime) {
        if (contiguousUpTo < 0) {
            restartAt(sequence, eventTime);
            return FIRST;
        }
        if (sequence <= contiguousUpTo) {
            // A genuine duplicate is never newer than everything already seen: the stream is ordered
            // by event time, so a replayed message carries the time it had the first time round.
            // Compared at microseconds, the precision the seeding checkpoint was stored at, or a
            // replayed checkpoint message with nanoseconds would look newer than itself.
            if (micros(eventTime).isAfter(micros(highestEventTime))) {
                writtenOff += outstanding();
                ahead.clear();
                restartAt(sequence, eventTime);
                return RESET;
            }
            return DUPLICATE;
        }
        if (highestEventTime == null || eventTime.isAfter(highestEventTime)) {
            highestEventTime = eventTime;
        }
        if (sequence == contiguousUpTo + 1) {
            boolean holeWasOpen = !ahead.isEmpty();
            contiguousUpTo = sequence;
            contiguousEventTime = eventTime;
            highestSeen = Math.max(highestSeen, sequence);
            extendOverFilledHoles();
            return holeWasOpen ? BACKFILL : IN_ORDER;
        }
        // Above the prefix: some hole is open, or is opening now.
        if (ahead.putIfAbsent(sequence, eventTime) != null) {
            return DUPLICATE;
        }
        Observation observation;
        if (sequence < highestSeen) {
            observation = BACKFILL;
        } else if (sequence == highestSeen + 1) {
            observation = IN_ORDER;
        } else {
            observation = new Observation(Verdict.GAP, highestSeen + 1, sequence - 1);
        }
        highestSeen = Math.max(highestSeen, sequence);
        if (ahead.size() > MAX_PENDING_AHEAD) {
            writeOffOldestHole();
        }
        return observation;
    }

    /** The oldest open hole, if any — the one holding the checkpoint back. */
    public Optional<Hole> oldestHole() {
        if (ahead.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Hole(contiguousUpTo + 1, ahead.firstKey() - 1));
    }

    /**
     * Gives up on the oldest hole: the prefix moves to the next sequence actually received, so the
     * checkpoint can advance past it. What was in the hole is counted as lost.
     *
     * @return how many sequences were written off
     */
    public long writeOffOldestHole() {
        Map.Entry<Long, Instant> next = ahead.pollFirstEntry();
        if (next == null) {
            return 0;
        }
        long lost = next.getKey() - contiguousUpTo - 1;
        writtenOff += lost;
        contiguousUpTo = next.getKey();
        contiguousEventTime = next.getValue();
        extendOverFilledHoles();
        return lost;
    }

    private static Instant micros(Instant instant) {
        return instant.truncatedTo(java.time.temporal.ChronoUnit.MICROS);
    }

    private void restartAt(long sequence, Instant eventTime) {
        contiguousUpTo = sequence;
        contiguousEventTime = eventTime;
        highestSeen = sequence;
        highestEventTime = eventTime;
    }

    /** After the prefix advances, absorb anything already received that now continues it. */
    private void extendOverFilledHoles() {
        Instant next;
        while ((next = ahead.remove(contiguousUpTo + 1)) != null) {
            contiguousUpTo++;
            contiguousEventTime = next;
        }
    }

    /**
     * Where it is safe to resume from, or null before anything has arrived.
     *
     * <p>This, not the highest sequence seen, is what the checkpoint must record.
     */
    public Position safePosition() {
        return contiguousEventTime == null ? null : new Position(contiguousUpTo, contiguousEventTime);
    }

    /**
     * Sequences currently missing: issued by the exchange, below the newest seen, not received.
     * Falls as replay fills holes, unlike a count of gaps detected.
     */
    public long outstanding() {
        return ahead.isEmpty() ? 0 : (highestSeen - contiguousUpTo) - ahead.size();
    }

    /** Sequences given up on. Anything above zero is a permanent hole in the stored history. */
    public long writtenOff() {
        return writtenOff;
    }

    /** Sequences received but not yet contiguous, i.e. sitting behind an open hole. */
    public int pendingAhead() {
        return ahead.size();
    }

    /** How far the checkpoint trails the newest message seen. */
    public long checkpointLagSequences() {
        return highestSeen < 0 ? 0 : highestSeen - contiguousUpTo;
    }
}
