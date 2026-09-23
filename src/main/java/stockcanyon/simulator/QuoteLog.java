package stockcanyon.simulator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Bounded append-only log addressed by a monotonic index. What makes replay possible.
 *
 * <p>Replay and live streaming are the same operation — a reader holds a cursor and pulls forward.
 * That removes the race in "replay the backlog, then subscribe", where quotes published between
 * the two fall into the seam.
 */
@Component
@ConditionalOnProperty(prefix = "marketdata.simulator", name = "enabled", havingValue = "true")
public class QuoteLog {

    /** Outcome of a read. {@code messages} is empty if the wait elapsed with nothing appended. */
    public record Batch(long nextCursor, List<ExchangeMessage> messages) {}

    /** Thrown when a cursor has fallen off the back of the ring: the data is genuinely gone. */
    public static class EvictedException extends RuntimeException {
        public EvictedException(String message) {
            super(message);
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition appended = lock.newCondition();
    private final ExchangeMessage[] ring;
    private final int capacity;

    /** Index the next message will be given. Doubles as the exchange's sequence number. */
    private long nextIndex;

    /** Index of the oldest message still retained. */
    private long baseIndex;

    public QuoteLog(stockcanyon.MarketDataProperties properties) {
        this.capacity = properties.getSimulator().getRetainedQuotes();
        this.ring = new ExchangeMessage[capacity];
    }

    /** Appends a message and returns the sequence it was assigned. */
    public long append(LongFunction<ExchangeMessage> factory) {
        lock.lock();
        try {
            long sequence = nextIndex;
            ring[slot(sequence)] = factory.apply(sequence);
            nextIndex = sequence + 1;
            if (nextIndex - baseIndex > capacity) {
                baseIndex = nextIndex - capacity;
            }
            appended.signalAll();
            return sequence;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads up to {@code max} messages from {@code cursor}, waiting up to {@code timeout}.
     *
     * @throws EvictedException if {@code cursor} names data already overwritten
     */
    public Batch read(long cursor, int max, Duration timeout) throws InterruptedException {
        lock.lock();
        try {
            long remaining = timeout.toNanos();
            while (cursor >= nextIndex && remaining > 0) {
                remaining = appended.awaitNanos(remaining);
            }
            if (cursor < baseIndex) {
                throw new EvictedException("cursor %d is older than the retained window (oldest=%d)"
                        .formatted(cursor, baseIndex));
            }
            int count = (int) Math.min(max, nextIndex - cursor);
            if (count <= 0) {
                return new Batch(cursor, List.of());
            }
            List<ExchangeMessage> out = new ArrayList<>(count);
            for (long i = cursor; i < cursor + count; i++) {
                out.add(ring[slot(i)]);
            }
            return new Batch(cursor + count, out);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resolves a {@code checkpoint_timestamp} to a cursor: the first message at or after it.
     *
     * <p>Inclusive on purpose. Several quotes can share an instant, so an exclusive boundary would drop
     * them all. The consumer deduplicates, so a duplicate is always preferred to a gap.
     *
     * @throws EvictedException if the checkpoint predates the retained window
     */
    public long cursorAtOrAfter(Instant checkpoint) {
        lock.lock();
        try {
            if (baseIndex < nextIndex) {
                ExchangeMessage oldest = ring[slot(baseIndex)];
                if (oldest.timestamp().isAfter(checkpoint)) {
                    throw new EvictedException(
                            "checkpoint %s predates the retained window (oldest=%s)"
                                    .formatted(checkpoint, oldest.timestamp()));
                }
            }
            // Assigned in append order, so the log is sorted.
            long low = baseIndex;
            long high = nextIndex;
            while (low < high) {
                long mid = low + (high - low) / 2;
                if (ring[slot(mid)].timestamp().isBefore(checkpoint)) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        } finally {
            lock.unlock();
        }
    }

    /** Where a "subscribe from now" reader starts. */
    public long tailCursor() {
        lock.lock();
        try {
            return nextIndex;
        } finally {
            lock.unlock();
        }
    }

    public long published() {
        return tailCursor();
    }

    private int slot(long index) {
        return (int) Math.floorMod(index, capacity);
    }
}
