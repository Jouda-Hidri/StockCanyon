package stockcanyon.simulator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import stockcanyon.MarketDataProperties;

/**
 * Bounded append-only log keyed by sequence. What makes replay possible.
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

    /** Thrown when a cursor has fallen off the back of the window: the data is genuinely gone. */
    public static class EvictedException extends RuntimeException {
        public EvictedException(String message) {
            super(message);
        }
    }

    /** How long a reader waits between polls while the log is idle. */
    private static final Duration POLL_INTERVAL = Duration.ofMillis(20);

    private final ConcurrentNavigableMap<Long, ExchangeMessage> messages = new ConcurrentSkipListMap<>();
    private final AtomicLong nextSequence = new AtomicLong();
    private final int capacity;

    public QuoteLog(MarketDataProperties properties) {
        this.capacity = properties.simulator().retainedQuotes();
    }

    /** Appends a message and returns the sequence it was assigned. */
    public long append(LongFunction<ExchangeMessage> factory) {
        long sequence = nextSequence.getAndIncrement();
        messages.put(sequence, factory.apply(sequence));
        // Evict by key rather than by size: ConcurrentSkipListMap.size() walks the whole map.
        messages.headMap(sequence - capacity + 1).clear();
        return sequence;
    }

    /**
     * Reads up to {@code max} messages from {@code cursor}, waiting up to {@code timeout}.
     *
     * @throws EvictedException if {@code cursor} names data already evicted
     */
    public Batch read(long cursor, int max, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (messages.tailMap(cursor).isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(POLL_INTERVAL.toMillis());
        }
        Map.Entry<Long, ExchangeMessage> oldest = messages.firstEntry();
        if (oldest != null && cursor < oldest.getKey()) {
            throw new EvictedException("cursor %d is older than the retained window (oldest=%d)"
                    .formatted(cursor, oldest.getKey()));
        }
        List<ExchangeMessage> page = messages.tailMap(cursor).values().stream().limit(max).toList();
        return new Batch(cursor + page.size(), page);
    }

    /**
     * Resolves a {@code checkpoint_timestamp} to a cursor: the first message at or after it.
     *
     * <p>Inclusive on purpose. Several quotes can share an instant, so an exclusive boundary would
     * drop them all. The consumer deduplicates, so a duplicate is always preferred to a gap.
     *
     * @throws EvictedException if the checkpoint predates the retained window
     */
    public long cursorAtOrAfter(Instant checkpoint) {
        Map.Entry<Long, ExchangeMessage> oldest = messages.firstEntry();
        if (oldest != null && oldest.getValue().timestamp().isAfter(checkpoint)) {
            throw new EvictedException("checkpoint %s predates the retained window (oldest=%s)"
                    .formatted(checkpoint, oldest.getValue().timestamp()));
        }
        // Sequences are assigned in publish order, so the log is already sorted by timestamp.
        return messages.entrySet().stream()
                .filter(entry -> !entry.getValue().timestamp().isBefore(checkpoint))
                .mapToLong(Map.Entry::getKey)
                .findFirst()
                .orElse(nextSequence.get());
    }

    /** Where a "subscribe from now" reader starts. */
    public long tailCursor() {
        return nextSequence.get();
    }

    public long published() {
        return tailCursor();
    }
}
