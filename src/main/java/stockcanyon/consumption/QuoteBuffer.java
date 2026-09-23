package stockcanyon.consumption;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import stockcanyon.Isin;
import stockcanyon.Quote;

/**
 * Bounded hand-off between the socket and the writer thread.
 *
 * <p>Bounded on purpose: when full, {@link #put} blocks, the socket stops being read, and the
 * exchange is throttled. An unbounded queue would instead absorb the backlog until the process
 * dies, losing everything in it.
 */
public class QuoteBuffer {

    private final BlockingQueue<Quote> queue;
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong blockedNanos = new AtomicLong();
    private final AtomicLong blockedCount = new AtomicLong();

    public QuoteBuffer(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /** Enqueues, blocking while full. Blocking time is measured: it is the first sign of lag. */
    public void put(Quote quote) throws InterruptedException {
        if (!queue.offer(quote)) {
            long start = System.nanoTime();
            queue.put(quote);
            blockedNanos.addAndGet(System.nanoTime() - start);
            blockedCount.incrementAndGet();
        }
        accepted.incrementAndGet();
    }

    /** Takes up to {@code max} quotes, waiting up to {@code timeout} for the first. */
    public List<Quote> drain(int max, Duration timeout) throws InterruptedException {
        Quote first = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (first == null) {
            return List.of();
        }
        List<Quote> batch = new ArrayList<>(Math.min(max, 1024));
        batch.add(first);
        queue.drainTo(batch, max - 1);
        return batch;
    }

    /**
     * Reduces a batch to at most one quote per instrument, keeping the newest.
     *
     * <p>Makes a flush cost one latest-quote write per instrument that moved, whether it moved once
     * or 5000 times. Also required: PostgreSQL rejects an {@code ON CONFLICT DO UPDATE} touching one
     * row twice in a command. History keeps every quote.
     */
    public static Collection<Quote> coalesceLatest(List<Quote> batch) {
        Map<Isin, Quote> newest = new LinkedHashMap<>();
        for (Quote quote : batch) {
            newest.merge(quote.isin(), quote, (existing, incoming) ->
                    incoming.isNewerThan(existing) ? incoming : existing);
        }
        return newest.values();
    }

    /**
     * Drops quotes sharing a primary key within one batch.
     *
     * <p>The driver rewrites a batched insert into one multi-row statement, where duplicate keys
     * collide with themselves instead of conflicting harmlessly. Replay makes that routine.
     */
    public static List<Quote> dedupeByKey(List<Quote> batch) {
        Map<String, Quote> unique = new LinkedHashMap<>(batch.size());
        for (Quote quote : batch) {
            unique.putIfAbsent(
                    quote.isin().value() + '|' + quote.eventTime() + '|' + quote.sequence(), quote);
        }
        return unique.size() == batch.size() ? batch : new ArrayList<>(unique.values());
    }

    public int depth() {
        return queue.size();
    }

    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    public long acceptedCount() {
        return accepted.get();
    }

    public Duration timeBlocked() {
        return Duration.ofNanos(blockedNanos.get());
    }

    public long blockedCount() {
        return blockedCount.get();
    }
}
