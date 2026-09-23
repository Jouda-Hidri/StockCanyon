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
 * The hand-off between the socket and the database.
 *
 * <p>It exists because the two sides have incompatible rhythms. Quotes arrive in bursts, dictated
 * by the market; the database is happiest with steady, batched writes. Writing each quote as it
 * lands would mean a round trip per message, and at a few hundred messages a second the commit
 * overhead alone becomes the bottleneck — with a burst on one instrument able to stall every other
 * instrument behind it.
 *
 * <p>The queue is bounded on purpose. An unbounded one does not remove the limit, it only moves the
 * failure from a place where it can be handled to a place where it cannot: the service absorbs the
 * backlog into the heap and eventually dies, losing the entire buffer rather than slowing down.
 * Bounded, a full queue blocks the feed's frame handler, which stops the socket being read, which
 * closes the TCP receive window and pushes the pressure back to the exchange where it belongs.
 */
public class QuoteBuffer {

    private final BlockingQueue<Quote> queue;
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong blockedNanos = new AtomicLong();
    private final AtomicLong blockedCount = new AtomicLong();

    public QuoteBuffer(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Enqueues a quote, blocking while the buffer is full.
     *
     * <p>Time spent blocked is measured rather than merely endured. It is the earliest honest
     * signal that the pipeline cannot keep up with the feed, and it appears long before the lag
     * shows up in the data.
     */
    public void put(Quote quote) throws InterruptedException {
        if (!queue.offer(quote)) {
            long start = System.nanoTime();
            queue.put(quote);
            blockedNanos.addAndGet(System.nanoTime() - start);
            blockedCount.incrementAndGet();
        }
        accepted.incrementAndGet();
    }

    /**
     * Takes up to {@code max} quotes, waiting up to {@code timeout} for the first.
     *
     * <p>The wait is what keeps a quiet market from being written one row per transaction, and the
     * cap is what keeps a busy one from building a batch so large that the transaction holding it
     * becomes the problem.
     */
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
     * <p>This is what makes uneven arrival rates a non-issue. The assignment notes that one
     * instrument may print X times in a second while another prints Y, and for a real feed the
     * ratio is extreme — in the simulated exchange here the busiest instrument outpaces the
     * quietest by roughly a hundred to one. Without coalescing, the cost of keeping the top of
     * book current would scale with the noisiest instrument's tick rate; with it, a flush costs one
     * write per instrument that moved, whether that instrument printed once or five thousand times.
     * The quiet instrument is unaffected either way, which is the property that matters: a busy
     * neighbour must not delay it.
     *
     * <p>It is also a correctness requirement, not only an optimisation. PostgreSQL refuses an
     * {@code ON CONFLICT DO UPDATE} that would touch one row twice in a single command, so an
     * uncoalesced batch containing two quotes for the same ISIN fails the whole write.
     *
     * <p>The history is untouched by this — every quote is still stored. Only the
     * "what is it worth right now" projection is collapsed, and only onto quotes that a later one
     * in the same batch already superseded.
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
     * Removes quotes that share a primary key within one batch.
     *
     * <p>Needed because the PostgreSQL driver rewrites a batched insert into a single multi-row
     * statement, at which point two identical keys in the same batch stop being separate
     * statements that conflict harmlessly and become one statement conflicting with itself.
     * Replay makes duplicates routine rather than exotic, so this runs on every flush.
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

    /** Total time the feed has spent waiting on a full buffer. */
    public Duration timeBlocked() {
        return Duration.ofNanos(blockedNanos.get());
    }

    public long blockedCount() {
        return blockedCount.get();
    }
}
