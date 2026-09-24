package stockcanyon.consumption;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import stockcanyon.Isin;
import stockcanyon.Quote;

/** Reductions applied to a batch before it is written. */
public final class QuoteBatch {

    private QuoteBatch() {
    }

    /**
     * Reduces a batch to at most one quote per instrument, keeping the newest.
     *
     * <p>The answer to the brief's note that one instrument may print X times a second while
     * another prints Y: a flush costs one latest-quote write per instrument that <em>moved</em>,
     * whether it moved once or 5000 times, so a busy instrument cannot delay a quiet one.
     *
     * <p>Also required, not merely an optimisation: PostgreSQL rejects an
     * {@code ON CONFLICT DO UPDATE} touching one row twice in a command. History keeps every quote.
     */
    public static Collection<Quote> coalesceLatest(List<Quote> batch) {
        return batch.stream()
                .collect(Collectors.toMap(
                        Quote::isin,
                        quote -> quote,
                        BinaryOperator.maxBy(Quote.BY_RECENCY),
                        LinkedHashMap::new))
                .values();
    }

    /**
     * Drops quotes sharing a primary key within one batch.
     *
     * <p>The driver rewrites a batched insert into one multi-row statement, where duplicate keys
     * collide with themselves instead of conflicting harmlessly. Replay makes that routine.
     */
    public static List<Quote> dedupeByKey(List<Quote> batch) {
        record Key(Isin isin, java.time.Instant eventTime, long sequence) {}
        Map<Key, Quote> unique = new LinkedHashMap<>(batch.size());
        for (Quote quote : batch) {
            unique.putIfAbsent(new Key(quote.isin(), quote.eventTime(), quote.sequence()), quote);
        }
        return unique.size() == batch.size() ? batch : new ArrayList<>(unique.values());
    }

    /**
     * Newest quote in the batch — what the checkpoint must name.
     *
     * <p>Computed rather than taken as the last element, so an out-of-order batch cannot set the
     * checkpoint from the wrong quote.
     *
     * <p><b>Assumes the exchange orders the stream by event time.</b> If it does not, a quote
     * stamped .150 could arrive in a later batch than one stamped .200 — the checkpoint would
     * already be .200, and a crash before the late quote arrived would lose it, since resuming
     * from .200 never re-delivers it. Safe here because the exchange stamps in publish order.
     * Against a real feed this needs confirming; if the stream can be unordered, the checkpoint
     * has to be the low-water mark — the highest instant below which nothing is still outstanding.
     */
    public static Quote highWaterMark(List<Quote> batch) {
        return Collections.max(batch, Quote.BY_RECENCY);
    }
}
