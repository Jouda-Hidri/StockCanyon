package stockcanyon.consumption;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * Newest quote in the batch — what the checkpoint must name.
     *
     * <p>Computed rather than taken as the last element, so a batch delivered slightly out of order
     * cannot set the checkpoint from the wrong quote and skip the other on resume.
     */
    public static Quote highWaterMark(List<Quote> batch) {
        Quote highest = batch.getFirst();
        for (Quote quote : batch) {
            if (quote.isNewerThan(highest)) {
                highest = quote;
            }
        }
        return highest;
    }
}
