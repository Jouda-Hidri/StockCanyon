package stockcanyon.consumption;

import java.util.Collections;
import java.util.List;

import stockcanyon.Quote;

/** Reductions applied to a batch before it is written. */
public final class QuoteBatch {

    private QuoteBatch() {
    }

    /**
     * Newest quote in the batch, for the lag it reports.
     *
     * <p>Computed rather than taken as the last element, so an out-of-order batch cannot report the
     * wrong quote. Not what the checkpoint records: that is the contiguous prefix from
     * {@link SequenceTracker#safePosition()}, which trails this whenever a hole is open.
     */
    public static Quote highWaterMark(List<Quote> batch) {
        return Collections.max(batch, Quote.BY_RECENCY);
    }
}
