package stockcanyon;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One top-of-book observation.
 *
 * <p>{@code eventTime} is when the exchange says it happened and is the only field that may order
 * quotes or resume a feed; {@code receivedTime} is when we saw it. The difference is consumption
 * lag. {@code sequence} is the exchange's gap-free counter — what lets a consumer prove it missed
 * nothing.
 *
 * <p>Prices and sizes are {@link BigDecimal}: binary floating point cannot hold 0.10, and sizes
 * are not always whole.
 */
public record Quote(
        Isin isin,
        long sequence,
        BigDecimal bid,
        BigDecimal ask,
        BigDecimal bidSize,
        BigDecimal askSize,
        String currency,
        Instant eventTime,
        Instant receivedTime) {

    public Quote {
        if (isin == null) {
            throw new IllegalArgumentException("Quote requires an ISIN");
        }
        if (eventTime == null) {
            throw new IllegalArgumentException("Quote requires an event time");
        }
        if (currency == null || currency.length() != 3) {
            throw new IllegalArgumentException("Quote requires a 3-letter currency, got: " + currency);
        }
        currency = currency.toUpperCase();
    }

    /** Midpoint, or whichever side is present. A one-sided book is normal, not an error. */
    public BigDecimal mid() {
        if (bid == null) {
            return ask;
        }
        if (ask == null) {
            return bid;
        }
        return bid.add(ask).divide(BigDecimal.valueOf(2), java.math.RoundingMode.HALF_UP);
    }

    /** How far behind the exchange we were when we saw it. */
    public java.time.Duration ingestionLag() {
        return java.time.Duration.between(eventTime, receivedTime);
    }

    /**
     * Whether this quote supersedes {@code other}: event time, with the sequence breaking ties.
     *
     * <p>The same rule the {@code latest_quote} upsert enforces in SQL.
     */
    public boolean isNewerThan(Quote other) {
        if (other == null) {
            return true;
        }
        int byTime = eventTime.compareTo(other.eventTime);
        return byTime != 0 ? byTime > 0 : sequence > other.sequence;
    }
}
