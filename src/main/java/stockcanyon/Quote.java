package stockcanyon;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One top-of-book observation for an instrument.
 *
 * <p>Three timestamps look redundant and are not. {@code eventTime} is when the exchange says the
 * quote happened and is the only one that may be used to order quotes or to resume a feed.
 * {@code receivedTime} is when this service saw it, and the difference between the two is the
 * consumer lag that tells an operator whether the pipeline is keeping up. Conflating them hides
 * exactly the condition you most want to see.
 *
 * <p>{@code sequence} is the exchange's own gap-free counter. It is what lets a consumer prove it
 * missed nothing rather than assume it: after a reconnect, a hole in the sequence is a hole in the
 * data, and no amount of successful reconnecting disproves it.
 *
 * <p>Prices are {@link BigDecimal} throughout. The reasoning is the same as {@code Money}'s in the
 * payments package — binary floating point cannot hold 0.10 — but market data cannot reuse
 * {@code Money}: a price is a rate rather than an amount, quoted to four or more decimal places
 * where the currency has two, so forcing it into minor units would round the tick away.
 *
 * <p>Sizes are {@link BigDecimal} rather than a count for the same reason. Equities trade in whole
 * shares, but the service also carries instruments that do not: a book quoting 1.40159 of
 * something is ordinary outside equities, and a {@code long} would silently floor it to 1.
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

    /**
     * The midpoint, or whichever side is present when the book is one-sided.
     *
     * <p>Returning null for a one-sided book would push the special case onto every caller; a
     * one-sided book is a normal state at the open and in thin names, not an error.
     */
    public BigDecimal mid() {
        if (bid == null) {
            return ask;
        }
        if (ask == null) {
            return bid;
        }
        return bid.add(ask).divide(BigDecimal.valueOf(2), java.math.RoundingMode.HALF_UP);
    }

    /** How far behind the exchange this service was when it saw the quote. */
    public java.time.Duration ingestionLag() {
        return java.time.Duration.between(eventTime, receivedTime);
    }

    /**
     * Whether this quote supersedes {@code other}.
     *
     * <p>Event time decides, with the sequence breaking ties, because several quotes can share one
     * instant. Used to keep a replayed or out-of-order message from overwriting a newer one — the
     * same rule the {@code latest_quote} upsert enforces in SQL.
     */
    public boolean isNewerThan(Quote other) {
        if (other == null) {
            return true;
        }
        int byTime = eventTime.compareTo(other.eventTime);
        return byTime != 0 ? byTime > 0 : sequence > other.sequence;
    }
}
