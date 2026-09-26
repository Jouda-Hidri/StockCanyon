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
        if (currency == null || !CURRENCY.matcher(currency).matches()) {
            throw new IllegalArgumentException("Quote requires a 3-letter currency, got: " + currency);
        }
        currency = currency.toUpperCase(java.util.Locale.ROOT);
        requireWithin("bid", bid, MAX_PRICE);
        requireWithin("ask", ask, MAX_PRICE);
        requireWithin("bidSize", bidSize, MAX_SIZE);
        requireWithin("askSize", askSize, MAX_SIZE);
    }

    private static final java.util.regex.Pattern CURRENCY = java.util.regex.Pattern.compile("[A-Za-z]{3}");

    /**
     * The column bounds, NUMERIC(20, 8) and NUMERIC(24, 8). Checked here, where one bad frame can be
     * rejected and counted, because a value the database refuses fails the whole batch — and every
     * retry, and every replay of it — which would stop ingestion outright.
     */
    private static final BigDecimal MAX_PRICE = BigDecimal.TEN.pow(12);
    private static final BigDecimal MAX_SIZE = BigDecimal.TEN.pow(16);

    private static void requireWithin(String field, BigDecimal value, BigDecimal bound) {
        if (value != null && value.abs().compareTo(bound) >= 0) {
            throw new IllegalArgumentException(field + " out of range: " + value);
        }
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
     * Total order over quotes: event time, sequence breaking ties. The same rule the distribution
     * service's newer-only write enforces in Redis.
     */
    public static final java.util.Comparator<Quote> BY_RECENCY =
            java.util.Comparator.comparing(Quote::eventTime).thenComparingLong(Quote::sequence);
}
