package stockcanyon.distribution;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonInclude;

import stockcanyon.Quote;

/**
 * What the API returns for a quote.
 *
 * <p>A response type distinct from the stored {@code Quote}, so the wire contract internal services
 * depend on does not change every time the internal model does.
 *
 * <p>{@code ageMillis} is included rather than left for the caller to compute. Every consumer of a
 * price needs to know how old it is, and making each of them subtract two timestamps invites each
 * to get the clock comparison subtly wrong — and to disagree with the others about whether a quote
 * is stale.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record QuoteResponse(
        String isin,
        String currency,
        BigDecimal bid,
        BigDecimal ask,
        BigDecimal mid,
        BigDecimal bidSize,
        BigDecimal askSize,
        long sequence,
        Instant eventTime,
        Instant receivedTime,
        long ageMillis) {

    public static QuoteResponse from(Quote quote, Duration age) {
        return new QuoteResponse(
                quote.isin().value(),
                quote.currency(),
                quote.bid(),
                quote.ask(),
                quote.mid(),
                quote.bidSize(),
                quote.askSize(),
                quote.sequence(),
                quote.eventTime(),
                quote.receivedTime(),
                Math.max(0, age.toMillis()));
    }
}
