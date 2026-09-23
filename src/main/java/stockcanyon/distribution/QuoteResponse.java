package stockcanyon.distribution;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonInclude;

import stockcanyon.Quote;

/**
 * API representation of a quote, kept separate from the stored model.
 *
 * <p>{@code ageMillis} is computed here so every caller does not subtract timestamps itself and
 * disagree with the others about what counts as stale.
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
