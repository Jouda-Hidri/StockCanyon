package stockcanyon.distribution;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import stockcanyon.Isin;
import stockcanyon.Quote;

/**
 * An instrument's latest quote moved: the outbox payload, as it arrives from Kafka and as it is
 * kept in Redis.
 *
 * <p>This is the contract between the two services. The consumption service writes it as JSON in
 * {@code QuoteRepository}'s outbox insert; nothing else is shared. Unknown fields are ignored, so
 * the producer can add one without breaking a consumer that has not been redeployed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record LatestQuoteEvent(
        String isin,
        String currency,
        BigDecimal bid,
        BigDecimal ask,
        BigDecimal bidSize,
        BigDecimal askSize,
        long sequence,
        Instant eventTime,
        Instant receivedTime) {

    public Quote toQuote() {
        return new Quote(Isin.of(isin), sequence, bid, ask, bidSize, askSize, currency, eventTime, receivedTime);
    }

    /**
     * The event time as whole microseconds since the epoch: the order Redis compares by. A number
     * rather than the ISO string, because Lua compares strings lexically and numbers exactly, and
     * microseconds since 1970 stay well inside the 2^53 a Lua number holds exactly.
     */
    long eventTimeMicros() {
        return ChronoUnit.MICROS.between(Instant.EPOCH, eventTime);
    }
}
