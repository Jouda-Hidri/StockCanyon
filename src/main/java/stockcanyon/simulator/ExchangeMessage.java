package stockcanyon.simulator;

import java.math.BigDecimal;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The exchange's wire format, kept separate from the domain {@code Quote} so a protocol change
 * does not silently become a model change.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ExchangeMessage(
        String isin,
        long sequence,
        BigDecimal bid,
        BigDecimal ask,
        BigDecimal bidSize,
        BigDecimal askSize,
        String currency,
        Instant timestamp) {

    /** Discriminator, so data and control frames are distinguishable on one socket. */
    @JsonProperty("type")
    public String type() {
        return "quote";
    }
}
