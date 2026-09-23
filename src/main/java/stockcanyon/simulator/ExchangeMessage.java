package stockcanyon.simulator;

import java.math.BigDecimal;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The simulated exchange's wire format.
 *
 * <p>Separate from the domain {@code Quote} on purpose. This is a third party's message shape, and
 * letting the two be the same type would mean every change to the exchange's protocol silently
 * became a change to the service's internal model. It also has no notion of {@code receivedTime}:
 * the exchange cannot know when we saw it.
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
