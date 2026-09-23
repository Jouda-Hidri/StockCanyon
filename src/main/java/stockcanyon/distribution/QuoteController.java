package stockcanyon.distribution;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import stockcanyon.Isin;
import stockcanyon.Quote;
import stockcanyon.consumption.QuoteConsumer;
import stockcanyon.storage.QuoteRepository;

/**
 * Exposes the consumed data to other internal services.
 *
 * <p>Read-only: quotes enter from the exchange and nowhere else, so there is no write endpoint to
 * secure or make idempotent.
 */
@RestController
@RequestMapping("/api/v1/marketdata")
// Scanning is not conditional, so without this the controller loads even when the module is off
// and fails the context looking for beans that were never defined.
@ConditionalOnProperty(prefix = "marketdata", name = "enabled", havingValue = "true")
public class QuoteController {

    private final QuoteRepository quotes;
    private final ObjectProvider<QuoteConsumer> consumer;
    private final Clock clock;

    public QuoteController(
            QuoteRepository quotes, ObjectProvider<QuoteConsumer> consumer, Clock clock) {
        this.quotes = quotes;
        this.consumer = consumer;
        this.clock = clock;
    }

    /**
     * The latest quote for an instrument, by ISIN.
     *
     * <p>400 for a malformed ISIN, 404 for one with no quote: retrying helps only in the second case.
     * 404 rather than an empty 200, because a caller that cannot tell "no price" from "a price of
     * nothing" will eventually treat one as the other.
     */
    @GetMapping("/quotes/{isin}/latest")
    public QuoteResponse latest(@PathVariable String isin) {
        Isin parsed;
        try {
            parsed = Isin.of(isin);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
        Quote quote = quotes.findLatest(parsed).orElseThrow(() -> new ResponseStatusException(
                HttpStatus.NOT_FOUND, "No quote stored for ISIN " + parsed.value()));
        return QuoteResponse.from(quote, Duration.between(quote.eventTime(), clock.instant()));
    }

    /** Consumption state. {@code quotesMissing} is the field that answers the no-gaps requirement. */
    @GetMapping("/status")
    public Map<String, Object> status() {
        QuoteConsumer active = consumer.getIfAvailable();
        if (active == null) {
            return Map.of(
                    "consuming", false,
                    "storedQuotes", quotes.countStoredQuotes(),
                    "instruments", quotes.countInstruments());
        }
        return Map.of(
                "consuming", true,
                "feed", active.status(),
                "storedQuotes", quotes.countStoredQuotes(),
                "instruments", quotes.countInstruments());
    }
}
