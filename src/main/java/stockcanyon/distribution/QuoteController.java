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
 * <p>Read-only, and only two endpoints. Quotes enter this service from the exchange and from
 * nowhere else, so there is no write endpoint to secure, rate-limit or make idempotent.
 */
@RestController
@RequestMapping("/api/v1/marketdata")
// Component scanning is not conditional, so without this the controller would be created even when
// the module is switched off, then fail the context looking for beans that were never defined.
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
     * <p>Two distinct failures, two distinct codes. A malformed ISIN is the caller's bug and
     * retrying will not help; an ISIN with no quote yet is a state that may resolve on its own.
     * Collapsing both into one status leaves a client unable to tell "stop" from "try again
     * shortly".
     *
     * <p>404 rather than an empty 200, because a caller that cannot distinguish "no price" from "a
     * price of nothing" will eventually treat one as the other — and in a pricing path that is the
     * expensive kind of mistake.
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

    /**
     * Consumption state.
     *
     * <p>The field worth watching is {@code quotesMissing}: sequence numbers the exchange issued
     * and this service never received. Anything but zero means the stored history has holes, and it
     * is the only number here that says so — a healthy connection and a climbing quote count are
     * both perfectly consistent with having missed a thousand messages.
     */
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
