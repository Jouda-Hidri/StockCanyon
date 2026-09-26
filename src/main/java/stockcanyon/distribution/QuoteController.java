package stockcanyon.distribution;

import java.time.Clock;
import java.time.Duration;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import stockcanyon.Isin;
import stockcanyon.Quote;

/**
 * Exposes the consumed data to other internal services, from the distribution service's own Redis.
 *
 * <p>Read-only: quotes enter from the exchange and nowhere else, so there is no write endpoint to
 * secure or make idempotent.
 */
@RestController
@RequestMapping("/api/v1/marketdata")
@ConditionalOnProperty(prefix = "marketdata.distribution", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class QuoteController {

    private final LatestQuoteStore store;
    private final Clock clock;

    public QuoteController(LatestQuoteStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    /**
     * The latest quote for an instrument, by ISIN.
     *
     * <p>400 for a malformed ISIN, 404 for one with no quote: retrying helps only in the second case.
     * 404 rather than an empty 200, because a caller that cannot tell "no price" from "a price of
     * nothing" will eventually treat one as the other.
     *
     * <p>{@code ageMillis} now includes the trip through the outbox, Kafka and Redis. It is the
     * number that tells a caller how stale this copy is.
     */
    @GetMapping("/quotes/{isin}/latest")
    public QuoteResponse latest(@PathVariable String isin) {
        Isin parsed;
        try {
            parsed = Isin.of(isin);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
        Quote quote = store.findLatest(parsed).orElseThrow(() -> new ResponseStatusException(
                HttpStatus.NOT_FOUND, "No quote stored for ISIN " + parsed.value()));
        return QuoteResponse.from(quote, Duration.between(quote.eventTime(), clock.instant()));
    }

    /**
     * Redis unreachable or slow: 503 with {@code Retry-After}, not 500. The request was fine and
     * will succeed once the store answers again, which a caller can only know if the status says
     * so. A 500 reads as "this request is broken"; a 503 as "come back shortly".
     */
    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ProblemDetail> storeUnavailable(DataAccessException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE, "The quote store is unavailable; retry shortly.");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(problem);
    }
}
