package stockcanyon.consumption;

import java.util.Map;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.transaction.support.TransactionTemplate;

import stockcanyon.storage.QuoteRepository;

/**
 * {@code POST /actuator/outboxreseed}: publishes every instrument's latest quote again.
 *
 * <p>The recovery for losing the downstream copy in a way replay cannot fix — the Debezium
 * replication slot dropped, the topic deleted, Redis rebuilt with the topic gone too. Safe to run
 * at any time and any number of times: the distribution service accepts a quote only if it is
 * newer than the one it holds.
 *
 * <p>An operator action, on the management port of an internal service, not a public API.
 */
@Endpoint(id = "outboxreseed")
public class OutboxReseedEndpoint {

    private final QuoteRepository quotes;
    private final TransactionTemplate transactions;

    public OutboxReseedEndpoint(QuoteRepository quotes, TransactionTemplate transactions) {
        this.quotes = quotes;
        this.transactions = transactions;
    }

    @WriteOperation
    public Map<String, Object> reseed() {
        Integer published = transactions.execute(status -> quotes.reseedOutbox());
        return Map.of("published", published == null ? 0 : published);
    }
}
