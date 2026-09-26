package stockcanyon.consumption;

import java.time.Duration;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Ingestion health, in {@code /actuator/health}. Deliberately not in the liveness or readiness
 * groups: restarting the leader because the exchange or database is down would not bring either
 * back, and an unready pod is taken out of the API rotation, which the API does not need.
 */
public class IngestionHealthIndicator implements HealthIndicator {

    /** Long enough to ride out a primary failover without paging anyone. */
    static final Duration WRITES_FAILING = Duration.ofSeconds(60);
    /** Beyond several full backoff cycles; normal reconnects are seconds. */
    static final Duration EXCHANGE_UNREACHABLE = Duration.ofSeconds(120);

    private final QuoteConsumer consumer;

    public IngestionHealthIndicator(QuoteConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public Health health() {
        QuoteConsumer.ConsumptionStatus status = consumer.status();
        if (!status.leader()) {
            return Health.up().withDetail("role", "standby").build();
        }
        Health.Builder health = Health.up();
        Duration failing = consumer.writesFailingFor();
        Duration disconnected = consumer.exchangeDisconnectedFor();
        if (failing.compareTo(WRITES_FAILING) > 0) {
            health = Health.down().withDetail("problem", "writes failing for " + failing.toSeconds() + "s");
        } else if (!status.queuePaused() && disconnected.compareTo(EXCHANGE_UNREACHABLE) > 0) {
            health = Health.down().withDetail("problem",
                    "exchange unreachable for " + disconnected.toSeconds() + "s");
        }
        return health
                .withDetail("role", "leader")
                .withDetail("term", status.term())
                .withDetail("connected", status.connected())
                .withDetail("queueDepth", status.queueDepth())
                .withDetail("queuePaused", status.queuePaused())
                .withDetail("sequencesOutstanding", status.sequencesOutstanding())
                .withDetail("sequencesLost", status.sequencesLost())
                .build();
    }
}
