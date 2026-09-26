package stockcanyon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import stockcanyon.consumption.QuoteConsumer;
import stockcanyon.simulator.SimulatedExchangeHandler;

/**
 * The two failures the socket cannot see for itself: a hole in the sequence on a healthy
 * connection, and a database that refuses writes. Both must end with an unbroken stored run.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class MarketDataFaultTest {

    private static final int PORT = freePort();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("server.port", () -> PORT);
        registry.add("marketdata.consumption.enabled", () -> true);
        registry.add("marketdata.distribution.enabled", () -> false);
        // Ingestion alone: no Debezium, so no replication slot to wait for.
        registry.add("marketdata.consumption.require-cdc-slot", () -> false);
        registry.add("marketdata.exchange-url", () -> "ws://localhost:" + PORT + "/exchange/quotes");
        registry.add("marketdata.simulator.enabled", () -> true);
        registry.add("marketdata.simulator.quotes-per-second", () -> 400);
        registry.add("marketdata.database.url", () -> SharedPostgres.jdbcUrlFor("fault_test"));
        registry.add("marketdata.database.username", SharedPostgres.INSTANCE::getUsername);
        registry.add("marketdata.database.password", SharedPostgres.INSTANCE::getPassword);

        registry.add("marketdata.consumption.flush-interval", () -> "50ms");
        registry.add("marketdata.consumption.reconnect-initial-delay", () -> "50ms");
        registry.add("marketdata.consumption.reconnect-max-delay", () -> "500ms");
        registry.add("marketdata.consumption.write-retry-initial-delay", () -> "50ms");
        registry.add("marketdata.consumption.write-retry-max-delay", () -> "500ms");
        registry.add("marketdata.consumption.gap-backfill-after", () -> "500ms");
        // Small, so a few seconds of refused writes at 400/s is enough to reach the high mark.
        registry.add("marketdata.consumption.queue-capacity", () -> 1_500);
        registry.add("marketdata.consumption.queue-high-watermark", () -> 1_000);
        registry.add("marketdata.consumption.queue-low-watermark", () -> 200);
        registry.add("marketdata.leadership.lock-ttl", () -> "3s");
    }

    @Autowired
    QuoteConsumer consumer;

    @Autowired
    SimulatedExchangeHandler exchange;

    @Autowired
    NamedParameterJdbcTemplate marketDataJdbcTemplate;

    @Autowired
    MeterRegistry meters;

    @Test
    @DisplayName("messages lost on a healthy socket are detected and replayed, not written off")
    void gapIsBackfilled() {
        awaitConsumed(500);
        awaitSteadyFeed();
        long gapsBefore = consumer.status().gapsDetected();
        double backfillsBefore = count("marketdata.gap.backfills");

        exchange.skipNext(5);

        await("the hole to be detected")
                .atMost(Duration.ofSeconds(20))
                .until(() -> consumer.status().gapsDetected() > gapsBefore);
        await("a replay to fill it")
                .atMost(Duration.ofSeconds(20))
                .until(() -> consumer.status().sequencesOutstanding() == 0
                        && count("marketdata.gap.backfills") > backfillsBefore);
        long consumedAfterFill = consumer.status().quotesConsumed();
        awaitConsumed(consumedAfterFill + 500);

        assertThat(consumer.status().sequencesLost()).isZero();
        assertThat(consumer.status().quotesMissing()).isZero();
        assertThat(storedSequencesAreContiguous()).as(sequenceSpan()).isTrue();
    }

    /**
     * The failure that used to lose data: a transaction that fails fast while the socket stays up.
     * Now the batch is retried, the queue absorbs arrivals until the high watermark closes the
     * socket, and consumption resumes from the checkpoint once writes succeed again.
     */
    @Test
    @DisplayName("refused writes are retried, the queue closes the socket, and nothing is lost")
    void databaseFailureIsRetriedWithBackpressure() throws Exception {
        awaitConsumed(500);
        double retriesBefore = count("marketdata.db.write.retries");
        double tripsBefore = count("marketdata.queue.high.watermark.trips");

        // NOT VALID skips checking existing rows but is enforced on every new one: each INSERT fails.
        marketDataJdbcTemplate.getJdbcTemplate().execute(
                "ALTER TABLE quote_history ADD CONSTRAINT refuse_writes CHECK (false) NOT VALID");
        try {
            long checkpointDuringFailure = consumer.status().checkpoint().sequence();
            await("the queue to fill and close the socket")
                    .atMost(Duration.ofSeconds(30))
                    .until(() -> count("marketdata.queue.high.watermark.trips") > tripsBefore);
            assertThat(consumer.status().queuePaused()).isTrue();
            assertThat(consumer.status().connected())
                    .as("paused means disconnected: the exchange holds the backlog, not our heap")
                    .isFalse();
            assertThat(count("marketdata.db.write.retries")).isGreaterThan(retriesBefore);
            assertThat(consumer.status().checkpoint().sequence())
                    .as("no commit, so the checkpoint cannot have moved")
                    .isEqualTo(checkpointDuringFailure);
        } finally {
            marketDataJdbcTemplate.getJdbcTemplate().execute(
                    "ALTER TABLE quote_history DROP CONSTRAINT refuse_writes");
        }

        await("the backlog to drain and the socket to reopen")
                .atMost(Duration.ofSeconds(30))
                .until(() -> !consumer.status().queuePaused() && consumer.status().connected());
        long consumedAfterRecovery = consumer.status().quotesConsumed();
        awaitConsumed(consumedAfterRecovery + 1_000);

        assertThat(consumer.status().quotesMissing()).isZero();
        assertThat(storedSequencesAreContiguous()).as(sequenceSpan()).isTrue();
    }

    /**
     * Each stop lands while the previous start is still claiming its term or loading the
     * checkpoint. The last run must end up owning the exchange client and ingesting; before the fix
     * a stopped run could start the client afterwards, and the next run's start then did nothing.
     */
    @Test
    @DisplayName("leadership lost during start-up and regained: the last run still ingests")
    void rapidLeadershipChangesStillIngest() {
        awaitConsumed(100);
        for (int i = 0; i < 5; i++) {
            consumer.stopConsuming(false);
            consumer.startConsuming(() -> { });
        }
        long before = consumer.status().quotesConsumed();
        await("the last run to own the socket and ingest")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> consumer.status().connected() && consumer.status().quotesConsumed() > before + 200);
    }

    @Test
    @DisplayName("this instance leads, and holds the current term")
    void leadsWithTheCurrentTerm() {
        awaitConsumed(100);
        Long term = marketDataJdbcTemplate.getJdbcTemplate()
                .queryForObject("SELECT term FROM ingest_leader", Long.class);
        assertThat(consumer.status().leader()).isTrue();
        assertThat(consumer.status().term()).isEqualTo(term);
    }

    /**
     * Any reconnect replays from the checkpoint and would fill the hole on its own, before the
     * forced backfill under test gets the chance. So start from a feed that is connected, not
     * paused, and not still catching up on another test's outage.
     */
    private void awaitSteadyFeed() {
        await("a steady feed")
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    QuoteConsumer.ConsumptionStatus s = consumer.status();
                    return s.connected() && !s.queuePaused() && s.queueDepth() < 100
                            && s.sequencesOutstanding() == 0
                            && s.lagMillis() < 1_000;
                });
    }

    private void awaitConsumed(long target) {
        await("consumption to reach " + target)
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> consumer.status().quotesConsumed() >= target);
    }

    private double count(String counter) {
        return meters.find(counter).counters().stream().mapToDouble(c -> c.count()).sum();
    }

    private boolean storedSequencesAreContiguous() {
        Boolean contiguous = marketDataJdbcTemplate.getJdbcTemplate().queryForObject(
                "SELECT count(DISTINCT sequence) = (max(sequence) - min(sequence) + 1) "
                        + "AND count(*) = count(DISTINCT sequence) FROM quote_history",
                Boolean.class);
        return Boolean.TRUE.equals(contiguous);
    }

    private String sequenceSpan() {
        return marketDataJdbcTemplate.getJdbcTemplate().queryForObject(
                "SELECT 'min=' || min(sequence) || ' max=' || max(sequence) "
                        + "|| ' distinct=' || count(DISTINCT sequence) || ' rows=' || count(*) FROM quote_history",
                String.class);
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not reserve a port for the test exchange", e);
        }
    }
}
