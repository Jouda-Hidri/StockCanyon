package stockcanyon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;

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
 * Proves consumption survives a network failure without losing a message.
 *
 * <p>The assertion is not "it reconnected" — that is consistent with having missed ten thousand
 * quotes. It is that the stored sequences form an unbroken run across the disconnect.
 *
 * <p>Against a real PostgreSQL, because what is under test is largely the database's behaviour:
 * the guarded upsert and the transaction that makes quotes and checkpoint durable together.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class MarketDataRecoveryTest {

    /**
     * Chosen before the context starts, because the consumer needs the exchange's URL at the time
     * its beans are built and a random servlet port is not known until after. The exchange and the
     * consumer run in one process here — the simulator is just another component — so a single
     * port serves both.
     */
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

        registry.add("marketdata.database.url", () -> SharedPostgres.jdbcUrlFor("recovery_test"));
        registry.add("marketdata.database.username", SharedPostgres.INSTANCE::getUsername);
        registry.add("marketdata.database.password", SharedPostgres.INSTANCE::getPassword);

        // Flush often, so the test does not spend most of its time waiting for a batch to fill.
        registry.add("marketdata.consumption.flush-interval", () -> "50ms");
        // Reconnect promptly; the default backoff starts at half a second.
        registry.add("marketdata.consumption.reconnect-initial-delay", () -> "50ms");
    }

    @Autowired
    QuoteConsumer consumer;

    @Autowired
    SimulatedExchangeHandler exchange;

    @Autowired
    NamedParameterJdbcTemplate marketDataJdbcTemplate;

    @Test
    @DisplayName("a mid-stream disconnect loses no messages")
    void recoversWithoutGaps() {
        await("the feed to start delivering")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> consumer.status().quotesConsumed() > 500);

        long consumedBefore = consumer.status().quotesConsumed();
        long storedBefore = countStored();

        int dropped = exchange.disconnectAll();
        assertThat(dropped)
                .as("the test must actually sever the connection it claims to test")
                .isGreaterThan(0);

        // Enough new quotes that the stream is unambiguously live again rather than merely
        // finishing what was already pending when the socket died.
        await("the feed to recover and resume")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> consumer.status().quotesConsumed() > consumedBefore + 1_000);


        // No wait for the batch to drain: contiguity is an invariant, not a quiescent state.
        // Committed rows are always a contiguous prefix, because batches commit in sequence order
        // and a failed one leaves the checkpoint behind it. Waiting on pending == 0 would also be
        // racy — quotes arrive continuously, so it is only briefly true between flushes.
        QuoteConsumer.ConsumptionStatus status = consumer.status();

        assertThat(status.quotesMissing())
                .as("sequence numbers the exchange issued and this service never received")
                .isZero();

        assertThat(storedSequencesAreContiguous())
                .as("the stored sequences must form an unbroken run across the disconnect: %s",
                        sequenceSpan())
                .isTrue();

        assertThat(countStored())
                .as("consumption must have continued past the disconnect")
                .isGreaterThan(storedBefore);

        // Recovery replays from a timestamp, and a timestamp does not address a single message, so
        // the resumed stream necessarily overlaps what was already stored. Seeing the duplicates
        // discarded is what shows the recovery went through the replay path rather than quietly
        // resuming at the live edge and leaving a hole behind it.
        assertThat(status.duplicatesDiscarded())
                .as("replay should have re-delivered messages already stored")
                .isPositive();
    }

    /**
     * The uneven arrival rate the requirements call out, measured rather than assumed.
     *
     * <p>Worth asserting because the coalescing write path is justified by this skew existing; if
     * the simulated feed were actually uniform, the recovery test above would be passing under
     * conditions the production path never sees.
     */
    @Test
    @DisplayName("arrival rates differ sharply between instruments")
    void arrivalRatesAreUneven() {
        await("a representative sample")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> consumer.status().quotesConsumed() > 3_000);

        List<Long> counts = marketDataJdbcTemplate.getJdbcTemplate().queryForList(
                "SELECT count(*) AS c FROM quote_history GROUP BY isin ORDER BY c DESC", Long.class);

        assertThat(counts).hasSizeGreaterThan(1);
        assertThat(counts.getFirst())
                .as("the busiest instrument should far outpace the quietest: %s", counts)
                .isGreaterThan(counts.getLast() * 5);
    }

    @Autowired
    org.springframework.boot.test.web.client.TestRestTemplate http;

    @Test
    @DisplayName("the consumption service reports its state, including whether anything was missed")
    @SuppressWarnings("unchecked")
    void statusReportsConsumption() {
        await("the feed to deliver quotes")
                .atMost(Duration.ofSeconds(30))
                .until(() -> consumer.status().quotesConsumed() > 500);

        Map<String, Object> body = http.getForObject(
                "http://localhost:" + PORT + "/api/v1/marketdata/status", Map.class);

        Map<String, Object> feed = (Map<String, Object>) body.get("feed");
        assertThat(feed.get("leader")).isEqualTo(true);
        assertThat(feed.get("connected")).isEqualTo(true);
        assertThat(((Number) feed.get("quotesMissing")).longValue()).isZero();
    }

    private long countStored() {
        return queryLong("SELECT count(*) FROM quote_history");
    }

    /**
     * A run of integers from min to max is unbroken exactly when it holds max - min + 1 distinct
     * values. Cheaper than listing them, and it is the same question.
     */
    private boolean storedSequencesAreContiguous() {
        Boolean contiguous = marketDataJdbcTemplate.getJdbcTemplate().queryForObject(
                "SELECT count(DISTINCT sequence) = (max(sequence) - min(sequence) + 1) FROM quote_history",
                Boolean.class);
        return Boolean.TRUE.equals(contiguous);
    }

    private String sequenceSpan() {
        return marketDataJdbcTemplate.getJdbcTemplate().queryForObject(
                "SELECT 'min=' || min(sequence) || ' max=' || max(sequence) "
                        + "|| ' distinct=' || count(DISTINCT sequence) FROM quote_history",
                String.class);
    }

    private long queryLong(String sql) {
        Long value = marketDataJdbcTemplate.getJdbcTemplate().queryForObject(sql, Long.class);
        return value == null ? 0 : value;
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not reserve a port for the test exchange", e);
        }
    }
}
