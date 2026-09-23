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
import org.springframework.jdbc.core.JdbcTemplate;
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
        registry.add("marketdata.enabled", () -> true);
        registry.add("marketdata.exchange-url", () -> "ws://localhost:" + PORT + "/exchange/quotes");

        registry.add("marketdata.simulator.enabled", () -> true);
        registry.add("marketdata.simulator.quotes-per-second", () -> 400);

        registry.add("marketdata.datasource.url", () -> SharedPostgres.jdbcUrlFor("recovery_test"));
        registry.add("marketdata.datasource.username", SharedPostgres.INSTANCE::getUsername);
        registry.add("marketdata.datasource.password", SharedPostgres.INSTANCE::getPassword);

        // Flush often, so the test does not spend most of its time waiting for a batch to fill.
        registry.add("marketdata.consumption.flush-interval", () -> "50ms");
        // Reconnect promptly; the production default deliberately jitters up to half a second.
        registry.add("marketdata.consumption.initial-backoff", () -> "50ms");
        registry.add("marketdata.consumption.max-backoff", () -> "500ms");
    }

    @Autowired
    QuoteConsumer consumer;

    @Autowired
    SimulatedExchangeHandler exchange;

    @Autowired
    JdbcTemplate marketDataJdbcTemplate;

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
        // finishing what was already buffered when the socket died.
        await("the feed to recover and resume")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> consumer.status().quotesConsumed() > consumedBefore + 1_000);

        await("the buffer to drain")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> consumer.status().bufferDepth() == 0);

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

    @Test
    @DisplayName("the latest quote never moves backwards, even while replaying")
    void latestQuoteIsMonotonic() {
        await("quotes for several instruments")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> countInstruments() >= 3);

        Map<String, Long> before = latestSequencesByIsin();

        exchange.disconnectAll();

        await("the feed to recover")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> consumer.status().quotesConsumed() > 2_000);
        await("the buffer to drain")
                .atMost(Duration.ofSeconds(15))
                .until(() -> consumer.status().bufferDepth() == 0);

        Map<String, Long> after = latestSequencesByIsin();

        // Every instrument that had a top of book must still have one, no older than before. The
        // guard in the upsert is what enforces this; without it the replayed quotes would arrive
        // after the newer ones and the last write would win.
        before.forEach((isin, sequenceBefore) ->
                assertThat(after.get(isin))
                        .as("latest_quote for %s must not regress", isin)
                        .isNotNull()
                        .isGreaterThanOrEqualTo(sequenceBefore));
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

        List<Long> counts = marketDataJdbcTemplate.queryForList(
                "SELECT count(*) AS c FROM quote GROUP BY isin ORDER BY c DESC", Long.class);

        assertThat(counts).hasSizeGreaterThan(1);
        assertThat(counts.getFirst())
                .as("the busiest instrument should far outpace the quietest: %s", counts)
                .isGreaterThan(counts.getLast() * 5);
    }

    private long countStored() {
        return queryLong("SELECT count(*) FROM quote");
    }

    private long countInstruments() {
        return queryLong("SELECT count(*) FROM latest_quote");
    }

    /**
     * A run of integers from min to max is unbroken exactly when it holds max - min + 1 distinct
     * values. Cheaper than listing them, and it is the same question.
     */
    private boolean storedSequencesAreContiguous() {
        Boolean contiguous = marketDataJdbcTemplate.queryForObject(
                "SELECT count(DISTINCT sequence) = (max(sequence) - min(sequence) + 1) FROM quote",
                Boolean.class);
        return Boolean.TRUE.equals(contiguous);
    }

    private String sequenceSpan() {
        return marketDataJdbcTemplate.queryForObject(
                "SELECT 'min=' || min(sequence) || ' max=' || max(sequence) "
                        + "|| ' distinct=' || count(DISTINCT sequence) FROM quote",
                String.class);
    }

    private Map<String, Long> latestSequencesByIsin() {
        return marketDataJdbcTemplate.query(
                "SELECT isin, sequence FROM latest_quote",
                rs -> {
                    Map<String, Long> out = new java.util.HashMap<>();
                    while (rs.next()) {
                        out.put(rs.getString("isin"), rs.getLong("sequence"));
                    }
                    return out;
                });
    }

    private long queryLong(String sql) {
        Long value = marketDataJdbcTemplate.queryForObject(sql, Long.class);
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
