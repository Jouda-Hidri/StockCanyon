package stockcanyon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.integration.leader.Context;
import org.springframework.integration.leader.DefaultCandidate;
import org.springframework.integration.support.leader.LockRegistryLeaderInitiator;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import stockcanyon.storage.CheckpointRepository;
import stockcanyon.storage.LeadershipRepository;
import stockcanyon.storage.QuoteRepository;

/**
 * The storage layer against a real PostgreSQL: the COPY write path, the fencing term, and the
 * election that decides who writes.
 */
class StorageTest {

    private static final Isin AAPL = Isin.of("US0378331005");
    private static final Isin MSFT = Isin.of("US5949181045");
    private static final Instant T0 = Instant.parse("2026-09-26T10:00:00.123456789Z");

    private static HikariDataSource dataSource;
    private static NamedParameterJdbcTemplate jdbc;
    private static DataSourceTransactionManager transactionManager;
    private static TransactionTemplate transactions;
    private static QuoteRepository quotes;
    private static CheckpointRepository checkpoints;
    private static LeadershipRepository leadership;

    @BeforeAll
    static void connect() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(SharedPostgres.jdbcUrlFor("storage_test"));
        dataSource.setUsername(SharedPostgres.INSTANCE.getUsername());
        dataSource.setPassword(SharedPostgres.INSTANCE.getPassword());
        Flyway.configure().dataSource(dataSource)
                .locations("classpath:db/migration/marketdata")
                .table("flyway_schema_history_marketdata")
                .load().migrate();
        jdbc = new NamedParameterJdbcTemplate(dataSource);
        transactionManager = new DataSourceTransactionManager(dataSource);
        transactions = new TransactionTemplate(transactionManager);
        quotes = new QuoteRepository(jdbc, dataSource);
        checkpoints = new CheckpointRepository(jdbc);
        leadership = new LeadershipRepository(jdbc);
    }

    @AfterAll
    static void disconnect() {
        dataSource.close();
    }

    @BeforeEach
    void clean() {
        jdbc.getJdbcTemplate().execute(
                "TRUNCATE quote_history, ingest_checkpoint, ingest_leader, int_lock, outbox");
    }

    // ------------------------------------------------------------------ COPY write path

    @Test
    @DisplayName("COPY writes every quote, including one-sided books, with nanoseconds rounded")
    void copyWritesTheBatch() {
        Quote oneSided = new Quote(MSFT, 3, null, new BigDecimal("200.5"), null, BigDecimal.TEN,
                "USD", T0.plusMillis(2), T0.plusMillis(3));
        int inserted = write(List.of(quote(AAPL, 1, T0, "100"), quote(AAPL, 2, T0.plusMillis(1), "101"), oneSided));

        assertThat(inserted).isEqualTo(3);
        Map<String, Object> stored = jdbc.getJdbcTemplate().queryForMap(
                "SELECT bid, ask, event_time FROM quote_history WHERE isin = ?", MSFT.value());
        assertThat(stored.get("bid")).isNull();
        assertThat((BigDecimal) stored.get("ask")).isEqualByComparingTo("200.5");
        assertThat(((java.sql.Timestamp) stored.get("event_time")).toInstant())
                .isEqualTo(Instant.parse("2026-09-26T10:00:00.125457Z"));
    }

    @Test
    @DisplayName("a replayed quote is discarded by the primary key, within a batch and across batches")
    void replayIsIdempotent() {
        Quote q1 = quote(AAPL, 1, T0, "100");
        Quote q2 = quote(AAPL, 2, T0.plusMillis(1), "101");

        assertThat(write(List.of(q1, q2, q1))).as("in-batch duplicate").isEqualTo(2);
        assertThat(write(List.of(q1, q2))).as("replayed batch").isZero();
        assertThat(storedQuotes()).isEqualTo(2);
    }

    @Test
    @DisplayName("the staging table does not leak rows from one batch into the next")
    void stagingIsEmptiedAtCommit() {
        write(List.of(quote(AAPL, 1, T0, "100")));
        write(List.of(quote(MSFT, 2, T0.plusMillis(1), "200")));

        assertThat(storedQuotes()).isEqualTo(2);
    }

    @Test
    @DisplayName("a rolled-back batch leaves neither quotes nor checkpoint behind")
    void rollbackIsAtomic() {
        assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
            quotes.writeBatch(List.of(quote(AAPL, 1, T0, "100")));
            checkpoints.save(T0, 1, T0);
            throw new IllegalStateException("simulated failure after the writes");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(storedQuotes()).isZero();
        assertThat(checkpoints.load().isPresent()).isFalse();
        // And the connection is still usable for the next batch.
        assertThat(write(List.of(quote(AAPL, 1, T0, "100")))).isEqualTo(1);
    }

    // ------------------------------------------------------------------ outbox

    /**
     * Reads what the outbox put in the WAL, through a logical decoding slot — the same stream
     * Debezium reads. The table itself is always empty after commit, so there is nothing else to
     * look at.
     */
    private static final class Wal implements AutoCloseable {

        private static final Pattern OUTBOX_INSERT = Pattern.compile(
                "^table public\\.outbox: INSERT: .*aggregateid\\[text\\]:'([^']*)'.*payload\\[jsonb\\]:'(.*)'$");

        Wal() {
            jdbc.getJdbcTemplate().queryForList(
                    "SELECT pg_create_logical_replication_slot('outbox_test', 'test_decoding')");
        }

        /** Outbox events committed since the last call: ISIN to payload, in commit order. */
        List<Map.Entry<String, String>> published() {
            List<String> changes = jdbc.getJdbcTemplate().queryForList(
                    "SELECT data FROM pg_logical_slot_get_changes('outbox_test', NULL, NULL)", String.class);
            List<Map.Entry<String, String>> events = new ArrayList<>();
            for (String change : changes) {
                Matcher m = OUTBOX_INSERT.matcher(change);
                if (m.matches()) {
                    events.add(Map.entry(m.group(1), m.group(2)));
                }
            }
            return events;
        }

        @Override
        public void close() {
            jdbc.getJdbcTemplate().queryForList("SELECT pg_drop_replication_slot('outbox_test')");
        }
    }

    @Test
    @DisplayName("each instrument in a batch is published once, with its newest quote there")
    void publishesNewestPerInstrument() {
        try (Wal wal = new Wal()) {
            QuoteRepository.WriteResult result = writeAndPublish(List.of(
                    quote(AAPL, 1, T0, "100"),
                    quote(AAPL, 4, T0.plusMillis(30), "104"),
                    quote(AAPL, 2, T0.plusMillis(10), "102"),
                    quote(MSFT, 3, T0.plusMillis(20), "200")));

            assertThat(result.published()).isEqualTo(2);
            var events = wal.published();
            assertThat(events).extracting(Map.Entry::getKey).containsExactlyInAnyOrder(AAPL.value(), MSFT.value());
            String aapl = events.stream().filter(e -> e.getKey().equals(AAPL.value())).findFirst().orElseThrow().getValue();
            assertThat(aapl).contains("\"sequence\": 4").contains("\"eventTime\": \"2026-09-26T10:00:00.153457Z\"");
            assertThat(jdbc.getJdbcTemplate().queryForObject("SELECT count(*) FROM outbox", Long.class))
                    .as("rows only need to exist in the WAL").isZero();
        }
    }

    /**
     * No latest state here, so nothing to filter against: a replayed batch publishes its old prices
     * again. That is deliberate — the distribution service owns "latest" and discards them.
     */
    @Test
    @DisplayName("a replayed batch publishes again, and leaves deciding what is old to the consumer")
    void replayIsPublishedAgain() {
        write(List.of(quote(AAPL, 4, T0.plusMillis(30), "104")));
        try (Wal wal = new Wal()) {
            QuoteRepository.WriteResult replay = writeAndPublish(List.of(
                    quote(AAPL, 2, T0.plusMillis(10), "102"), quote(AAPL, 4, T0.plusMillis(30), "104")));

            assertThat(replay.inserted()).as("only 2 is new to the history").isEqualTo(1);
            assertThat(replay.published()).isEqualTo(1);
            assertThat(wal.published()).singleElement()
                    .satisfies(e -> assertThat(e.getValue()).contains("\"sequence\": 4"));
        }
    }

    @Test
    @DisplayName("a fenced transaction publishes nothing: the event rolls back with the data")
    void fencedWritePublishesNothing() {
        long stale = leadership.claim("paused-leader", T0);
        leadership.claim("new-leader", T0);
        try (Wal wal = new Wal()) {
            assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
                leadership.assertCurrent(stale);
                quotes.writeBatch(List.of(quote(AAPL, 1, T0, "100")));
            })).isInstanceOf(LeadershipRepository.FencedException.class);

            // A second, rolled-back write that got as far as the outbox insert.
            assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
                quotes.writeBatch(List.of(quote(MSFT, 2, T0, "200")));
                throw new IllegalStateException("fails after the outbox insert");
            })).isInstanceOf(IllegalStateException.class);

            assertThat(wal.published()).isEmpty();
        }
    }

    @Test
    @DisplayName("reseed publishes every instrument's current latest quote")
    void reseedPublishesEverything() {
        write(List.of(quote(AAPL, 1, T0, "100"), quote(MSFT, 2, T0.plusMillis(1), "200")));
        try (Wal wal = new Wal()) {
            Integer published = transactions.execute(status -> quotes.reseedOutbox());

            assertThat(published).isEqualTo(2);
            assertThat(wal.published()).extracting(Map.Entry::getKey)
                    .containsExactlyInAnyOrder(AAPL.value(), MSFT.value());
        }
    }

    // ------------------------------------------------------------------ fencing

    @Test
    @DisplayName("each claim increments the term")
    void claimsIncrement() {
        assertThat(leadership.claim("a", T0)).isEqualTo(1);
        assertThat(leadership.claim("b", T0)).isEqualTo(2);
    }

    /** The case a lock alone cannot handle: a leader that paused past its TTL and woke up mid-write. */
    @Test
    @DisplayName("a deposed leader's write is refused and nothing from it lands")
    void staleTermIsFenced() {
        long stale = leadership.claim("paused-leader", T0);
        leadership.claim("new-leader", T0);

        assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
            leadership.assertCurrent(stale);
            quotes.writeBatch(List.of(quote(AAPL, 1, T0, "100")));
            checkpoints.save(T0, 1, T0);
        })).isInstanceOf(LeadershipRepository.FencedException.class);

        assertThat(storedQuotes()).isZero();
        assertThat(checkpoints.load().isPresent()).isFalse();
    }

    // ------------------------------------------------------------------ election

    @Test
    @DisplayName("exactly one candidate leads, and a standby takes over when it stops")
    void electionFailsOver() throws Exception {
        AtomicInteger leaders = new AtomicInteger();
        Recorder a = new Recorder("a", leaders);
        Recorder b = new Recorder("b", leaders);
        LockRegistryLeaderInitiator first = initiator(a);
        LockRegistryLeaderInitiator second = initiator(b);
        first.start();
        second.start();
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> leaders.get() == 1);
            Thread.sleep(1_000);
            assertThat(leaders.get()).as("never two leaders at once").isEqualTo(1);

            Recorder leader = a.leading ? a : b;
            Recorder standby = leader == a ? b : a;
            (leader == a ? first : second).stop();

            await().atMost(Duration.ofSeconds(10)).until(() -> standby.leading);
            assertThat(leaders.get()).isEqualTo(1);
        } finally {
            first.stop();
            second.stop();
        }
    }

    private static LockRegistryLeaderInitiator initiator(DefaultCandidate candidate) {
        DefaultLockRepository repository = new DefaultLockRepository(dataSource);
        repository.setRegion("marketdata");
        repository.setTimeToLive(2_000);
        repository.setTransactionManager(transactionManager);
        repository.afterPropertiesSet();
        repository.afterSingletonsInstantiated();
        LockRegistryLeaderInitiator initiator =
                new LockRegistryLeaderInitiator(new JdbcLockRegistry(repository), candidate);
        initiator.setHeartBeatMillis(300);
        initiator.setBusyWaitMillis(100);
        return initiator;
    }

    private static final class Recorder extends DefaultCandidate {

        private final AtomicInteger leaders;
        volatile boolean leading;

        Recorder(String id, AtomicInteger leaders) {
            super(id, "marketdata-ingestion");
            this.leaders = leaders;
        }

        @Override
        public void onGranted(Context context) {
            leading = true;
            leaders.incrementAndGet();
        }

        @Override
        public void onRevoked(Context context) {
            leading = false;
            leaders.decrementAndGet();
        }
    }

    // ------------------------------------------------------------------ helpers

    private static long storedQuotes() {
        return jdbc.getJdbcTemplate().queryForObject("SELECT count(*) FROM quote_history", Long.class);
    }

    private static int write(List<Quote> batch) {
        return writeAndPublish(batch).inserted();
    }

    private static QuoteRepository.WriteResult writeAndPublish(List<Quote> batch) {
        return transactions.execute(status -> quotes.writeBatch(batch));
    }

    private static Quote quote(Isin isin, long sequence, Instant eventTime, String price) {
        BigDecimal mid = new BigDecimal(price);
        return new Quote(isin, sequence, mid.subtract(new BigDecimal("0.01")), mid.add(new BigDecimal("0.01")),
                BigDecimal.valueOf(100), BigDecimal.valueOf(100), "USD", eventTime, eventTime.plusMillis(5));
    }
}
