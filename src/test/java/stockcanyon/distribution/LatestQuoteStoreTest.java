package stockcanyon.distribution;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;

import stockcanyon.Isin;

/** The newer-only rule, in Redis, against the redeliveries Kafka actually produces. */
class LatestQuoteStoreTest {

    private static final String AAPL = "US0378331005";
    private static final String MSFT = "US5949181045";
    private static final Instant T0 = Instant.parse("2026-09-26T10:00:00.123456Z");

    private static GenericContainer<?> redisContainer;
    private static LettuceConnectionFactory connections;
    private static StringRedisTemplate redis;
    private static LatestQuoteStore store;

    @BeforeAll
    static void start() {
        redisContainer = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);
        redisContainer.start();
        connections = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getMappedPort(6379));
        connections.afterPropertiesSet();
        redis = new StringRedisTemplate(connections);
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        store = new LatestQuoteStore(redis, mapper);
    }

    @AfterAll
    static void stop() {
        connections.destroy();
        redisContainer.stop();
    }

    @BeforeEach
    void flush() {
        connections.getConnection().serverCommands().flushDb();
    }

    @Test
    @DisplayName("a newer quote replaces the stored one and round-trips exactly")
    void newerWins() {
        assertThat(store.apply(List.of(event(AAPL, 1, T0, "100.10000000")))).isEqualTo(1);
        assertThat(store.apply(List.of(event(AAPL, 2, T0.plusMillis(5), "100.20000000")))).isEqualTo(1);

        var stored = store.findLatest(Isin.of(AAPL)).orElseThrow();
        assertThat(stored.sequence()).isEqualTo(2);
        assertThat(stored.bid()).isEqualByComparingTo("100.20000000");
        assertThat(stored.eventTime()).isEqualTo(T0.plusMillis(5));
    }

    /** What a consumer crash or rebalance produces: a stretch of events already applied, again. */
    @Test
    @DisplayName("a redelivered older quote is refused, so the price never moves backwards")
    void redeliveryCannotRegress() {
        store.apply(List.of(event(AAPL, 1, T0, "100"), event(AAPL, 2, T0.plusMillis(1), "101"),
                event(AAPL, 3, T0.plusMillis(2), "102")));

        int moved = store.apply(List.of(event(AAPL, 1, T0, "100"), event(AAPL, 2, T0.plusMillis(1), "101")));

        assertThat(moved).isZero();
        assertThat(store.findLatest(Isin.of(AAPL)).orElseThrow().sequence()).isEqualTo(3);
    }

    @Test
    @DisplayName("at the same instant, the sequence breaks the tie; an exact duplicate is a no-op")
    void sequenceBreaksTies() {
        store.apply(List.of(event(AAPL, 7, T0, "100")));

        assertThat(store.apply(List.of(event(AAPL, 6, T0, "99")))).isZero();
        assertThat(store.apply(List.of(event(AAPL, 7, T0, "100")))).isZero();
        assertThat(store.apply(List.of(event(AAPL, 8, T0, "101")))).isEqualTo(1);
        assertThat(store.findLatest(Isin.of(AAPL)).orElseThrow().sequence()).isEqualTo(8);
    }

    @Test
    @DisplayName("instruments are independent within one pipelined batch")
    void batchIsPerInstrument() {
        int moved = store.apply(List.of(event(AAPL, 5, T0, "100"), event(MSFT, 1, T0, "200"),
                event(AAPL, 4, T0.minusMillis(1), "99")));

        assertThat(moved).isEqualTo(2);
        assertThat(store.findLatest(Isin.of(AAPL)).orElseThrow().sequence()).isEqualTo(5);
        assertThat(store.findLatest(Isin.of(MSFT)).orElseThrow().sequence()).isEqualTo(1);
    }

    @Test
    @DisplayName("the consumer position is stored with the prices it covers, and lost with them")
    void positionTravelsWithTheData() {
        store.apply(List.of(event(AAPL, 1, T0, "100")), Map.of("marketdata.latest-quote:0", 42L));
        assertThat(store.storedOffsets()).containsEntry("marketdata.latest-quote:0", 42L);

        connections.getConnection().serverCommands().flushDb();
        assertThat(store.storedOffsets()).isEmpty();
        assertThat(store.findLatest(Isin.of(AAPL))).isEmpty();
    }

    @Test
    @DisplayName("an instrument never quoted is absent, not zero")
    void unknownIsAbsent() {
        assertThat(store.findLatest(Isin.of(AAPL))).isEmpty();
    }

    private static LatestQuoteEvent event(String isin, long sequence, Instant eventTime, String bid) {
        BigDecimal price = new BigDecimal(bid);
        return new LatestQuoteEvent(isin, "USD", price, price.add(BigDecimal.ONE), BigDecimal.TEN,
                BigDecimal.TEN, sequence, eventTime, eventTime.plusMillis(1));
    }
}
