package stockcanyon;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import stockcanyon.distribution.LatestQuoteEvent;
import stockcanyon.distribution.LatestQuoteStore;

/**
 * The distribution service on its own: Redis and the API, no database and no feed.
 *
 * <p>Redis is seeded directly, as the Kafka projection would; {@code OutboxEndToEndTest} covers
 * the path that fills it in production. Driven over HTTP, because what is checked — status codes,
 * the ISIN parsed from a path variable — only exists once a request has been through the stack.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class MarketDataApiTest {

    private static final String AAPL = "US0378331005";

    /** Valid ISIN (Toyota), never seeded. */
    private static final String NEVER_QUOTED = "JP3633400001";

    private static final int PORT = freePort();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("server.port", () -> PORT);
        registry.add("marketdata.consumption.enabled", () -> false);
        registry.add("marketdata.distribution.enabled", () -> true);
        registry.add("spring.data.redis.host", SharedRedis.INSTANCE::getHost);
        registry.add("spring.data.redis.port", SharedRedis::port);
        // No Kafka here: the projector is not under test.
        registry.add("spring.kafka.listener.auto-startup", () -> false);
    }

    @Autowired
    TestRestTemplate http;

    @Autowired
    LatestQuoteStore store;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    ApplicationContext context;

    @BeforeEach
    void seed() {
        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
        Instant now = Instant.now();
        store.apply(List.of(new LatestQuoteEvent(AAPL, "USD", new BigDecimal("225.96040000"),
                new BigDecimal("226.05080000"), BigDecimal.TEN, BigDecimal.ONE, 21457, now, now)));
    }

    @Test
    @DisplayName("the latest quote is served by ISIN, from Redis")
    void latestByIsin() {
        ResponseEntity<Map<String, Object>> response = http.exchange(
                url("/quotes/" + AAPL + "/latest"), HttpMethod.GET, null,
                new ParameterizedTypeReference<Map<String, Object>>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.get("isin")).isEqualTo(AAPL);
        assertThat(body.get("currency")).isEqualTo("USD");
        assertThat(((Number) body.get("sequence")).longValue()).isEqualTo(21457);
        assertThat(body).containsKeys("bid", "ask", "mid", "sequence", "eventTime", "ageMillis");
        assertThat(((Number) body.get("ageMillis")).longValue()).isLessThan(60_000);
    }

    /**
     * Two distinct failures need two distinct codes: a malformed ISIN will never succeed however
     * often it is retried, while one with no quote yet may resolve on its own.
     */
    @Test
    @DisplayName("a malformed ISIN is rejected, a merely unknown one is not found")
    void distinguishesMalformedFromUnknown() {
        // Apple's ISIN with the check digit changed: right shape, wrong identifier.
        assertThat(http.getForEntity(url("/quotes/US0378331006/latest"), String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForEntity(url("/quotes/NOTANISIN/latest"), String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForEntity(url("/quotes/" + NEVER_QUOTED + "/latest"), String.class)
                .getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("an unreachable store is 503 with Retry-After, not 500")
    void storeDownIsRetryable() {
        SharedRedis.INSTANCE.getDockerClient().pauseContainerCmd(SharedRedis.INSTANCE.getContainerId()).exec();
        try {
            ResponseEntity<String> response = http.getForEntity(url("/quotes/" + AAPL + "/latest"), String.class);
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
            assertThat(response.getHeaders().getFirst("Retry-After")).isEqualTo("1");
        } finally {
            SharedRedis.INSTANCE.getDockerClient().unpauseContainerCmd(SharedRedis.INSTANCE.getContainerId()).exec();
        }
    }

    /** One datastore per service: the distribution service must not even be able to reach PostgreSQL. */
    @Test
    @DisplayName("the distribution service has no database and no consumption endpoints")
    void ownsOnlyRedis() {
        assertThat(context.getBeanNamesForType(javax.sql.DataSource.class)).isEmpty();
        assertThat(http.getForEntity(url("/status"), String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    private static String url(String path) {
        return "http://localhost:" + PORT + "/api/v1/marketdata" + path;
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not reserve a port for the test", e);
        }
    }
}
