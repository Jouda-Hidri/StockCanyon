package stockcanyon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import stockcanyon.consumption.QuoteConsumer;

/**
 * Covers data distribution. Driven over HTTP, because what is checked — status codes, the ISIN
 * parsed from a path variable — only exists once a request has been through the whole stack.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class MarketDataApiTest {

    private static final String AAPL = "US0378331005";

    /** Valid ISIN (Toyota), but outside the exchange's universe, so nothing is ever stored for it. */
    private static final String NEVER_QUOTED = "JP3633400001";

    private static final int PORT = freePort();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("server.port", () -> PORT);
        registry.add("marketdata.enabled", () -> true);
        registry.add("marketdata.exchange-url", () -> "ws://localhost:" + PORT + "/exchange/quotes");
        registry.add("marketdata.simulator.enabled", () -> true);
        registry.add("marketdata.simulator.quotes-per-second", () -> 400);
        registry.add("marketdata.database.url", () -> SharedPostgres.jdbcUrlFor("api_test"));
        registry.add("marketdata.database.username", SharedPostgres.INSTANCE::getUsername);
        registry.add("marketdata.database.password", SharedPostgres.INSTANCE::getPassword);
        registry.add("marketdata.consumption.flush-interval", () -> "50ms");
    }

    @Autowired
    TestRestTemplate http;

    @Autowired
    QuoteConsumer consumer;

    private void awaitQuotes() {
        await("the feed to deliver quotes")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> consumer.status().quotesConsumed() > 500);
    }

    @Test
    @DisplayName("the latest quote is served by ISIN")
    void latestByIsin() {
        awaitQuotes();

        ResponseEntity<Map<String, Object>> response = http.exchange(
                url("/quotes/" + AAPL + "/latest"), HttpMethod.GET, null,
                new ParameterizedTypeReference<Map<String, Object>>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.get("isin")).isEqualTo(AAPL);
        assertThat(body.get("currency")).isEqualTo("USD");
        assertThat(body).containsKeys("bid", "ask", "mid", "sequence", "eventTime", "ageMillis");
        // A price that arrived seconds ago must not be reported as hours old.
        assertThat(((Number) body.get("ageMillis")).longValue()).isLessThan(60_000);
    }

    /**
     * Two distinct failures need two distinct codes: a malformed ISIN will never succeed however
     * often it is retried, while one with no quote yet may resolve on its own.
     */
    @Test
    @DisplayName("a malformed ISIN is rejected, a merely unknown one is not found")
    void distinguishesMalformedFromUnknown() {
        awaitQuotes();

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
    @DisplayName("status reports consumption state, including whether anything was missed")
    @SuppressWarnings("unchecked")
    void statusReportsConsumption() {
        awaitQuotes();

        Map<String, Object> body = http.getForObject(url("/status"), Map.class);

        assertThat(body).isNotNull();
        assertThat(body.get("consuming")).isEqualTo(true);
        assertThat(((Number) body.get("storedQuotes")).longValue()).isPositive();

        Map<String, Object> feed = (Map<String, Object>) body.get("feed");
        assertThat(feed.get("connected")).isEqualTo(true);
        assertThat(((Number) feed.get("quotesMissing")).longValue()).isZero();
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
