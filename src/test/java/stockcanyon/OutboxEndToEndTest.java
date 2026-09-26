package stockcanyon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;

import stockcanyon.consumption.QuoteConsumer;
import stockcanyon.simulator.SimulatedExchangeHandler;

/**
 * The whole path, with nothing stubbed: exchange → consumption service → PostgreSQL + outbox →
 * Debezium (Kafka Connect) → compacted topic → distribution service → Redis → API.
 *
 * <p>The connector is registered from {@code deploy/compose/marketdata-outbox-connector.json}, the
 * file Compose uses, so the test fails if that configuration is wrong.
 *
 * <p>What it proves is the property the outbox exists for: every instrument's latest quote reaches
 * Redis, and it never moves backwards there — not even while the consumption service is replaying
 * old quotes after a disconnect.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class OutboxEndToEndTest {

    private static final String AAPL = "US0378331005";
    private static final int PORT = freePort();

    private static final Network NETWORK = Network.newNetwork();

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("marketdata").withUsername("marketdata").withPassword("marketdata")
            .withCommand("postgres", "-c", "wal_level=logical")
            .withNetwork(NETWORK).withNetworkAliases("postgres");

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1")
            .withNetwork(NETWORK).withNetworkAliases("kafka").withListener("kafka:19092");

    private static final GenericContainer<?> CONNECT =
            new GenericContainer<>("quay.io/debezium/connect:3.6.3.Final")
                    .withNetwork(NETWORK)
                    .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
                    .withEnv("GROUP_ID", "marketdata-connect")
                    .withEnv("CONFIG_STORAGE_TOPIC", "connect-configs")
                    .withEnv("OFFSET_STORAGE_TOPIC", "connect-offsets")
                    .withEnv("STATUS_STORAGE_TOPIC", "connect-status")
                    .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                    .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                    .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                    // The image bundles a dozen connectors, and scanning them all took over three
                    // minutes on a loaded machine. Keep only PostgreSQL, and discover plugins through
                    // their service manifests instead of a reflective classpath scan.
                    .withEnv("CONNECT_PLUGIN_DISCOVERY", "service_load")
                    .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("bash", "-c",
                            "find /kafka/connect -mindepth 1 -maxdepth 1 ! -name debezium-connector-postgres "
                                    + "-exec rm -rf {} + && exec /docker-entrypoint.sh start"))
                    .withExposedPorts(8083)
                    .waitingFor(Wait.forHttp("/connectors").forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(5)));

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

    static {
        POSTGRES.start();
        KAFKA.start();
        CONNECT.dependsOn(KAFKA).start();
        REDIS.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("server.port", () -> PORT);
        registry.add("marketdata.consumption.enabled", () -> true);
        registry.add("marketdata.distribution.enabled", () -> true);
        registry.add("marketdata.simulator.enabled", () -> true);
        registry.add("marketdata.simulator.quotes-per-second", () -> 300);
        registry.add("marketdata.exchange-url", () -> "ws://localhost:" + PORT + "/exchange/quotes");
        registry.add("marketdata.database.url", POSTGRES::getJdbcUrl);
        registry.add("marketdata.database.username", POSTGRES::getUsername);
        registry.add("marketdata.database.password", POSTGRES::getPassword);
        registry.add("marketdata.consumption.flush-interval", () -> "50ms");
        // Small batches, so a replay is published as batches of old quotes only. With large ones a
        // slow reconnect lets live quotes pile up behind the replay, every instrument's newest in the
        // batch is then a fresh quote, and there is nothing old left to refuse.
        registry.add("marketdata.consumption.max-batch-size", () -> 100);
        registry.add("marketdata.consumption.reconnect-initial-delay", () -> "50ms");
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    private static boolean connectorRegistered;

    @Autowired
    QuoteConsumer consumer;

    @Autowired
    SimulatedExchangeHandler exchange;

    @Autowired
    NamedParameterJdbcTemplate marketDataJdbcTemplate;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    TestRestTemplate http;

    @Autowired
    io.micrometer.core.instrument.MeterRegistry meters;

    /** After the context has started, because the connector needs the migrated outbox and publication. */
    @BeforeEach
    void registerConnector() throws Exception {
        if (connectorRegistered) {
            return;
        }
        String config = Files.readString(Path.of("deploy/compose/marketdata-outbox-connector.json"));
        String connect = "http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083);
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> put = client.send(HttpRequest.newBuilder(
                        URI.create(connect + "/connectors/marketdata-outbox/config"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(config)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(put.statusCode()).as(put.body()).isIn(200, 201);

        await("the connector and its task to run")
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    String status = client.send(HttpRequest.newBuilder(
                                    URI.create(connect + "/connectors/marketdata-outbox/status")).build(),
                            HttpResponse.BodyHandlers.ofString()).body();
                    return status.contains("\"tasks\":[{") && !status.contains("FAILED")
                            && status.split("RUNNING", -1).length - 1 >= 2;
                });
        connectorRegistered = true;
    }

    @Test
    @DisplayName("every instrument's latest quote reaches Redis and the API through the outbox")
    void latestQuotesPropagate() {
        await("the first quote to reach the API")
                .atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> http.getForEntity(url(AAPL), String.class).getStatusCode().is2xxSuccessful());

        Map<String, Long> source = latestInPostgres();
        assertThat(source).hasSizeGreaterThan(3);
        awaitRedisCatchesUpTo(source);

        assertThat(marketDataJdbcTemplate.getJdbcTemplate().queryForObject("SELECT count(*) FROM outbox", Long.class))
                .as("the outbox only ever holds rows inside a transaction").isZero();
    }

    /**
     * A replay makes the ingestion service publish old prices again. Redis must refuse every one
     * of them: the distribution service is the only place that decides what is newer.
     *
     * <p>Forced with a hole rather than a plain disconnect: a disconnect replays only the last
     * flush interval, whose old prices share a batch with newer ones and are coalesced away before
     * publishing. A hole holds the checkpoint back for the whole backfill grace period, so the
     * replay re-publishes two seconds of prices the distribution service already has.
     */
    @Test
    @DisplayName("replayed old prices are published again, and refused by the distribution service")
    void replayNeverRegressesRedis() {
        await().atMost(Duration.ofSeconds(90)).until(() -> latestInRedis().size() > 3);
        Map<String, Long> before = latestInRedis();
        long gapsBefore = consumer.status().gapsDetected();

        exchange.skipNext(3);
        await("the hole to be detected and backfilled")
                .atMost(Duration.ofSeconds(60))
                .until(() -> consumer.status().gapsDetected() > gapsBefore
                        && consumer.status().sequencesOutstanding() == 0);

        Map<String, Long> after = latestInRedis();
        before.forEach((isin, sequence) -> assertThat(after.get(isin))
                .as("Redis latest for %s must not regress", isin)
                .isGreaterThanOrEqualTo(sequence));
        awaitRedisCatchesUpTo(latestInPostgres());

        await("the replayed old prices to reach the distribution service and be refused there")
                .atMost(Duration.ofSeconds(30))
                .until(() -> meters.counter("marketdata.projection.events", "outcome", "stale").count() > 0);
    }

    @Test
    @DisplayName("the connector created the topic compacted, keyed by ISIN")
    void topicIsCompacted() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            await().atMost(Duration.ofSeconds(90))
                    .until(() -> admin.listTopics().names().get().contains("marketdata.latest-quote"));
            ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, "marketdata.latest-quote");
            String policy = admin.describeConfigs(List.of(topic)).all().get().get(topic)
                    .get("cleanup.policy").value();
            assertThat(policy).isEqualTo("compact");
        }
    }

    private void awaitRedisCatchesUpTo(Map<String, Long> source) {
        await("Redis to hold every instrument's latest quote, or a newer one")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> {
                    Map<String, Long> projected = latestInRedis();
                    source.forEach((isin, sequence) -> assertThat(projected.get(isin))
                            .as("Redis latest for %s", isin)
                            .isNotNull()
                            .isGreaterThanOrEqualTo(sequence));
                });
    }

    private Map<String, Long> latestInPostgres() {
        Map<String, Long> latest = new HashMap<>();
        marketDataJdbcTemplate.getJdbcTemplate().query(
                "SELECT DISTINCT ON (isin) isin, sequence FROM quote_history ORDER BY isin, event_time DESC, sequence DESC",
                rs -> {
                    latest.put(rs.getString("isin"), rs.getLong("sequence"));
                });
        return latest;
    }

    private Map<String, Long> latestInRedis() {
        Map<String, Long> latest = new HashMap<>();
        var keys = redis.keys("marketdata:latest:*");
        if (keys == null) {
            return latest;
        }
        for (String key : keys) {
            Object sequence = redis.opsForHash().get(key, "seq");
            if (sequence != null) {
                latest.put(key.substring("marketdata:latest:".length()), Long.parseLong(sequence.toString()));
            }
        }
        return latest;
    }

    private static String url(String isin) {
        return "http://localhost:" + PORT + "/api/v1/marketdata/quotes/" + isin + "/latest";
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not reserve a port for the test", e);
        }
    }
}
