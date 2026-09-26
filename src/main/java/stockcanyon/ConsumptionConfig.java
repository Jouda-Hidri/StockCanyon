package stockcanyon;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.UUID;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.flywaydb.core.Flyway;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.integration.support.leader.LockRegistryLeaderInitiator;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import stockcanyon.consumption.ExchangeWebSocketClient;
import stockcanyon.consumption.IngestionHealthIndicator;
import stockcanyon.consumption.IngestionLeader;
import stockcanyon.consumption.OutboxReseedEndpoint;
import stockcanyon.consumption.QuoteConsumer;
import stockcanyon.storage.CheckpointRepository;
import stockcanyon.storage.LeadershipRepository;
import stockcanyon.storage.QuoteRepository;

/**
 * The consumption service: the feed, its PostgreSQL, the election, and the outbox.
 *
 * <p>Every instance stands for election; exactly one ingests at a time. Its only output to the rest
 * of the system is the outbox, which Debezium publishes to Kafka. It serves no quotes itself.
 */
@Configuration
@ConditionalOnProperty(prefix = "marketdata.consumption", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class ConsumptionConfig {

    /** The writer, the lock renewals and the read path each need a connection or two. */
    private static final int POOL_SIZE = 10;

    @Bean(destroyMethod = "close")
    public DataSource marketDataDataSource(MarketDataProperties properties, MeterRegistry meters) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(properties.database().url());
        config.setUsername(properties.database().username());
        config.setPassword(properties.database().password());
        config.setMaximumPoolSize(POOL_SIZE);
        config.setPoolName("marketdata");
        // Set before the pool starts: Hikari cannot attach metrics to a pool already running.
        // Gives hikaricp_connections_{active,pending,timeout} and acquisition/usage timings.
        config.setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory(meters));
        return new HikariDataSource(config);
    }

    /** Migrates on start-up, before anything can query. */
    @Bean
    public Flyway marketDataFlyway(DataSource marketDataDataSource) {
        Flyway flyway = Flyway.configure()
                .dataSource(marketDataDataSource)
                .locations("classpath:db/migration/marketdata")
                .table("flyway_schema_history_marketdata")
                .load();
        flyway.migrate();
        return flyway;
    }

    // On the template, not each repository, so a new repository cannot forget it.
    @Bean
    @DependsOn("marketDataFlyway")
    public NamedParameterJdbcTemplate marketDataJdbcTemplate(DataSource marketDataDataSource) {
        return new NamedParameterJdbcTemplate(marketDataDataSource);
    }

    @Bean
    public PlatformTransactionManager marketDataTransactionManager(DataSource marketDataDataSource) {
        return new DataSourceTransactionManager(marketDataDataSource);
    }

    @Bean
    public TransactionTemplate marketDataTransactionTemplate(
            PlatformTransactionManager marketDataTransactionManager) {
        return new TransactionTemplate(marketDataTransactionManager);
    }

    @Bean
    public QuoteRepository quoteRepository(
            NamedParameterJdbcTemplate marketDataJdbcTemplate, DataSource marketDataDataSource) {
        return new QuoteRepository(marketDataJdbcTemplate, marketDataDataSource);
    }

    @Bean
    public CheckpointRepository checkpointRepository(NamedParameterJdbcTemplate marketDataJdbcTemplate) {
        return new CheckpointRepository(marketDataJdbcTemplate);
    }

    @Bean
    OutboxReseedEndpoint outboxReseedEndpoint(
            QuoteRepository quoteRepository, TransactionTemplate marketDataTransactionTemplate) {
        return new OutboxReseedEndpoint(quoteRepository, marketDataTransactionTemplate);
    }

    /**
     * Pod name under Kubernetes, host name elsewhere, plus a suffix so two processes on one host
     * are still two candidates. It is what {@code ingest_leader.holder} records.
     */
    @Bean
    String ingestionInstanceId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "unknown";
        }
        return host + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Bean
    LeadershipRepository leadershipRepository(NamedParameterJdbcTemplate marketDataJdbcTemplate) {
        return new LeadershipRepository(marketDataJdbcTemplate);
    }

    @Bean
    ExchangeWebSocketClient exchangeWebSocketClient(
            MarketDataProperties properties, ObjectMapper objectMapper, Clock clock, MeterRegistry meters) {
        return new ExchangeWebSocketClient(
                URI.create(properties.exchangeUrl()), objectMapper, clock, properties.consumption(), meters);
    }

    @Bean
    QuoteConsumer quoteConsumer(
            ExchangeWebSocketClient exchangeWebSocketClient,
            QuoteRepository quoteRepository,
            CheckpointRepository checkpointRepository,
            LeadershipRepository leadershipRepository,
            TransactionTemplate marketDataTransactionTemplate,
            Clock clock,
            MarketDataProperties properties,
            MeterRegistry meters,
            String ingestionInstanceId) {
        return new QuoteConsumer(
                exchangeWebSocketClient,
                quoteRepository,
                checkpointRepository,
                leadershipRepository,
                marketDataTransactionTemplate,
                clock,
                properties.consumption(),
                meters,
                ingestionInstanceId);
    }

    @Bean
    IngestionHealthIndicator ingestionHealthIndicator(QuoteConsumer quoteConsumer) {
        return new IngestionHealthIndicator(quoteConsumer);
    }

    // ------------------------------------------------------------------ election

    @Bean
    @DependsOn("marketDataFlyway")
    DefaultLockRepository ingestionLockRepository(
            DataSource marketDataDataSource,
            PlatformTransactionManager marketDataTransactionManager,
            MarketDataProperties properties) {
        DefaultLockRepository repository = new DefaultLockRepository(marketDataDataSource);
        repository.setRegion("marketdata");
        repository.setTimeToLive((int) properties.leadership().lockTtl().toMillis());
        repository.setTransactionManager(marketDataTransactionManager);
        return repository;
    }

    @Bean
    JdbcLockRegistry ingestionLockRegistry(DefaultLockRepository ingestionLockRepository) {
        return new JdbcLockRegistry(ingestionLockRepository);
    }

    @Bean
    IngestionLeader ingestionLeader(
            String ingestionInstanceId, QuoteConsumer quoteConsumer, MeterRegistry meters) {
        return new IngestionLeader(ingestionInstanceId, quoteConsumer, meters);
    }

    /**
     * Stops after the consumer (a lower phase stops later), so shutdown drains the queue while
     * still holding the lock, and only then lets a standby take over.
     */
    @Bean
    LockRegistryLeaderInitiator ingestionLeaderInitiator(
            JdbcLockRegistry ingestionLockRegistry,
            IngestionLeader ingestionLeader,
            MarketDataProperties properties) {
        LockRegistryLeaderInitiator initiator =
                new LockRegistryLeaderInitiator(ingestionLockRegistry, ingestionLeader);
        initiator.setHeartBeatMillis(properties.leadership().heartbeat().toMillis());
        initiator.setBusyWaitMillis(properties.leadership().heartbeat().toMillis());
        initiator.setPhase(Integer.MAX_VALUE - 1000);
        return initiator;
    }
}
