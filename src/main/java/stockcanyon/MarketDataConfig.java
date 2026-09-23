package stockcanyon;

import java.net.URI;
import java.time.Clock;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import stockcanyon.consumption.ExchangeWebSocketClient;
import stockcanyon.consumption.QuoteBuffer;
import stockcanyon.consumption.QuoteConsumer;
import stockcanyon.storage.CheckpointRepository;
import stockcanyon.storage.QuoteRepository;

/**
 * Wires the three parts of the market data service: consumption, storage and distribution.
 *
 * <p>Everything here is conditional on {@code marketdata.enabled}. That matters more than a feature
 * flag normally would: this is the only part of Bankster that needs a database, and an
 * unconditional {@code DataSource} would mean every existing component — none of which has ever
 * persisted anything — could no longer start without a PostgreSQL to connect to.
 *
 * <p>The pool, the migrations and the transaction manager are this module's own rather than
 * application-wide, for the same reason: adding market data should not change how anything else
 * starts.
 */
@Configuration
@EnableConfigurationProperties(MarketDataProperties.class)
@ConditionalOnProperty(prefix = "marketdata", name = "enabled", havingValue = "true")
public class MarketDataConfig {

    // ---------------------------------------------------------------- storage

    @Bean(destroyMethod = "close")
    public DataSource marketDataDataSource(MarketDataProperties properties) {
        MarketDataProperties.DataSourceSettings settings = properties.getDatasource();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(settings.getUrl());
        config.setUsername(settings.getUsername());
        config.setPassword(settings.getPassword());
        config.setMaximumPoolSize(settings.getMaxPoolSize());
        config.setPoolName("marketdata");
        // Turns each batched insert into one multi-row statement. On the write path this is the
        // difference between one round trip per quote and one per flush.
        config.addDataSourceProperty("reWriteBatchedInserts", "true");
        return new HikariDataSource(config);
    }

    /** Migrates on start-up, before anything can query. Its own history table. */
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

    // Expressing the migration dependency on the template rather than on each repository means it
    // cannot be forgotten when a repository is added.
    @Bean
    @DependsOn("marketDataFlyway")
    public JdbcTemplate marketDataJdbcTemplate(DataSource marketDataDataSource) {
        return new JdbcTemplate(marketDataDataSource);
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
    public QuoteRepository quoteRepository(JdbcTemplate marketDataJdbcTemplate) {
        return new QuoteRepository(marketDataJdbcTemplate);
    }

    @Bean
    public CheckpointRepository checkpointRepository(JdbcTemplate marketDataJdbcTemplate) {
        return new CheckpointRepository(marketDataJdbcTemplate);
    }

    // ---------------------------------------------------------------- consumption

    /**
     * Only on the instance that consumes.
     *
     * <p>Switched off on read replicas so exactly one process writes. Several writers would each
     * keep their own checkpoint and replay each other's work — correct, thanks to the
     * deduplicating key, but a multiple of the necessary write load.
     */
    @Bean
    @ConditionalOnProperty(prefix = "marketdata.consumption", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public ExchangeWebSocketClient exchangeWebSocketClient(
            MarketDataProperties properties, ObjectMapper objectMapper, Clock clock) {
        return new ExchangeWebSocketClient(
                URI.create(properties.getExchangeUrl()),
                objectMapper,
                clock,
                properties.getConsumption());
    }

    @Bean
    @ConditionalOnProperty(prefix = "marketdata.consumption", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public QuoteBuffer quoteBuffer(MarketDataProperties properties) {
        return new QuoteBuffer(properties.getConsumption().getBufferCapacity());
    }

    @Bean
    @ConditionalOnProperty(prefix = "marketdata.consumption", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public QuoteConsumer quoteConsumer(
            ExchangeWebSocketClient exchangeWebSocketClient,
            QuoteBuffer quoteBuffer,
            QuoteRepository quoteRepository,
            CheckpointRepository checkpointRepository,
            TransactionTemplate marketDataTransactionTemplate,
            Clock clock,
            MarketDataProperties properties) {
        return new QuoteConsumer(
                exchangeWebSocketClient,
                quoteBuffer,
                quoteRepository,
                checkpointRepository,
                marketDataTransactionTemplate,
                clock,
                properties.getConsumption().getMaxBatchSize(),
                properties.getConsumption().getFlushInterval());
    }
}
