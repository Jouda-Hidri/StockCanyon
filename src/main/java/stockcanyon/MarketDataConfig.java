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
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import stockcanyon.consumption.ExchangeWebSocketClient;
import stockcanyon.consumption.QuoteConsumer;
import stockcanyon.storage.CheckpointRepository;
import stockcanyon.storage.QuoteRepository;

/** Wires consumption, storage and distribution. */
@Configuration
@EnableConfigurationProperties(MarketDataProperties.class)
@ConditionalOnProperty(prefix = "marketdata", name = "enabled", havingValue = "true")
public class MarketDataConfig {

    /** The write path is one thread and the read path one indexed lookup. */
    private static final int POOL_SIZE = 8;

    @Bean(destroyMethod = "close")
    public DataSource marketDataDataSource(MarketDataProperties properties) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(properties.database().url());
        config.setUsername(properties.database().username());
        config.setPassword(properties.database().password());
        config.setMaximumPoolSize(POOL_SIZE);
        config.setPoolName("marketdata");
        // One multi-row statement per batch: one round trip per flush, not per quote.
        config.addDataSourceProperty("reWriteBatchedInserts", "true");
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
    public QuoteRepository quoteRepository(NamedParameterJdbcTemplate marketDataJdbcTemplate) {
        return new QuoteRepository(marketDataJdbcTemplate);
    }

    @Bean
    public CheckpointRepository checkpointRepository(NamedParameterJdbcTemplate marketDataJdbcTemplate) {
        return new CheckpointRepository(marketDataJdbcTemplate);
    }

    @Bean
    public ExchangeWebSocketClient exchangeWebSocketClient(
            MarketDataProperties properties, ObjectMapper objectMapper, Clock clock) {
        return new ExchangeWebSocketClient(
                URI.create(properties.exchangeUrl()), objectMapper, clock, properties.consumption());
    }

    @Bean
    public QuoteConsumer quoteConsumer(
            ExchangeWebSocketClient exchangeWebSocketClient,
            QuoteRepository quoteRepository,
            CheckpointRepository checkpointRepository,
            TransactionTemplate marketDataTransactionTemplate,
            Clock clock,
            MarketDataProperties properties) {
        return new QuoteConsumer(
                exchangeWebSocketClient,
                quoteRepository,
                checkpointRepository,
                marketDataTransactionTemplate,
                clock,
                properties.consumption());
    }
}
