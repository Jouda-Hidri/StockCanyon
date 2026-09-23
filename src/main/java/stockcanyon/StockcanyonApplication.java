package stockcanyon;

import java.time.Clock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Market Data Service.
 *
 * <p>Consumes a price feed from a Stock Exchange over a WebSocket, stores it, and exposes the
 * latest quote by ISIN to other internal services. See {@code DESIGN.md} in this package.
 *
 * <p>Spring Boot's JDBC auto-configuration is excluded because this service configures its own
 * {@code DataSource}, Flyway run and transaction manager in {@link MarketDataConfig}. Left on, it
 * would define a second {@code JdbcTemplate} and a second transaction manager beside them, and
 * would fail the context outright whenever the module is disabled — HikariCP on the classpath is
 * enough for it to conclude the application wants a {@code DataSource}, and it then finds no
 * {@code spring.datasource.url} to build one from.
 */
@SpringBootApplication(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class
})
public class StockcanyonApplication {

    public static void main(String[] args) {
        SpringApplication.run(StockcanyonApplication.class, args);
    }

    /**
     * One clock for the whole service.
     *
     * <p>Injected rather than calling {@code Instant.now()} in place, because almost everything
     * here is time-dependent — quote age, consumption lag, the checkpoint — and none of it is
     * testable if the time source is a static method.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
