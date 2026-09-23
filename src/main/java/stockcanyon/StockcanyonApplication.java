package stockcanyon;

import java.time.Clock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Consumes a price feed from a Stock Exchange over a WebSocket, stores it, and exposes the latest
 * quote by ISIN. See {@code DESIGN.md}.
 *
 * <p>Spring Boot's JDBC auto-configuration is excluded because {@link MarketDataConfig} defines its
 * own {@code DataSource}, Flyway run and transaction manager. Left on it would duplicate the last
 * two, and would fail the context whenever the module is disabled.
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

    /** One clock, injected, so time-dependent behaviour is testable. */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
