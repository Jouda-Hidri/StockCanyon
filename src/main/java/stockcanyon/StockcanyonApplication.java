package stockcanyon;

import java.time.Clock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Consumes a price feed from a Stock Exchange over a WebSocket, stores it, and exposes the latest
 * quote by ISIN. See {@code DESIGN.md}.
 *
 * <p>Two services and a stand-in exchange, one per role — see {@link MarketDataProperties}.
 * Spring Boot's JDBC auto-configuration is excluded because {@link ConsumptionConfig} defines its
 * own {@code DataSource}, Flyway run and transaction manager, and the distribution service must
 * not have a database at all.
 */
@SpringBootApplication(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class
})
// Here, not on a role's configuration, so every role can read it whichever others are off.
@EnableConfigurationProperties(MarketDataProperties.class)
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
