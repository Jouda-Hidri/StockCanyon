package stockcanyon;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import stockcanyon.consumption.QuoteConsumer;
import stockcanyon.simulator.SimulatedExchangeHandler;

/**
 * The exchange container's configuration: simulator on, service off, no database.
 *
 * <p>Every other test enables the service and the simulator in one process, so nothing covered the
 * mode Compose actually deploys — and it was broken, because the properties were registered only by
 * the service's own configuration.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "marketdata.consumption.enabled=false",
                "marketdata.distribution.enabled=false",
                "marketdata.simulator.enabled=true"})
class ExchangeOnlyStartupTest {

    @Autowired
    ApplicationContext context;

    @Autowired
    SimulatedExchangeHandler exchange;

    @Test
    @DisplayName("the exchange starts with both services disabled and no database")
    void startsWithoutTheService() {
        assertThat(exchange).isNotNull();
        assertThat(context.getBeanNamesForType(QuoteConsumer.class))
                .as("the exchange must not consume")
                .isEmpty();
        assertThat(context.getBeanNamesForType(javax.sql.DataSource.class))
                .as("the exchange must not need a database")
                .isEmpty();
    }
}
