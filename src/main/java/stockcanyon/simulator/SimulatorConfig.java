package stockcanyon.simulator;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Publishes the simulated exchange at {@code /exchange/quotes}.
 *
 * <p>This package is <b>not part of the service</b>. It stands in for the Stock Exchange, which
 * would otherwise be a third party we were given the address of. It exists for two reasons: no
 * public market data feed implements the {@code checkpoint_timestamp} replay the requirements
 * describe, and no real exchange will drop your connection on request — without which the recovery
 * path can only be asserted about, never demonstrated.
 *
 * <p>Enabled separately from the service itself, so one process can play the exchange while another
 * consumes it. That is how the Compose deployment runs them: one image, two containers, different
 * properties.
 */
@Configuration
@EnableWebSocket
@ConditionalOnProperty(prefix = "marketdata.simulator", name = "enabled", havingValue = "true")
public class SimulatorConfig implements WebSocketConfigurer {

    private final SimulatedExchangeHandler handler;

    public SimulatorConfig(SimulatedExchangeHandler handler) {
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/exchange/quotes").setAllowedOrigins("*");
    }
}
