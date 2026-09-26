package stockcanyon.simulator;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Publishes the simulated exchange at {@code /exchange/quotes}.
 *
 * <p><b>Not part of the service.</b> It stands in for the Stock Exchange, whose endpoint was not
 * provided: no public feed offers {@code checkpoint_timestamp} replay, and no real exchange drops
 * your connection on request — without which recovery could not be demonstrated.
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
