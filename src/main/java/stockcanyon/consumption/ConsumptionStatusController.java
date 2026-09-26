package stockcanyon.consumption;

import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The consumption service's state: leadership, queue, sequence holes, checkpoint.
 *
 * <p>Served by the consumption service, from its own database and memory. On a standby instance
 * {@code feed.leader} is false and the counters cover only what it did while it led.
 */
@RestController
@RequestMapping("/api/v1/marketdata")
@ConditionalOnProperty(prefix = "marketdata.consumption", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class ConsumptionStatusController {

    private final QuoteConsumer consumer;

    public ConsumptionStatusController(QuoteConsumer consumer) {
        this.consumer = consumer;
    }

    /**
     * {@code feed.quotesMissing} is the field that answers the no-gaps requirement. From memory
     * only: no query, so polling it costs the writer nothing.
     */
    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of("feed", consumer.status());
    }
}
