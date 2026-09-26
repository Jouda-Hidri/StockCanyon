package stockcanyon;

import java.time.Clock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import stockcanyon.distribution.LatestQuoteProjector;
import stockcanyon.distribution.LatestQuoteStore;

/**
 * The distribution service: the latest-quote topic, its own Redis, and the API.
 *
 * <p>It has no database connection. Its only input is the topic the consumption service's outbox
 * publishes to; its only state is Redis, which it can rebuild from that topic. Redis and Kafka
 * connections come from Spring Boot's auto-configuration ({@code spring.data.redis.*},
 * {@code spring.kafka.*}).
 */
@Configuration
@ConditionalOnProperty(prefix = "marketdata.distribution", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class DistributionConfig {

    private static final Logger log = LoggerFactory.getLogger(DistributionConfig.class);

    @Bean
    LatestQuoteStore latestQuoteStore(StringRedisTemplate redis, ObjectMapper mapper) {
        return new LatestQuoteStore(redis, mapper);
    }

    @Bean
    LatestQuoteProjector latestQuoteProjector(
            LatestQuoteStore store, ObjectMapper mapper, Clock clock, MeterRegistry meters) {
        return new LatestQuoteProjector(store, mapper, clock, meters);
    }

    /**
     * Retries a failed batch with backoff, forever. The only failure left once parsing is handled
     * in the listener is Redis being unavailable, and giving up on a batch would leave instruments
     * stale until they next traded. Offsets are not committed meanwhile, so a restart resumes here.
     */
    @Bean
    CommonErrorHandler latestQuoteErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(100, 2.0);
        backOff.setMaxInterval(10_000);
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, e) -> log.error("Giving up on {}: {}", record, e.toString()), backOff);
        handler.setLogLevel(org.springframework.kafka.KafkaException.Level.WARN);
        return handler;
    }
}
