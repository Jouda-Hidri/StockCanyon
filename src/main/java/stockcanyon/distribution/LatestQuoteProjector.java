package stockcanyon.distribution;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;

/**
 * Keeps Redis in step with the topic the ingestion service's outbox publishes to.
 *
 * <p><b>Redis, not Kafka, holds the position.</b> Each batch is applied together with the next
 * offset per partition in one MULTI/EXEC ({@link LatestQuoteStore}). On assignment the consumer
 * seeks to the offset Redis holds, or to the start of the compacted topic if it holds none. Before
 * each batch it checks that Redis still holds the offset this consumer last wrote; if not, Redis lost
 * writes — an asynchronous-replication failover, or a wipe — and the partition is replayed from what
 * Redis does hold. Kafka's committed offsets still advance, for lag monitoring only.
 *
 * <p>Replays and redeliveries are harmless: the store accepts a quote only if it is newer than the
 * one it holds. A batch that fails to apply (Redis down) is retried with backoff by the container's
 * error handler, indefinitely. A record that cannot be parsed is skipped and counted.
 */
public class LatestQuoteProjector extends AbstractConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(LatestQuoteProjector.class);

    private final LatestQuoteStore store;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final Counter applied;
    private final Counter stale;
    private final Counter invalid;
    private final Counter rewinds;
    private final Timer staleness;

    /** Per partition, the next offset this consumer last recorded in Redis. */
    private final Map<String, Long> lastWritten = new ConcurrentHashMap<>();

    public LatestQuoteProjector(LatestQuoteStore store, ObjectMapper mapper, Clock clock, MeterRegistry meters) {
        this.store = store;
        this.mapper = mapper;
        this.clock = clock;
        this.applied = Counter.builder("marketdata.projection.events").tag("outcome", "applied")
                .description("Latest-quote events that moved an instrument forward in Redis").register(meters);
        this.stale = Counter.builder("marketdata.projection.events").tag("outcome", "stale")
                .description("Events older than what Redis holds: replays and redeliveries").register(meters);
        this.invalid = Counter.builder("marketdata.projection.events").tag("outcome", "invalid")
                .description("Events that could not be parsed and were skipped").register(meters);
        this.rewinds = Counter.builder("marketdata.projection.rewinds")
                .description("Partitions replayed because Redis lost writes (failover or wipe)").register(meters);
        this.staleness = Timer.builder("marketdata.projection.staleness")
                .description("Exchange event time to the moment the API can serve it: end-to-end freshness")
                .publishPercentiles(0.5, 0.99)
                .register(meters);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        Map<String, Long> stored;
        try {
            stored = store.storedOffsets();
        } catch (RuntimeException e) {
            // Rebuilding from the start is always correct, only slower.
            log.warn("Could not read positions from Redis, rebuilding from the start: {}", e.toString());
            stored = Map.of();
        }
        for (TopicPartition partition : assignments.keySet()) {
            String key = key(partition);
            Long next = stored.get(key);
            if (next == null) {
                callback.seekToBeginning(partition.topic(), partition.partition());
                lastWritten.remove(key);
            } else {
                callback.seek(partition.topic(), partition.partition(), next);
                lastWritten.put(key, next);
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        super.onPartitionsRevoked(partitions);
        partitions.forEach(partition -> lastWritten.remove(key(partition)));
    }

    @KafkaListener(
            id = "latest-quote-projector",
            // Otherwise the listener id doubles as the consumer group, and spring.kafka.consumer.group-id
            // would be silently ignored.
            idIsGroup = false,
            clientIdPrefix = "latest-quote-projector",
            topics = "${marketdata.distribution.topic}",
            batch = "true")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> byPartition = new LinkedHashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            byPartition.computeIfAbsent(new TopicPartition(record.topic(), record.partition()),
                    p -> new ArrayList<>()).add(record);
        }

        Map<String, Long> stored = store.storedOffsets();
        List<LatestQuoteEvent> events = new ArrayList<>(records.size());
        Map<String, Long> nextOffsets = new HashMap<>();
        for (var entry : byPartition.entrySet()) {
            TopicPartition partition = entry.getKey();
            String key = key(partition);
            Long expected = lastWritten.get(key);
            if (expected != null && !Objects.equals(stored.get(key), expected)) {
                rewind(partition, expected, stored.get(key));
                continue;
            }
            long next = -1;
            for (ConsumerRecord<String, String> record : entry.getValue()) {
                LatestQuoteEvent event = parse(record);
                if (event != null) {
                    events.add(event);
                }
                next = Math.max(next, record.offset() + 1);
            }
            nextOffsets.put(key, next);
        }

        int moved = store.apply(events, nextOffsets);
        lastWritten.putAll(nextOffsets);
        applied.increment(moved);
        stale.increment(events.size() - moved);

        var now = clock.instant();
        for (LatestQuoteEvent event : events) {
            staleness.record(Duration.between(event.eventTime(), now));
        }
    }

    /**
     * Redis no longer holds what this consumer wrote to it. Its records in this batch are dropped,
     * and the partition is re-read from the position Redis does hold — or from the start of the
     * compacted topic if it holds none. The seek takes effect when this listener returns.
     */
    private void rewind(TopicPartition partition, long expected, Long stored) {
        rewinds.increment();
        log.error("Redis lost writes for {}: expected position {}, found {}; replaying from there",
                partition, expected, stored == null ? "none" : stored);
        ConsumerSeekCallback callback = getSeekCallbackFor(partition);
        if (stored == null) {
            callback.seekToBeginning(partition.topic(), partition.partition());
            lastWritten.remove(key(partition));
        } else {
            callback.seek(partition.topic(), partition.partition(), stored);
            lastWritten.put(key(partition), stored);
        }
    }

    /**
     * The Outbox Event Router emits the payload column as the message value: a JSON object, or,
     * with other connector settings, a JSON string holding one. Bound directly from the text so
     * prices keep their exact digits and scale; a tree would read them as doubles.
     */
    private LatestQuoteEvent parse(ConsumerRecord<String, String> record) {
        if (record.value() == null) {
            return null;
        }
        try {
            String json = record.value().trim();
            if (json.startsWith("\"")) {
                json = mapper.readValue(json, String.class);
            }
            LatestQuoteEvent event = mapper.readValue(json, LatestQuoteEvent.class);
            event.toQuote();  // validates the ISIN, currency, bounds and event time
            return event;
        } catch (Exception e) {
            invalid.increment();
            log.error("Skipping unreadable event at {}-{}@{} (key {}): {}", record.topic(), record.partition(),
                    record.offset(), record.key(), e.toString());
            return null;
        }
    }

    private static String key(TopicPartition partition) {
        return LatestQuoteStore.partitionKey(partition.topic(), partition.partition());
    }
}
