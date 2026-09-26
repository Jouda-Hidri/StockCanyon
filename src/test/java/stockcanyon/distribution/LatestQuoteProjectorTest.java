package stockcanyon.distribution;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.testcontainers.containers.GenericContainer;

import stockcanyon.Isin;

/**
 * The consumer's position lives in Redis. These are the cases where Redis loses writes it had
 * acknowledged — an asynchronous-replication failover, or a wipe — and the consumer must notice and
 * replay, instead of carrying on past the prices Redis no longer has.
 */
class LatestQuoteProjectorTest {

    private static final String TOPIC = "marketdata.latest-quote";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final String AAPL = "US0378331005";

    private static GenericContainer<?> redisContainer;
    private static LettuceConnectionFactory connections;
    private static LatestQuoteStore store;
    private static ObjectMapper mapper;

    private LatestQuoteProjector projector;
    private RecordingSeeks seeks;

    @BeforeAll
    static void start() {
        redisContainer = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);
        redisContainer.start();
        connections = new LettuceConnectionFactory(redisContainer.getHost(), redisContainer.getMappedPort(6379));
        connections.afterPropertiesSet();
        mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        store = new LatestQuoteStore(new StringRedisTemplate(connections), mapper);
    }

    @AfterAll
    static void stop() {
        connections.destroy();
        redisContainer.stop();
    }

    @BeforeEach
    void assign() {
        connections.getConnection().serverCommands().flushDb();
        projector = new LatestQuoteProjector(store, mapper, Clock.systemUTC(), new SimpleMeterRegistry());
        seeks = new RecordingSeeks();
        // What the listener container does on its consumer thread before assigning partitions.
        projector.registerSeekCallback(seeks);
        projector.onPartitionsAssigned(Map.of(P0, 0L), seeks);
    }

    @Test
    @DisplayName("with no position in Redis, a partition is read from the start")
    void emptyRedisReadsFromTheStart() {
        assertThat(seeks.calls).containsExactly("beginning:0");
    }

    @Test
    @DisplayName("each batch records the next offset with the prices")
    void positionAdvances() {
        projector.onBatch(List.of(record(0, 1, "100"), record(1, 2, "101")));

        assertThat(store.storedOffsets()).containsEntry(LatestQuoteStore.partitionKey(TOPIC, 0), 2L);
        assertThat(store.findLatest(Isin.of(AAPL)).orElseThrow().sequence()).isEqualTo(2);
    }

    @Test
    @DisplayName("Redis wiped under a running consumer: the batch is not applied and the partition replays from the start")
    void wipeTriggersReplay() {
        projector.onBatch(List.of(record(0, 1, "100"), record(1, 2, "101")));
        connections.getConnection().serverCommands().flushDb();
        seeks.calls.clear();

        projector.onBatch(List.of(record(2, 3, "102")));

        assertThat(seeks.calls).containsExactly("beginning:0");
        assertThat(store.findLatest(Isin.of(AAPL))).as("not applied past the lost writes").isEmpty();
    }

    @Test
    @DisplayName("a failover that lost the last writes: the partition replays from what Redis still holds")
    void failoverRewindTriggersReplay() {
        projector.onBatch(List.of(record(0, 1, "100")));
        projector.onBatch(List.of(record(1, 2, "101")));
        // What an asynchronous replica promoted before the second batch replicated would hold.
        new StringRedisTemplate(connections).opsForHash()
                .put(LatestQuoteStore.OFFSETS_KEY, LatestQuoteStore.partitionKey(TOPIC, 0), "1");
        seeks.calls.clear();

        projector.onBatch(List.of(record(2, 3, "102")));

        assertThat(seeks.calls).containsExactly("seek:0@1");
    }

    private ConsumerRecord<String, String> record(long offset, long sequence, String bid) {
        String json = """
                {"isin":"%s","currency":"USD","bid":%s,"ask":%s,"bidSize":100,"askSize":100,
                 "sequence":%d,"eventTime":"2026-09-26T10:00:00.%06dZ","receivedTime":"2026-09-26T10:00:01Z"}
                """.formatted(AAPL, bid, bid, sequence, sequence);
        return new ConsumerRecord<>(TOPIC, 0, offset, AAPL, json);
    }

    private static final class RecordingSeeks implements ConsumerSeekCallback {

        final List<String> calls = new ArrayList<>();

        @Override
        public void seek(String topic, int partition, long offset) {
            calls.add("seek:" + partition + "@" + offset);
        }

        @Override
        public void seekToBeginning(String topic, int partition) {
            calls.add("beginning:" + partition);
        }

        @Override
        public void seekToEnd(String topic, int partition) {
            calls.add("end:" + partition);
        }

        @Override
        public void seekRelative(String topic, int partition, long offset, boolean toCurrent) {
            calls.add("relative:" + partition);
        }

        @Override
        public void seekToTimestamp(String topic, int partition, long timestamp) {
            calls.add("timestamp:" + partition);
        }

        @Override
        public void seekToTimestamp(java.util.Collection<TopicPartition> partitions, long timestamp) {
            calls.add("timestamp");
        }

        @Override
        public void seek(String topic, int partition, Function<Long, Long> offsetComputeFunction) {
            calls.add("compute:" + partition);
        }
    }
}
