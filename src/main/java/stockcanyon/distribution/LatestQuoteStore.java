package stockcanyon.distribution;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import stockcanyon.Isin;
import stockcanyon.Quote;

/**
 * The distribution service's datastore: one Redis hash per instrument, holding its latest quote.
 *
 * <pre>
 *   marketdata:latest:US0378331005 -> { et: 1790000000123457, seq: 21457, quote: {...json...} }
 * </pre>
 *
 * <p>A projection, not a source of truth. It is rebuilt by replaying the compacted topic from the
 * start, so losing it costs a rebuild, never data.
 *
 * <p>It also holds the consumer's position — the next offset per topic partition, in
 * {@code marketdata:offsets} — written in the same MULTI/EXEC as the prices it covers. Redis
 * replicates asynchronously, so a failover can lose acknowledged writes; because position and data
 * are lost <em>together</em>, the consumer can always resume from what Redis actually holds rather
 * than from a Kafka offset that has moved past the lost writes.
 *
 * <p>Writes are conditional, in a Lua script so the compare and the set are one atomic step:
 * a quote is stored only if it is newer, by (event time, sequence), than the one already there.
 * Kafka delivers at least once, and after a consumer crash or a rebalance it re-delivers a stretch
 * of events already applied; without the guard, the API would briefly serve those older prices.
 * With it, delivery order and duplicates stop mattering.
 */
public class LatestQuoteStore {

    static final String KEY_PREFIX = "marketdata:latest:";
    static final String OFFSETS_KEY = "marketdata:offsets";

    /**
     * KEYS[1] the instrument's hash; ARGV event time in µs, sequence, quote JSON.
     * Returns 1 if stored, 0 if the stored quote is as new or newer.
     */
    private static final String APPLY_IF_NEWER = """
            local current = redis.call('HMGET', KEYS[1], 'et', 'seq')
            if current[1] then
              local et, stored_et = tonumber(ARGV[1]), tonumber(current[1])
              if et < stored_et or (et == stored_et and tonumber(ARGV[2]) <= tonumber(current[2])) then
                return 0
              end
            end
            redis.call('HSET', KEYS[1], 'et', ARGV[1], 'seq', ARGV[2], 'quote', ARGV[3])
            return 1
            """;

    private static final byte[] SCRIPT = APPLY_IF_NEWER.getBytes(StandardCharsets.UTF_8);

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;

    public LatestQuoteStore(StringRedisTemplate redis, ObjectMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
    }

    /** Applies a batch without recording a consumer position. */
    public int apply(List<LatestQuoteEvent> events) {
        return apply(events, Map.of());
    }

    /**
     * Applies a batch and records the consumer's next offsets, atomically: one MULTI/EXEC, so Redis
     * never holds prices without the position that covers them, or the reverse.
     *
     * @param nextOffsets per partition key ({@link #partitionKey}), the next offset to consume
     * @return how many events moved an instrument forward; the rest were stale or duplicate
     */
    public int apply(List<LatestQuoteEvent> events, Map<String, Long> nextOffsets) {
        if (events.isEmpty() && nextOffsets.isEmpty()) {
            return 0;
        }
        List<Object> results = redis.execute((RedisCallback<List<Object>>) connection -> {
            connection.multi();
            for (LatestQuoteEvent event : events) {
                eval(connection, event);
            }
            if (!nextOffsets.isEmpty()) {
                Map<byte[], byte[]> offsets = new HashMap<>();
                nextOffsets.forEach((partition, offset) -> offsets.put(bytes(partition), bytes(Long.toString(offset))));
                connection.hashCommands().hMSet(bytes(OFFSETS_KEY), offsets);
            }
            return connection.exec();
        });
        int moved = 0;
        for (int i = 0; results != null && i < events.size() && i < results.size(); i++) {
            if (results.get(i) instanceof Long stored && stored == 1L) {
                moved++;
            }
        }
        return moved;
    }

    /** The consumer positions Redis holds, per partition key. Empty after a wipe. */
    public Map<String, Long> storedOffsets() {
        Map<String, Long> offsets = new HashMap<>();
        redis.<String, String>opsForHash().entries(OFFSETS_KEY)
                .forEach((partition, offset) -> offsets.put(partition, Long.parseLong(offset)));
        return offsets;
    }

    public static String partitionKey(String topic, int partition) {
        return topic + ":" + partition;
    }

    private void eval(RedisConnection connection, LatestQuoteEvent event) {
        connection.scriptingCommands().eval(SCRIPT, ReturnType.INTEGER, 1,
                key(event.isin()),
                bytes(Long.toString(event.eventTimeMicros())),
                bytes(Long.toString(event.sequence())),
                bytes(toJson(event)));
    }

    /** The latest quote for an instrument — what the API serves. */
    public Optional<Quote> findLatest(Isin isin) {
        Object json = redis.opsForHash().get(KEY_PREFIX + isin.value(), "quote");
        if (json == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(json.toString(), LatestQuoteEvent.class).toQuote());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unreadable quote stored for " + isin.value(), e);
        }
    }

    private String toJson(LatestQuoteEvent event) {
        try {
            return mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialise the quote for " + event.isin(), e);
        }
    }

    private static byte[] key(String isin) {
        return bytes(KEY_PREFIX + isin);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
