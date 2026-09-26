package stockcanyon;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings, bound from {@code marketdata.*}.
 *
 * <p>One jar, three roles, each switched on by its own {@code enabled} flag: the consumption
 * service (feed to PostgreSQL), the distribution service (Kafka to Redis to the API), and the
 * simulated exchange. A deployment runs one role per process; a local run may run all three.
 */
@ConfigurationProperties(prefix = "marketdata")
public record MarketDataProperties(
        String exchangeUrl,
        Database database,
        Consumption consumption,
        Leadership leadership,
        Distribution distribution,
        Simulator simulator) {

    public MarketDataProperties {
        if (database == null) {
            database = new Database(null, null, null);
        }
        if (consumption == null) {
            consumption = new Consumption(null, 0, null, 0, 0, 0, null, null, null, null, null, null, null, 0, null, null);
        }
        if (leadership == null) {
            leadership = new Leadership(null, null);
        }
        if (distribution == null) {
            distribution = new Distribution(null, null);
        }
        if (simulator == null) {
            simulator = new Simulator(false, 0, 0, 0);
        }
    }

    public record Database(String url, String username, String password) {}

    /**
     * @param enabled runs the consumption service: stands for election and, when elected, ingests
     * @param maxBatchSize bounds one transaction
     * @param flushInterval latency versus throughput: how stale the latest quote may be, and how
     *     much work one commit amortises
     * @param queueCapacity hard bound on quotes held in memory between the socket and the writer
     * @param queueHighWatermark depth at which the socket is closed, so the exchange holds the
     *     backlog instead of the heap. Below capacity, to leave room for frames already in flight.
     * @param queueLowWatermark depth the writer must drain to before the socket may reopen. The
     *     distance from the high mark is hysteresis: without it the socket would flap open and shut
     *     on every batch.
     * @param reconnectInitialDelay first backoff step after a failed connection
     * @param reconnectMaxDelay backoff cap
     * @param stallTimeout silence after which the socket is presumed dead. Must exceed the
     *     exchange's heartbeat interval, or a quiet market reads as a broken connection.
     * @param writeRetryInitialDelay first backoff step after a failed transaction
     * @param writeRetryMaxDelay backoff cap for the write
     * @param gapBackfillAfter how long a sequence hole may stay open before a replay is forced. Long
     *     enough for a merely reordered message to arrive on its own.
     * @param shutdownDrainTimeout how long shutdown keeps writing what is queued before giving up and
     *     leaving it to be replayed
     * @param gapMaxBackfills replays attempted for one hole before it is written off as lost
     * @param requireCdcSlot write nothing until Debezium's replication slot exists: an outbox event
     *     committed before the slot is created never reaches the WAL reader, so it is never published
     * @param cdcSlot that slot's name
     */
    public record Consumption(
            Boolean enabled,
            int maxBatchSize,
            Duration flushInterval,
            int queueCapacity,
            int queueHighWatermark,
            int queueLowWatermark,
            Duration reconnectInitialDelay,
            Duration reconnectMaxDelay,
            Duration stallTimeout,
            Duration writeRetryInitialDelay,
            Duration writeRetryMaxDelay,
            Duration gapBackfillAfter,
            Duration shutdownDrainTimeout,
            int gapMaxBackfills,
            Boolean requireCdcSlot,
            String cdcSlot) {

        public Consumption {
            if (enabled == null) {
                enabled = true;
            }
            if (maxBatchSize <= 0) {
                maxBatchSize = 2_000;
            }
            if (flushInterval == null) {
                flushInterval = Duration.ofMillis(200);
            }
            if (queueCapacity <= 0) {
                queueCapacity = 50_000;
            }
            if (queueHighWatermark <= 0) {
                queueHighWatermark = queueCapacity * 8 / 10;
            }
            if (queueLowWatermark <= 0) {
                queueLowWatermark = queueCapacity * 2 / 10;
            }
            if (!(queueLowWatermark < queueHighWatermark && queueHighWatermark < queueCapacity)) {
                throw new IllegalArgumentException("need low < high < capacity, got "
                        + queueLowWatermark + " / " + queueHighWatermark + " / " + queueCapacity);
            }
            if (reconnectInitialDelay == null) {
                reconnectInitialDelay = Duration.ofMillis(500);
            }
            if (reconnectMaxDelay == null) {
                reconnectMaxDelay = Duration.ofSeconds(30);
            }
            if (stallTimeout == null) {
                stallTimeout = Duration.ofSeconds(15);
            }
            if (writeRetryInitialDelay == null) {
                writeRetryInitialDelay = Duration.ofMillis(100);
            }
            if (writeRetryMaxDelay == null) {
                writeRetryMaxDelay = Duration.ofSeconds(10);
            }
            if (gapBackfillAfter == null) {
                gapBackfillAfter = Duration.ofSeconds(2);
            }
            if (shutdownDrainTimeout == null) {
                shutdownDrainTimeout = Duration.ofSeconds(10);
            }
            if (gapMaxBackfills <= 0) {
                gapMaxBackfills = 3;
            }
            if (requireCdcSlot == null) {
                requireCdcSlot = true;
            }
            if (cdcSlot == null) {
                cdcSlot = "marketdata_outbox";
            }
        }
    }

    /**
     * @param lockTtl how long a silent leader keeps the lock. Failover takes about this long; it must
     *     also exceed the clock skew between instances, since the lock row is stamped by each
     *     instance's own clock.
     * @param heartbeat how often the leader renews. A third of the TTL tolerates two missed renewals.
     */
    public record Leadership(Duration lockTtl, Duration heartbeat) {

        public Leadership {
            if (lockTtl == null) {
                lockTtl = Duration.ofSeconds(10);
            }
            if (heartbeat == null) {
                heartbeat = lockTtl.dividedBy(3);
            }
        }
    }

    /**
     * @param enabled runs the distribution service: projects the topic into Redis, serves the API
     * @param topic the compacted topic the outbox connector publishes latest quotes to
     */
    public record Distribution(Boolean enabled, String topic) {

        public Distribution {
            if (enabled == null) {
                enabled = true;
            }
            if (topic == null) {
                topic = "marketdata.latest-quote";
            }
        }
    }

    /**
     * @param retainedQuotes replay window; an outage longer than this cannot be recovered
     * @param skew Zipf exponent. At 1.1 the busiest instrument gets ~100x the quietest, which is
     *     the uneven arrival rate the brief calls out.
     */
    public record Simulator(boolean enabled, int quotesPerSecond, int retainedQuotes, double skew) {

        public Simulator {
            if (quotesPerSecond <= 0) {
                quotesPerSecond = 200;
            }
            if (retainedQuotes <= 0) {
                retainedQuotes = 500_000;
            }
            if (skew <= 0) {
                skew = 1.1;
            }
        }
    }
}
