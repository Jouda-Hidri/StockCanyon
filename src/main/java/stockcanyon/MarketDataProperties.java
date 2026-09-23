package stockcanyon;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Settings, bound from {@code marketdata.*}. */
@ConfigurationProperties(prefix = "marketdata")
public record MarketDataProperties(
        boolean enabled,
        String exchangeUrl,
        Database database,
        Consumption consumption,
        Simulator simulator) {

    public MarketDataProperties {
        if (database == null) {
            database = new Database(null, null, null);
        }
        if (consumption == null) {
            consumption = new Consumption(0, null, null, null);
        }
        if (simulator == null) {
            simulator = new Simulator(false, 0, 0, 0);
        }
    }

    public record Database(String url, String username, String password) {}

    /**
     * @param maxBatchSize bounds one transaction
     * @param flushInterval latency versus throughput: how stale the latest quote may be, and how
     *     much work one commit amortises
     * @param reconnectDelay fixed wait between reconnect attempts, so a refusing exchange is not
     *     hammered in a tight loop
     * @param stallTimeout silence after which the socket is presumed dead. Must exceed the
     *     exchange's heartbeat interval, or a quiet market reads as a broken connection.
     */
    public record Consumption(
            int maxBatchSize,
            Duration flushInterval,
            Duration reconnectDelay,
            Duration stallTimeout) {

        public Consumption {
            if (maxBatchSize <= 0) {
                maxBatchSize = 2_000;
            }
            if (flushInterval == null) {
                flushInterval = Duration.ofMillis(200);
            }
            if (reconnectDelay == null) {
                reconnectDelay = Duration.ofSeconds(1);
            }
            if (stallTimeout == null) {
                stallTimeout = Duration.ofSeconds(15);
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
