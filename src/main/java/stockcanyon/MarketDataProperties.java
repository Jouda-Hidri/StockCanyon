package stockcanyon;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the market data service.
 *
 * <p>The whole feature is off unless {@code marketdata.enabled} is set. It is the only module here
 * that needs a database, and defaulting it on would mean the rest of the application — none of
 * which has ever needed persistence — could no longer start without a PostgreSQL to connect to.
 */
@ConfigurationProperties(prefix = "marketdata")
public class MarketDataProperties {

    /** Master switch. Off by default; nothing below is read unless this is true. */
    private boolean enabled = false;

    /** The Stock Exchange feed. The {@code checkpoint_timestamp} parameter is appended by the client. */
    private String exchangeUrl = "ws://localhost:8099/exchange/quotes";

    private final DataSourceSettings datasource = new DataSourceSettings();
    private final ConsumptionSettings consumption = new ConsumptionSettings();
    private final SimulatorSettings simulator = new SimulatorSettings();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getExchangeUrl() {
        return exchangeUrl;
    }

    public void setExchangeUrl(String exchangeUrl) {
        this.exchangeUrl = exchangeUrl;
    }

    public DataSourceSettings getDatasource() {
        return datasource;
    }

    public ConsumptionSettings getConsumption() {
        return consumption;
    }

    public SimulatorSettings getSimulator() {
        return simulator;
    }

    /**
     * A dedicated connection pool rather than the application's.
     *
     * <p>There is no application one to share: nothing else in Bankster persists anything.
     */
    public static class DataSourceSettings {

        private String url = "jdbc:postgresql://localhost:5432/marketdata";
        private String username = "marketdata";
        private String password = "marketdata";

        /**
         * Small on purpose. The write path is a single thread, so extra connections buy nothing
         * there, and the read path is one indexed lookup per request.
         */
        private int maxPoolSize = 8;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public int getMaxPoolSize() {
            return maxPoolSize;
        }

        public void setMaxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
        }
    }

    public static class ConsumptionSettings {

        /** Whether this instance consumes the feed. Off on read-only replicas. */
        private boolean enabled = true;

        /**
         * How many quotes may wait to be written before the feed is throttled.
         *
         * <p>At a few hundred messages a second this is several seconds of slack — enough to ride
         * out a slow commit, short of enough to hide a database that has genuinely stopped.
         */
        private int bufferCapacity = 50_000;

        /** Upper bound on one transaction, so a burst cannot build an unboundedly large commit. */
        private int maxBatchSize = 2_000;

        /**
         * How long a partial batch waits for company.
         *
         * <p>The latency-versus-throughput dial: it bounds how stale the latest quote can be, while
         * also setting how many quotes a single commit gets to amortise.
         */
        private Duration flushInterval = Duration.ofMillis(200);

        private Duration initialBackoff = Duration.ofMillis(500);
        private Duration maxBackoff = Duration.ofSeconds(30);

        /**
         * Silence after which the socket is presumed dead.
         *
         * <p>Must exceed the exchange's heartbeat interval, or a quiet market is mistaken for a
         * broken connection and reconnected every time trading slows down.
         */
        private Duration stallTimeout = Duration.ofSeconds(15);

        /**
         * How long a connection must hold before the backoff resets.
         *
         * <p>Without it, an exchange that accepts a connection and immediately drops it produces an
         * unthrottled reconnect loop, because every attempt looks like a fresh success.
         */
        private Duration stableAfter = Duration.ofSeconds(30);

        private Duration connectTimeout = Duration.ofSeconds(10);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getBufferCapacity() {
            return bufferCapacity;
        }

        public void setBufferCapacity(int bufferCapacity) {
            this.bufferCapacity = bufferCapacity;
        }

        public int getMaxBatchSize() {
            return maxBatchSize;
        }

        public void setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }

        public Duration getFlushInterval() {
            return flushInterval;
        }

        public void setFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
        }

        public Duration getInitialBackoff() {
            return initialBackoff;
        }

        public void setInitialBackoff(Duration initialBackoff) {
            this.initialBackoff = initialBackoff;
        }

        public Duration getMaxBackoff() {
            return maxBackoff;
        }

        public void setMaxBackoff(Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
        }

        public Duration getStallTimeout() {
            return stallTimeout;
        }

        public void setStallTimeout(Duration stallTimeout) {
            this.stallTimeout = stallTimeout;
        }

        public Duration getStableAfter() {
            return stableAfter;
        }

        public void setStableAfter(Duration stableAfter) {
            this.stableAfter = stableAfter;
        }

        public Duration getConnectTimeout() {
            return connectTimeout;
        }

        public void setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
        }
    }

    /** The stand-in exchange. Not part of the service; see {@code simulator/SimulatorConfig}. */
    public static class SimulatorSettings {

        private boolean enabled = false;

        /** Aggregate rate across all instruments. */
        private int quotesPerSecond = 200;

        /**
         * Size of the replay log, which bounds how long an outage can be and still be recovered
         * from. At 200 quotes a second this is roughly forty minutes.
         */
        private int retainedQuotes = 500_000;

        /**
         * Zipf exponent for per-instrument arrival rates.
         *
         * <p>Zero spreads quotes evenly and tests nothing interesting. At 1.1 the busiest
         * instrument receives about a hundred times more than the quietest, which is the uneven
         * distribution the requirements call out.
         */
        private double skew = 1.1;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getQuotesPerSecond() {
            return quotesPerSecond;
        }

        public void setQuotesPerSecond(int quotesPerSecond) {
            this.quotesPerSecond = quotesPerSecond;
        }

        public int getRetainedQuotes() {
            return retainedQuotes;
        }

        public void setRetainedQuotes(int retainedQuotes) {
            this.retainedQuotes = retainedQuotes;
        }

        public double getSkew() {
            return skew;
        }

        public void setSkew(double skew) {
            this.skew = skew;
        }
    }
}
