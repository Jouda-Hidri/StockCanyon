package stockcanyon;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for the market data service. */
@ConfigurationProperties(prefix = "marketdata")
public class MarketDataProperties {

    /** Master switch. */
    private boolean enabled = false;

    /** The Stock Exchange feed. {@code ?checkpoint_timestamp=} is appended by the client. */
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

    /** A dedicated pool, so the market data schema is this module's concern alone. */
    public static class DataSourceSettings {

        private String url = "jdbc:postgresql://localhost:5432/marketdata";
        private String username = "marketdata";
        private String password = "marketdata";

        /** Small: the write path is one thread, the read path one indexed lookup. */
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

        /** Whether this instance consumes. Off on read replicas. */
        private boolean enabled = true;

        /** Seconds of slack at a few hundred msg/s; short of hiding a database that has stopped. */
        private int bufferCapacity = 50_000;

        /** Bounds one transaction. */
        private int maxBatchSize = 2_000;

        /** Latency vs throughput: bounds staleness, and how much one commit amortises. */
        private Duration flushInterval = Duration.ofMillis(200);

        private Duration initialBackoff = Duration.ofMillis(500);
        private Duration maxBackoff = Duration.ofSeconds(30);

        /** Must exceed the heartbeat interval, or a quiet market reads as a dead socket. */
        private Duration stallTimeout = Duration.ofSeconds(15);

        /** Uptime before the backoff resets, so a flapping exchange is still throttled. */
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

    /** The stand-in exchange. Not part of the service. */
    public static class SimulatorSettings {

        private boolean enabled = false;

        /** Aggregate rate across all instruments. */
        private int quotesPerSecond = 200;

        /** Replay window. At 200 q/s this is roughly forty minutes. */
        private int retainedQuotes = 500_000;

        /** Zipf exponent. At 1.1 the busiest instrument gets ~100x the quietest. */
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
