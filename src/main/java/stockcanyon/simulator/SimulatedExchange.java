package stockcanyon.simulator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import stockcanyon.MarketDataProperties;

/**
 * Publishes a synthetic quote stream into the {@link QuoteLog}.
 *
 * <p>Zipf-weighted rather than uniform, so arrival rates are genuinely lopsided — the busiest
 * instrument gets ~100x the quietest. A uniform feed exercises none of the machinery that matters.
 */
@Component
@ConditionalOnProperty(prefix = "marketdata.simulator", name = "enabled", havingValue = "true")
public class SimulatedExchange {

    private static final Logger log = LoggerFactory.getLogger(SimulatedExchange.class);
    private static final int TICKS_PER_SECOND = 20;

    private record Instrument(String isin, BigDecimal openingPrice) {}

    /** Real ISINs, so payloads are realistic end to end. */
    private static final List<Instrument> INSTRUMENTS = List.of(
            new Instrument("US0378331005", new BigDecimal("232.40")),   // Apple
            new Instrument("US5949181045", new BigDecimal("430.15")),   // Microsoft
            new Instrument("US67066G1040", new BigDecimal("122.80")),   // NVIDIA
            new Instrument("US0231351067", new BigDecimal("186.50")),   // Amazon
            new Instrument("US02079K3059", new BigDecimal("164.20")),   // Alphabet
            new Instrument("US30303M1027", new BigDecimal("512.60")),   // Meta
            new Instrument("US88160R1014", new BigDecimal("248.90")),   // Tesla
            new Instrument("US46625H1005", new BigDecimal("214.35")),   // JPMorgan
            new Instrument("US4781601046", new BigDecimal("158.70")),   // Johnson & Johnson
            new Instrument("US92826C8394", new BigDecimal("281.45")));  // Visa

    private final QuoteLog quoteLog;
    private final MarketDataProperties.Simulator settings;
    private final Clock clock;
    private final double[] cumulativeWeights;
    private final BigDecimal[] prices;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            runnable -> Thread.ofPlatform().name("marketdata-simulator").unstarted(runnable));

    public SimulatedExchange(QuoteLog quoteLog, MarketDataProperties properties, Clock clock) {
        this.quoteLog = quoteLog;
        this.settings = properties.simulator();
        this.clock = clock;
        this.prices = INSTRUMENTS.stream().map(Instrument::openingPrice).toArray(BigDecimal[]::new);
        this.cumulativeWeights = zipfWeights(INSTRUMENTS.size(), settings.skew());
    }

    @PostConstruct
    void start() {
        int perTick = Math.max(1, settings.quotesPerSecond() / TICKS_PER_SECOND);
        scheduler.scheduleAtFixedRate(() -> {
            try {
                emit(perTick);
            } catch (RuntimeException e) {
                log.error("Simulated exchange tick failed", e);
            }
        }, 0, 1000 / TICKS_PER_SECOND, TimeUnit.MILLISECONDS);
        log.info("Simulated exchange publishing ~{} quotes/s across {} instruments (Zipf skew {})",
                settings.quotesPerSecond(), INSTRUMENTS.size(), settings.skew());
    }

    @PreDestroy
    void stop() {
        scheduler.shutdownNow();
    }

    private void emit(int count) {
        for (int i = 0; i < count; i++) {
            int index = pickInstrument();
            BigDecimal mid = nextPrice(index);
            BigDecimal halfSpread = mid.multiply(new BigDecimal("0.0002")).setScale(4, RoundingMode.HALF_UP);
            String isin = INSTRUMENTS.get(index).isin();
            quoteLog.append(sequence -> new ExchangeMessage(
                    isin,
                    sequence,
                    mid.subtract(halfSpread).setScale(4, RoundingMode.HALF_UP),
                    mid.add(halfSpread).setScale(4, RoundingMode.HALF_UP),
                    roundLot(),
                    roundLot(),
                    "USD",
                    clock.instant()));
        }
    }

    /** Geometric random walk, so prices stay positive. */
    private BigDecimal nextPrice(int index) {
        double drift = ThreadLocalRandom.current().nextGaussian() * 0.0004;
        BigDecimal next = prices[index]
                .multiply(BigDecimal.valueOf(1 + drift))
                .setScale(4, RoundingMode.HALF_UP);
        if (next.signum() <= 0) {
            next = INSTRUMENTS.get(index).openingPrice();
        }
        prices[index] = next;
        return next;
    }

    private static BigDecimal roundLot() {
        return BigDecimal.valueOf(ThreadLocalRandom.current().nextInt(1, 40) * 25L);
    }

    private int pickInstrument() {
        double target = ThreadLocalRandom.current().nextDouble()
                * cumulativeWeights[cumulativeWeights.length - 1];
        int position = Arrays.binarySearch(cumulativeWeights, target);
        return position >= 0 ? position : Math.min(-position - 1, cumulativeWeights.length - 1);
    }

    /** Cumulative Zipf weights: rank k gets weight proportional to 1/k^skew. */
    private static double[] zipfWeights(int size, double skew) {
        double[] cumulative = new double[size];
        double running = 0;
        for (int k = 0; k < size; k++) {
            running += 1.0 / Math.pow(k + 1, skew);
            cumulative[k] = running;
        }
        return cumulative;
    }
}
