package stockcanyon.consumption;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.transaction.support.TransactionTemplate;

import stockcanyon.Checkpoint;
import stockcanyon.Quote;
import stockcanyon.storage.CheckpointRepository;
import stockcanyon.storage.QuoteRepository;

/**
 * Drives consumption: socket to buffer to database.
 *
 * <p>Quotes, the latest-quote projection and the checkpoint are written <b>in one transaction</b>,
 * so there is no window between the data being durable and the position describing it being
 * durable. A crash after the commit resumes from a checkpoint that names exactly what survived.
 *
 * <p>The cost is replayed duplicates on every recovery, which the primary key discards. That is
 * the trade: a duplicate the database removes for free, rather than a gap nothing can rebuild.
 */
public class QuoteConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(QuoteConsumer.class);

    /** How long shutdown waits for buffered quotes to be written. */
    private static final Duration SHUTDOWN_DRAIN_TIMEOUT = Duration.ofSeconds(10);

    private final ExchangeWebSocketClient exchange;
    private final QuoteBuffer buffer;
    private final QuoteRepository quotes;
    private final CheckpointRepository checkpoints;
    private final TransactionTemplate transactions;
    private final Clock clock;
    private final int maxBatchSize;
    private final Duration flushInterval;

    private final SequenceTracker sequences = new SequenceTracker();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong missing = new AtomicLong();

    private volatile boolean running;
    private volatile Instant lastQuoteAt;
    private volatile Duration lastLag = Duration.ZERO;
    private Thread writer;

    public QuoteConsumer(
            ExchangeWebSocketClient exchange,
            QuoteBuffer buffer,
            QuoteRepository quotes,
            CheckpointRepository checkpoints,
            TransactionTemplate transactions,
            Clock clock,
            int maxBatchSize,
            Duration flushInterval) {
        this.exchange = exchange;
        this.buffer = buffer;
        this.quotes = quotes;
        this.checkpoints = checkpoints;
        this.transactions = transactions;
        this.clock = clock;
        this.maxBatchSize = maxBatchSize;
        this.flushInterval = flushInterval;
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public void start() {
        if (running) {
            return;
        }
        running = true;
        writer = new Thread(this::writeLoop, "marketdata-writer");
        writer.start();

        // A supplier, not a value: re-read on every attempt, so a reconnect resumes from disk.
        exchange.start(buffer::put, checkpoints::load);

        Checkpoint from = checkpoints.load();
        log.info("Market data consumption started (resume from {})",
                from.isPresent() ? from.eventTime() : "now");
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        log.info("Stopping market data consumption");
        // Socket first, so the buffer is not refilled while it drains.
        exchange.stop();
        running = false;
        if (writer != null) {
            try {
                // Not interrupted: the final flush needs a connection, and the pool refuses one to
                // an interrupted thread. Interrupting first silently drops the buffer.
                writer.join(SHUTDOWN_DRAIN_TIMEOUT.toMillis());
                if (writer.isAlive()) {
                    log.warn("Writer did not finish within {}; abandoning the buffered quotes",
                            SHUTDOWN_DRAIN_TIMEOUT);
                    writer.interrupt();
                    writer.join(Duration.ofSeconds(2).toMillis());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("Consumption stopped after {} quote(s), {} duplicate(s), {} missing",
                consumed.get(), duplicates.get(), missing.get());
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Started after the web server, stopped before it. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    // ------------------------------------------------------------------ writing

    private void writeLoop() {
        while (running) {
            try {
                List<Quote> batch = buffer.drain(maxBatchSize, flushInterval);
                if (!batch.isEmpty()) {
                    writeBatch(batch);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                // The checkpoint did not advance, so the next reconnect replays this window.
                log.error("Write failed; the checkpoint was not advanced so these quotes will be "
                        + "replayed: {}", e.toString());
                sleepQuietly(Duration.ofSeconds(1));
            }
        }
        // Clear any interrupt: the pool will not serve an interrupted thread, and this flush is
        // what saves the buffered quotes.
        Thread.interrupted();
        drainRemaining();
    }

    private void drainRemaining() {
        try {
            List<Quote> tail;
            while (!(tail = buffer.drain(maxBatchSize, Duration.ZERO)).isEmpty()) {
                writeBatch(tail);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.error("Could not flush the remaining buffer on shutdown: {}", e.toString());
        }
    }

    private void writeBatch(List<Quote> batch) {
        inspectSequences(batch);

        List<Quote> unique = QuoteBuffer.dedupeByKey(batch);
        Collection<Quote> coalesced = QuoteBuffer.coalesceLatest(unique);
        Quote highWater = highWaterMark(unique);
        Instant now = clock.instant();

        transactions.executeWithoutResult(status -> {
            quotes.insertHistory(unique);
            quotes.upsertLatest(coalesced, now);
            // Same transaction: the position is only true once the data above is.
            checkpoints.save(highWater.eventTime(), highWater.sequence(), now);
        });

        consumed.addAndGet(unique.size());
        lastQuoteAt = highWater.eventTime();
        lastLag = highWater.ingestionLag();
    }

    /** Counts duplicates and gaps. See {@link SequenceTracker}. */
    private void inspectSequences(List<Quote> batch) {
        for (Quote quote : batch) {
            SequenceTracker.Observation observation = sequences.observe(quote.sequence());
            switch (observation.verdict()) {
                case DUPLICATE -> duplicates.incrementAndGet();
                case GAP -> {
                    missing.addAndGet(observation.missingCount());
                    log.error("Sequence gap: {}..{} ({} message(s)) never arrived",
                            observation.missingFrom(), observation.missingTo(),
                            observation.missingCount());
                }
                default -> { }
            }
        }
    }

    /**
     * Newest quote in the batch — what the checkpoint must name.
     *
     * <p>Computed rather than taken as the last element, so a batch delivered slightly out of order
     * cannot set the checkpoint from the wrong quote and skip the other on resume.
     */
    private static Quote highWaterMark(List<Quote> batch) {
        Quote highest = batch.getFirst();
        for (Quote quote : batch) {
            if (quote.isNewerThan(highest)) {
                highest = quote;
            }
        }
        return highest;
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------ status

    /**
     * @param quotesMissing sequence numbers the exchange issued and this service never received.
     *     Anything but zero means the stored history has holes.
     */
    public record ConsumptionStatus(
            boolean running,
            boolean connected,
            Instant lastQuoteAt,
            long lagMillis,
            Checkpoint checkpoint,
            int bufferDepth,
            long quotesConsumed,
            long duplicatesDiscarded,
            long quotesMissing) {}

    public ConsumptionStatus status() {
        return new ConsumptionStatus(
                running,
                exchange.isConnected(),
                lastQuoteAt,
                lastLag.toMillis(),
                checkpoints.load(),
                buffer.depth(),
                consumed.get(),
                duplicates.get(),
                missing.get());
    }
}
