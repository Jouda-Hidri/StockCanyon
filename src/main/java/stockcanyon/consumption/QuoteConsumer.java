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
 * Drives consumption: socket to buffer to database, and the checkpoint that ties them together.
 *
 * <p>The guarantee this class exists for is narrow and worth stating exactly. Quotes, the current
 * top of book, and the checkpoint are written <em>in one transaction</em>. Either the batch and the
 * position describing it are both durable, or neither is. Everything else follows:
 *
 * <ul>
 *   <li>Crash between writing quotes and saving the checkpoint: impossible, there is no between.
 *   <li>Crash after the commit: the checkpoint names exactly what survived, so the reconnect
 *       resumes from there and misses nothing.
 *   <li>Reconnect mid-stream: the exchange rewinds to the checkpoint's instant, which necessarily
 *       re-delivers quotes already stored, and the primary key discards them.
 * </ul>
 *
 * <p>The cost is duplicate work on every recovery, and that is deliberate. Committing the
 * checkpoint separately, or ahead of the data, would trade a duplicate the database removes for
 * free against a gap nothing can reconstruct.
 */
public class QuoteConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(QuoteConsumer.class);

    /** How long shutdown waits for buffered quotes to be written before giving up on them. */
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

        // A supplier, not a value: the checkpoint is re-read on every connection attempt, so a
        // reconnect resumes from what is on disk now rather than from where this process began.
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
        // Stop the socket first, so the buffer holds a fixed amount rather than being refilled
        // while it is drained.
        exchange.stop();
        running = false;
        if (writer != null) {
            try {
                // Deliberately not interrupted here. The loop notices `running` on its next poll
                // and then flushes what is left — and that final flush needs a database
                // connection, which the pool refuses to hand to a thread already carrying an
                // interrupt. Interrupting first is how a clean shutdown silently drops a buffer.
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

    /** Started after the web server and stopped before it. */
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
                // The batch is lost, but the checkpoint was not advanced past it, so the next
                // reconnect replays the same window and the data comes back. Backing off avoids
                // spinning against a database that is down.
                log.error("Write failed; the checkpoint was not advanced so these quotes will be "
                        + "replayed: {}", e.toString());
                sleepQuietly(Duration.ofSeconds(1));
            }
        }
        // Clear any pending interrupt before the last flush: the connection pool will not serve an
        // interrupted thread, and this flush is what saves the buffered quotes.
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
            // Last, and in the same transaction: the position is only true once the data above is.
            checkpoints.save(highWater.eventTime(), highWater.sequence(), now);
        });

        consumed.addAndGet(unique.size());
        lastQuoteAt = highWater.eventTime();
        lastLag = highWater.ingestionLag();
    }

    /**
     * Watches the exchange's sequence for holes.
     *
     * <p>This is the only thing that can actually demonstrate the no-gaps requirement. A healthy
     * socket, a successful reconnect and a climbing quote count are all perfectly consistent with
     * having missed a thousand messages; a counter that goes 41, 42, 44 is not.
     */
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
     * The newest quote in the batch, which is what the checkpoint must name.
     *
     * <p>Computed rather than taken as the last element, so that a batch delivered slightly out of
     * order cannot set the checkpoint from the wrong quote and skip the other on resume.
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
     *     The number that answers the requirement: anything but zero means the history has holes.
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
