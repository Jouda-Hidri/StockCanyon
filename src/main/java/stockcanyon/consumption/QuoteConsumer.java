package stockcanyon.consumption;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
 * Drives consumption: socket to database.
 *
 * <p>Quotes, the latest-quote projection and the checkpoint are written <b>in one transaction</b>,
 * so there is no window between the data being durable and the position describing it being
 * durable. A crash after the commit resumes from a checkpoint that names exactly what survived.
 *
 * <p>The cost is replayed duplicates on every recovery, which the primary key discards. That is the
 * trade: a duplicate the database removes for free, rather than a gap nothing can rebuild.
 *
 * <p>Quotes accumulate and are written in batches, on the socket's own thread. While a batch is
 * being written the socket is simply not read, which throttles the exchange at the transport layer.
 * Shutdown is therefore just a final flush.
 *
 * <p>TODO (not required here): decouple with a bounded queue and a writer thread, so arrival bursts
 * are absorbed rather than stalling the socket for the duration of each write. {@code accept} would
 * {@code put} onto an {@code ArrayBlockingQueue} and a writer thread would {@code drain} it into
 * this same {@code flush}. The queue must be bounded — a full one blocks the producer, which stops
 * the socket being read and pushes back on the exchange, whereas an unbounded one absorbs the
 * backlog into the heap until the process dies and loses all of it. Worth doing when a write can no
 * longer keep up with arrivals; measure before adding it, since it buys smoothing, not correctness.
 */
public class QuoteConsumer implements SmartLifecycle, ExchangeWebSocketClient.QuoteSink {

    private static final Logger log = LoggerFactory.getLogger(QuoteConsumer.class);

    private final ExchangeWebSocketClient exchange;
    private final QuoteRepository quotes;
    private final CheckpointRepository checkpoints;
    private final TransactionTemplate transactions;
    private final Clock clock;
    private final int maxBatchSize;
    private final Duration flushInterval;

    private final SequenceTracker sequences = new SequenceTracker();
    private final List<Quote> pending = new ArrayList<>();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong missing = new AtomicLong();

    private volatile boolean running;
    private volatile Instant lastQuoteAt;
    private volatile Duration lastLag = Duration.ZERO;
    private long lastFlushNanos = System.nanoTime();

    public QuoteConsumer(
            ExchangeWebSocketClient exchange,
            QuoteRepository quotes,
            CheckpointRepository checkpoints,
            TransactionTemplate transactions,
            Clock clock,
            int maxBatchSize,
            Duration flushInterval) {
        this.exchange = exchange;
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
        // A supplier, not a value: re-read on every attempt, so a reconnect resumes from disk.
        exchange.start(this, checkpoints::load);

        Checkpoint from = checkpoints.load();
        log.info("Consumption started (resume from {})", from.isPresent() ? from.eventTime() : "now");
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        exchange.stop();
        try {
            flush();
        } catch (RuntimeException e) {
            log.error("Could not flush on shutdown; these quotes will be replayed: {}", e.toString());
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

    // ------------------------------------------------------------------ consuming

    /**
     * Accepts one quote from the socket, writing a batch when one is due.
     *
     * <p>Synchronized because a reconnect delivers on a new thread, and the batch must not be
     * shared across the two.
     */
    @Override
    public synchronized void accept(Quote quote) {
        pending.add(quote);
        boolean full = pending.size() >= maxBatchSize;
        boolean due = System.nanoTime() - lastFlushNanos >= flushInterval.toNanos();
        if (full || due) {
            flush();
        }
    }

    /** A quiet market still flushes: the exchange's heartbeat drives this. */
    @Override
    public synchronized void onIdle() {
        if (System.nanoTime() - lastFlushNanos >= flushInterval.toNanos()) {
            flush();
        }
    }

    /**
     * Writes whatever has accumulated.
     *
     * <p>A failure here leaves the checkpoint where it was, so the next reconnect replays the same
     * window and the quotes come back. The batch is dropped rather than retried in place, because
     * retrying while the socket is blocked would stall consumption behind a database that is down.
     */
    private synchronized void flush() {
        if (pending.isEmpty()) {
            lastFlushNanos = System.nanoTime();
            return;
        }
        List<Quote> batch = List.copyOf(pending);
        pending.clear();

        inspectSequences(batch);

        List<Quote> unique = QuoteBatch.dedupeByKey(batch);
        Collection<Quote> coalesced = QuoteBatch.coalesceLatest(unique);
        Quote highWater = QuoteBatch.highWaterMark(unique);
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
        // Timed from the end of the write, not the start. Measured from the start, a write slower
        // than the flush interval leaves every subsequent quote already overdue, so each one
        // flushes a batch of itself — which makes writes slower still.
        lastFlushNanos = System.nanoTime();
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
            int pendingQuotes,
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
                pendingCount(),
                consumed.get(),
                duplicates.get(),
                missing.get());
    }

    private synchronized int pendingCount() {
        return pending.size();
    }
}
