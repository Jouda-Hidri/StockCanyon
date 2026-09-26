package stockcanyon.consumption;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.transaction.support.TransactionTemplate;

import stockcanyon.Checkpoint;
import stockcanyon.MarketDataProperties;
import stockcanyon.Quote;
import stockcanyon.storage.CheckpointRepository;
import stockcanyon.storage.LeadershipRepository;
import stockcanyon.storage.QuoteRepository;

/**
 * Drives consumption while this instance leads: socket, bounded queue, writer, database.
 *
 * <pre>
 *   socket thread ──put──▶ IngestQueue (bounded) ──drain──▶ writer thread ──▶ one transaction
 *        ▲                    │ depth ≥ high: close the socket                  quote_history (COPY)
 *        │                    │ depth ≤ low:  reopen it, from the checkpoint    outbox
 *        └────── gate ◀───────┘                                                 checkpoint
 * </pre>
 *
 * <p>Quotes, their outbox events and the checkpoint are written <b>in one transaction</b>,
 * so there is no window between the data being durable and the position describing it being
 * durable. A failed transaction is retried with backoff — never dropped — while the queue absorbs
 * arrivals; if it fills, the socket closes and the exchange holds the rest.
 *
 * <p>Started and stopped by leader election ({@link IngestionLeader}), not by the application
 * lifecycle: on a standby instance nothing here runs. Each leadership starts from the stored
 * checkpoint with an empty queue and a fresh term.
 */
public class QuoteConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(QuoteConsumer.class);

    private final ExchangeWebSocketClient exchange;
    private final QuoteRepository quotes;
    private final CheckpointRepository checkpoints;
    private final LeadershipRepository leadership;
    private final TransactionTemplate transactions;
    private final Clock clock;
    private final MarketDataProperties.Consumption settings;
    private final MeterRegistry meters;
    private final String instanceId;

    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong gapsDetected = new AtomicLong();

    private final Counter duplicatesCounter;
    private final Counter gapsCounter;
    private final Counter backfilledCounter;
    private final Counter backfillRequests;
    private final Counter lostCounter;
    private final Counter resetsCounter;
    private final Counter quotesWritten;
    private final Counter rowsInserted;
    private final Counter outboxPublished;
    private final Counter writeRetries;
    private final Counter highWatermarkTrips;
    private final Timer ingestLag;

    private volatile boolean lifecycleRunning;
    private volatile Run run;

    /**
     * The exchange client is shared by successive leaderships, so only one run may own it at a
     * time. A new run waits until the previous owner has stopped it; a run stops it only if it is
     * still the owner. Without this, a run stopped mid-start-up could start the client after being
     * told to stop, and the next run's start would find it running and do nothing.
     */
    private final ReentrantLock clientLock = new ReentrantLock();
    private Run clientOwner;

    public QuoteConsumer(
            ExchangeWebSocketClient exchange,
            QuoteRepository quotes,
            CheckpointRepository checkpoints,
            LeadershipRepository leadership,
            TransactionTemplate transactions,
            Clock clock,
            MarketDataProperties.Consumption settings,
            MeterRegistry meters,
            String instanceId) {
        this.exchange = exchange;
        this.quotes = quotes;
        this.checkpoints = checkpoints;
        this.leadership = leadership;
        this.transactions = transactions;
        this.clock = clock;
        this.settings = settings;
        this.meters = meters;
        this.instanceId = instanceId;

        duplicatesCounter = Counter.builder("marketdata.sequence.duplicates")
                .description("Messages received twice, normally replay overlap").register(meters);
        gapsCounter = Counter.builder("marketdata.sequence.gaps")
                .description("Sequence holes opened").register(meters);
        backfilledCounter = Counter.builder("marketdata.sequence.backfilled")
                .description("Messages that arrived late, inside an open hole").register(meters);
        backfillRequests = Counter.builder("marketdata.gap.backfills")
                .description("Replays forced to fill an open hole").register(meters);
        lostCounter = Counter.builder("marketdata.sequence.lost")
                .description("Messages written off after replay failed to recover them").register(meters);
        resetsCounter = Counter.builder("marketdata.sequence.resets")
                .description("Times the exchange restarted its sequence numbering").register(meters);
        quotesWritten = Counter.builder("marketdata.db.write.quotes")
                .description("Quotes committed, duplicates included").register(meters);
        rowsInserted = Counter.builder("marketdata.db.write.rows.inserted")
                .description("History rows actually added; the rest were duplicates").register(meters);
        outboxPublished = Counter.builder("marketdata.outbox.published")
                .description("Events written to the outbox, one per instrument per batch")
                .register(meters);
        writeRetries = Counter.builder("marketdata.db.write.retries")
                .description("Transactions retried after a failure").register(meters);
        highWatermarkTrips = Counter.builder("marketdata.queue.high.watermark.trips")
                .description("Times the queue filled to its high watermark and the socket was closed")
                .register(meters);
        ingestLag = Timer.builder("marketdata.ingest.lag")
                .description("Exchange event time to receipt, newest quote per batch")
                .publishPercentiles(0.5, 0.99)
                .register(meters);

        Gauge.builder("marketdata.leader", () -> run != null ? 1 : 0)
                .description("1 on the instance currently ingesting").register(meters);
        Gauge.builder("marketdata.leader.term", () -> run != null ? run.term : 0)
                .description("Fencing term held by this instance").register(meters);
        Gauge.builder("marketdata.queue.depth", () -> run != null ? run.queue.depth() : 0)
                .description("Quotes received and not yet written").register(meters);
        Gauge.builder("marketdata.queue.capacity", settings, s -> s.queueCapacity())
                .register(meters);
        Gauge.builder("marketdata.queue.high.watermark", settings, s -> s.queueHighWatermark())
                .register(meters);
        Gauge.builder("marketdata.queue.paused", () -> run != null && run.queue.isPaused() ? 1 : 0)
                .description("1 while the socket is closed for back-pressure").register(meters);
        Gauge.builder("marketdata.sequence.outstanding", () -> run != null ? run.outstanding : 0)
                .description("Sequences missing right now, awaiting backfill").register(meters);
        Gauge.builder("marketdata.checkpoint.lag", () -> run != null ? run.checkpointLagSeconds() : 0)
                .description("Seconds between now and the committed checkpoint's event time")
                .baseUnit("seconds").register(meters);
        Gauge.builder("marketdata.checkpoint.lag.sequences", () -> run != null ? run.checkpointLagSequences : 0)
                .description("Newest sequence seen minus the checkpoint's; grows while a hole is open")
                .register(meters);
        Gauge.builder("marketdata.db.write.consecutive.failures",
                        () -> run != null ? run.consecutiveFailures : 0)
                .register(meters);
    }

    // ------------------------------------------------------------------ lifecycle

    /** Nothing starts here: consumption waits for leadership. */
    @Override
    public void start() {
        lifecycleRunning = true;
    }

    /** Shutdown: stop reading, and write what is already queued, within a deadline. */
    @Override
    public void stop() {
        stopConsuming(true);
        lifecycleRunning = false;
        log.info("Consumption stopped after {} quote(s), {} duplicate(s), {} gap(s)",
                consumed.get(), duplicates.get(), gapsDetected.get());
    }

    @Override
    public boolean isRunning() {
        return lifecycleRunning;
    }

    /** Stopped before the leader initiator, so shutdown drains before it gives the lock up. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    // ------------------------------------------------------------------ leadership

    /**
     * Begins consuming. Returns at once — the caller is the election thread, which must keep
     * renewing the lock — and does the work on a writer thread of its own.
     *
     * @param yieldLeadership called when this run ends without being told to — fenced, or failed —
     *     so the lock goes to a standby instead of being renewed by an instance that ingests nothing
     */
    public synchronized void startConsuming(Runnable yieldLeadership) {
        if (run != null) {
            return;
        }
        Run next = new Run(yieldLeadership);
        run = next;
        next.writer = Thread.ofPlatform().name("marketdata-writer").daemon().start(next::execute);
    }

    /**
     * Ends consuming.
     *
     * @param drain write what is queued first (shutdown), or abandon it (leadership lost: the next
     *     leader replays it from the checkpoint, and our writes would be fenced anyway)
     */
    public void stopConsuming(boolean drain) {
        Run current;
        synchronized (this) {
            current = run;
            if (current == null) {
                return;
            }
            run = null;
        }
        // Outside the monitor: the writer takes it on its way out, and joining it while holding the
        // monitor would wait out the full timeout.
        current.stop(drain);
    }

    public boolean isLeading() {
        return run != null;
    }

    // ------------------------------------------------------------------ one leadership

    private final class Run {

        final IngestQueue queue = new IngestQueue(
                settings.queueCapacity(), settings.queueHighWatermark(), settings.queueLowWatermark());
        final Runnable yieldLeadership;
        Thread writer;

        volatile boolean consuming = true;
        volatile boolean drain;
        volatile long drainDeadlineNanos;

        volatile long term;
        volatile long outstanding;
        volatile long checkpointLagSequences;
        volatile int consecutiveFailures;
        volatile long failingSinceNanos;
        volatile Instant lastQuoteAt;
        volatile Duration lastLag = Duration.ZERO;
        volatile Checkpoint committed = Checkpoint.none();

        /** Writer-thread state. */
        SequenceTracker tracker;
        long holeFrom = -1;
        long holeSinceNanos;
        int backfillAttempts;
        /** Queue position when the last replay was requested; the hole's clock waits until past it. */
        long replayRequestedAt = -1;
        long reportedWrittenOff;

        Run(Runnable yieldLeadership) {
            this.yieldLeadership = yieldLeadership;
        }

        void execute() {
            try {
                term = retrying("claim the feed", () -> leadership.claim(instanceId, clock.instant()));
                awaitReplicationSlot();
                committed = retrying("load the checkpoint", checkpoints::load);
                tracker = committed.isPresent()
                        ? new SequenceTracker(committed.sequence(), committed.eventTime())
                        : new SequenceTracker();
                log.info("Leading ingestion with term {} (resume from {})",
                        term, committed.isPresent() ? committed.eventTime() : "now");

                if (!startClient()) {
                    return;
                }
                writeLoop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                log.error("Ingestion failed", e);
            } finally {
                releaseClient();
                if (!queue.isEmpty()) {
                    log.warn("{} queued quote(s) not written; they will be replayed from the checkpoint",
                            queue.depth());
                }
                boolean endedOnItsOwn;
                synchronized (QuoteConsumer.this) {
                    endedOnItsOwn = run == this;
                    if (endedOnItsOwn) {
                        run = null;
                    }
                }
                // Fenced, failed, or interrupted by something other than stopConsuming. Keeping the
                // lock would leave no instance ingesting and the standby unable to take over.
                if (endedOnItsOwn) {
                    log.warn("Ingestion ended without being stopped; yielding leadership");
                    yieldLeadership.run();
                }
            }
        }

        /**
         * Writes nothing until the outbox can be read. Holding back costs latency, and the
         * exchange's replay window absorbs it; writing first would publish events into a WAL
         * nobody is reading, and the distribution service would never learn of them.
         */
        private void awaitReplicationSlot() throws InterruptedException {
            if (!settings.requireCdcSlot()) {
                return;
            }
            long lastWarned = 0;
            while (consuming) {
                boolean ready;
                try {
                    ready = quotes.replicationSlotReady(settings.cdcSlot());
                } catch (RuntimeException e) {
                    ready = false;
                }
                if (ready) {
                    return;
                }
                if (lastWarned == 0 || System.nanoTime() - lastWarned > TimeUnit.SECONDS.toNanos(30)) {
                    log.warn("Waiting for replication slot {} before ingesting; is the outbox connector "
                            + "registered?", settings.cdcSlot());
                    lastWarned = System.nanoTime();
                }
                TimeUnit.SECONDS.sleep(2);
            }
            throw new InterruptedException("stopped while waiting for the replication slot");
        }

        /** Takes the exchange client once the previous leadership has let go of it. */
        private boolean startClient() throws InterruptedException {
            while (consuming) {
                if (clientLock.tryLock(100, TimeUnit.MILLISECONDS)) {
                    try {
                        // Re-checked under the lock that stop() takes: a stop that arrived during
                        // start-up must win, or the client would run with nobody to stop it.
                        if (!consuming) {
                            return false;
                        }
                        if (clientOwner == null) {
                            exchange.start(this::accept, checkpoints::load, queue::awaitResumed);
                            clientOwner = this;
                            return true;
                        }
                    } finally {
                        clientLock.unlock();
                    }
                }
                TimeUnit.MILLISECONDS.sleep(100);
            }
            return false;
        }

        private void releaseClient() {
            clientLock.lock();
            try {
                if (clientOwner == this) {
                    exchange.stop();
                    clientOwner = null;
                }
            } finally {
                clientLock.unlock();
            }
        }

        void stop(boolean drainFirst) {
            drain = drainFirst;
            drainDeadlineNanos = System.nanoTime() + settings.shutdownDrainTimeout().toNanos();
            consuming = false;
            // Stop the producer first, so the drain has a fixed amount to get through.
            releaseClient();
            if (!drainFirst) {
                writer.interrupt();
            }
            try {
                writer.join(settings.shutdownDrainTimeout().plusSeconds(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /** Socket thread. */
        void accept(Quote quote) throws InterruptedException {
            if (queue.put(quote)) {
                highWatermarkTrips.increment();
                log.warn("Ingest queue reached its high watermark ({} of {}); closing the socket "
                                + "until it drains to {}",
                        queue.depth(), queue.capacity(), settings.queueLowWatermark());
                exchange.disconnect(ExchangeWebSocketClient.Reason.BACKPRESSURE);
            }
        }

        // -------------------------------------------------------------- writer thread

        private void writeLoop() throws InterruptedException {
            List<Quote> batch = new ArrayList<>(settings.maxBatchSize());
            while (consuming || (drain && !queue.isEmpty() && System.nanoTime() < drainDeadlineNanos)) {
                batch.clear();
                queue.drain(batch, settings.maxBatchSize(), settings.flushInterval());
                if (!batch.isEmpty() && !write(batch)) {
                    return;
                }
                queue.afterDrain();
                superviseGaps();
            }
        }

        /** @return false if the batch was abandoned: fenced, or shutting down while failing */
        private boolean write(List<Quote> batch) throws InterruptedException {
            inspectSequences(batch);
            SequenceTracker.Position safe = tracker.safePosition();
            Quote newest = QuoteBatch.highWaterMark(batch);

            for (int attempt = 0; ; attempt++) {
                Instant now = clock.instant();
                Timer.Sample sample = Timer.start(meters);
                try {
                    QuoteRepository.WriteResult result = transactions.execute(status -> {
                        // First, so a deposed leader's transaction holds nothing when it is refused
                        // — and publishes nothing, since the outbox rows are in this transaction too.
                        leadership.assertCurrent(term);
                        QuoteRepository.WriteResult written = quotes.writeBatch(batch);
                        // The contiguous prefix, not the newest quote: resuming from the newest would
                        // step over an open hole and make it permanent.
                        checkpoints.save(safe.eventTime(), safe.sequence(), now);
                        return written;
                    });
                    sample.stop(writeTimer("success"));
                    committed(batch, result, safe, newest, now);
                    return true;
                } catch (LeadershipRepository.FencedException e) {
                    sample.stop(writeTimer("fenced"));
                    meters.counter("marketdata.leader.transitions", "event", "fenced").increment();
                    log.error("Write refused, {}; stopping ingestion on this instance", e.getMessage());
                    consuming = false;
                    drain = false;
                    return false;
                } catch (RuntimeException e) {
                    sample.stop(writeTimer("failure"));
                    if (consecutiveFailures++ == 0) {
                        failingSinceNanos = System.nanoTime();
                    }
                    if (!consuming && !(drain && System.nanoTime() < drainDeadlineNanos)) {
                        log.error("Abandoning a batch of {} after {} failed attempt(s); it will be "
                                + "replayed from the checkpoint", batch.size(), attempt + 1);
                        return false;
                    }
                    Duration delay = retryDelay(attempt);
                    writeRetries.increment();
                    log.warn("Write of {} quote(s) failed (attempt {}), retrying in {} ms: {}",
                            batch.size(), attempt + 1, delay.toMillis(),
                            NestedExceptionUtils.getMostSpecificCause(e).toString());
                    TimeUnit.MILLISECONDS.sleep(delay.toMillis());
                }
            }
        }

        private void committed(List<Quote> batch, QuoteRepository.WriteResult result,
                SequenceTracker.Position safe, Quote newest, Instant now) {
            consecutiveFailures = 0;
            consumed.addAndGet(batch.size());
            quotesWritten.increment(batch.size());
            rowsInserted.increment(result.inserted());
            outboxPublished.increment(result.published());
            committed = new Checkpoint(safe.eventTime(), safe.sequence(), now);
            lastQuoteAt = newest.eventTime();
            lastLag = newest.ingestionLag();
            ingestLag.record(lastLag.isNegative() ? Duration.ZERO : lastLag);
        }

        private Timer writeTimer(String outcome) {
            return Timer.builder("marketdata.db.write")
                    .description("One batch transaction: fence check, COPY, history + outbox, checkpoint")
                    .tag("outcome", outcome)
                    .publishPercentiles(0.5, 0.99)
                    .register(meters);
        }

        private void inspectSequences(List<Quote> batch) {
            for (Quote quote : batch) {
                SequenceTracker.Observation observation =
                        tracker.observe(quote.sequence(), quote.eventTime());
                switch (observation.verdict()) {
                    case DUPLICATE -> {
                        duplicates.incrementAndGet();
                        duplicatesCounter.increment();
                    }
                    case GAP -> {
                        gapsDetected.incrementAndGet();
                        gapsCounter.increment();
                        log.warn("Sequence gap: {}..{} ({} message(s)) not received",
                                observation.missingFrom(), observation.missingTo(),
                                observation.missingCount());
                    }
                    case BACKFILL -> backfilledCounter.increment();
                    case RESET -> {
                        resetsCounter.increment();
                        log.warn("Exchange sequence restarted at {}", quote.sequence());
                    }
                    default -> { }
                }
            }
            reportWriteOffs();
            checkpointLagSequences = tracker.checkpointLagSequences();
        }

        /**
         * Every write-off reaches the loss metric and the log, whichever path made it: exhausted
         * replays, a sequence reset, or the tracker's memory bound.
         */
        private void reportWriteOffs() {
            long total = tracker.writtenOff();
            if (total > reportedWrittenOff) {
                long lost = total - reportedWrittenOff;
                lostCounter.increment(lost);
                log.error("{} message(s) written off as lost", lost);
                reportedWrittenOff = total;
            }
            outstanding = tracker.outstanding();
        }

        /**
         * Resolves open holes. A message may merely be late, so a hole gets a grace period; then the
         * socket is closed, which replays everything from the checkpoint — the end of the contiguous
         * run, i.e. the start of the hole. After a few replays that do not fill it, the exchange
         * evidently does not have it, and it is written off so the checkpoint can move on.
         */
        private void superviseGaps() {
            Optional<SequenceTracker.Hole> hole = tracker.oldestHole();
            if (hole.isEmpty()) {
                holeFrom = -1;
                return;
            }
            long now = System.nanoTime();
            boolean replayNotYetSeen = replayRequestedAt >= 0 && queue.takenCount() <= replayRequestedAt;
            if (hole.get().from() != holeFrom || !exchange.isConnected() || replayNotYetSeen) {
                // A new hole, or no replay could have reached the tracker yet — not connected, or
                // still working through what was queued before the replay was requested. Without
                // the last condition a deep backlog would burn through every attempt, and write
                // the hole off, before the first replay was even read.
                if (hole.get().from() != holeFrom) {
                    backfillAttempts = 0;
                    replayRequestedAt = -1;
                }
                holeFrom = hole.get().from();
                holeSinceNanos = now;
                return;
            }
            if (now - holeSinceNanos < settings.gapBackfillAfter().toNanos()) {
                return;
            }
            SequenceTracker.Hole open = hole.get();
            if (backfillAttempts < settings.gapMaxBackfills()) {
                backfillAttempts++;
                holeSinceNanos = now;
                backfillRequests.increment();
                replayRequestedAt = queue.putCount();
                log.warn("Sequence hole {}..{} still open; replaying from the checkpoint (attempt {}/{})",
                        open.from(), open.to(), backfillAttempts, settings.gapMaxBackfills());
                exchange.disconnect(ExchangeWebSocketClient.Reason.GAP_BACKFILL);
                return;
            }
            log.error("Sequence hole {}..{} not recovered after {} replays; writing it off",
                    open.from(), open.to(), settings.gapMaxBackfills());
            tracker.writeOffOldestHole();
            reportWriteOffs();
            holeFrom = -1;
            replayRequestedAt = -1;
        }

        private Duration retryDelay(int attempt) {
            long initial = settings.writeRetryInitialDelay().toMillis();
            long cap = settings.writeRetryMaxDelay().toMillis();
            long step = Math.min(cap, initial << Math.min(attempt, 20));
            long half = step / 2;
            return Duration.ofMillis(half + ThreadLocalRandom.current().nextLong(half + 1));
        }

        /** Start-up steps retried like writes, since the database may be down when leadership arrives. */
        private <T> T retrying(String what, java.util.function.Supplier<T> step) throws InterruptedException {
            for (int attempt = 0; ; attempt++) {
                if (!consuming) {
                    throw new InterruptedException("stopped while trying to " + what);
                }
                try {
                    return step.get();
                } catch (RuntimeException e) {
                    Duration delay = retryDelay(attempt);
                    log.warn("Could not {} (attempt {}), retrying in {} ms: {}", what, attempt + 1,
                            delay.toMillis(), NestedExceptionUtils.getMostSpecificCause(e).toString());
                    TimeUnit.MILLISECONDS.sleep(delay.toMillis());
                }
            }
        }

        double checkpointLagSeconds() {
            Checkpoint c = committed;
            return c.isPresent() ? Duration.between(c.eventTime(), clock.instant()).toMillis() / 1000.0 : 0;
        }
    }

    // ------------------------------------------------------------------ status

    /**
     * @param quotesMissing sequences missing right now plus those written off. Anything but zero
     *     means the stored history has holes, open or permanent.
     */
    public record ConsumptionStatus(
            boolean leader,
            long term,
            boolean connected,
            Instant lastQuoteAt,
            long lagMillis,
            Checkpoint checkpoint,
            int queueDepth,
            int queueCapacity,
            boolean queuePaused,
            long highWatermarkTrips,
            int consecutiveWriteFailures,
            long quotesConsumed,
            long duplicatesDiscarded,
            long gapsDetected,
            long sequencesOutstanding,
            long sequencesLost,
            long quotesMissing) {}

    public ConsumptionStatus status() {
        Run current = run;
        long lost = (long) lostCounter.count();
        if (current == null) {
            return new ConsumptionStatus(false, 0, false, null, 0, Checkpoint.none(), 0,
                    settings.queueCapacity(), false, 0, 0, consumed.get(), duplicates.get(),
                    gapsDetected.get(), 0, lost, lost);
        }
        return new ConsumptionStatus(
                true,
                current.term,
                exchange.isConnected(),
                current.lastQuoteAt,
                current.lastLag.toMillis(),
                current.committed,
                current.queue.depth(),
                current.queue.capacity(),
                current.queue.isPaused(),
                (long) highWatermarkTrips.count(),
                current.consecutiveFailures,
                consumed.get(),
                duplicates.get(),
                gapsDetected.get(),
                current.outstanding,
                lost,
                current.outstanding + lost);
    }

    /** How long writes have been failing without a success; zero if the last one succeeded. */
    public Duration writesFailingFor() {
        Run current = run;
        if (current == null || current.consecutiveFailures == 0) {
            return Duration.ZERO;
        }
        return Duration.ofNanos(System.nanoTime() - current.failingSinceNanos);
    }

    public boolean isPausedForBackpressure() {
        Run current = run;
        return current != null && current.queue.isPaused();
    }

    public Duration exchangeDisconnectedFor() {
        return exchange.disconnectedFor();
    }
}
