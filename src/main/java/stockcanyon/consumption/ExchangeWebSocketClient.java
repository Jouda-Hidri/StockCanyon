package stockcanyon.consumption;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;

import stockcanyon.Checkpoint;
import stockcanyon.Isin;
import stockcanyon.MarketDataProperties;
import stockcanyon.Quote;

/**
 * Consumes {@code GET /quotes?checkpoint_timestamp=}. The brief's fallback mechanism:
 *
 * <ol>
 *   <li><b>Resume from a checkpoint</b>, re-read on every attempt, so the exchange replays what was
 *       published while the socket was down.
 *   <li><b>Reconnect</b> with exponential backoff and jitter, so a fleet that loses the exchange at
 *       the same moment does not come back in lockstep.
 *   <li><b>Detect a stalled socket.</b> A wedged peer sends no error and no close — TCP is
 *       satisfied and the connection just goes quiet. Only a stall timer catches that, which is
 *       why the exchange sends heartbeats.
 *   <li><b>Disconnect on request</b>: when the ingest queue passes its high watermark, and when a
 *       sequence hole needs replaying. Both rely on the same resume-from-checkpoint path.
 * </ol>
 *
 * <p>One supervisor loop drives reconnection, not the socket callbacks: an error and a close fire
 * for the same failure, and acting on both opens two live sockets.
 */
public class ExchangeWebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(ExchangeWebSocketClient.class);

    private static final Duration STALL_CHECK_INTERVAL = Duration.ofSeconds(1);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    /** Receives each quote. May block; the socket is not read while it does. */
    @FunctionalInterface
    public interface QuoteSink {
        void accept(Quote quote) throws InterruptedException;
    }

    /** Holds the next connection attempt until the consumer can take more. */
    @FunctionalInterface
    public interface Gate {
        void awaitOpen() throws InterruptedException;
    }

    /** Why a connection ended. The metric tag, and whether backoff applies. */
    public enum Reason {
        CLOSED(false), ERROR(false), STALL(false), CONNECT_FAILED(false),
        BACKPRESSURE(true), GAP_BACKFILL(true);

        /** Deliberate: reconnect as soon as the gate allows, without backing off. */
        final boolean requested;

        Reason(boolean requested) {
            this.requested = requested;
        }

        String tag() {
            return name().toLowerCase();
        }
    }

    private final URI endpoint;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final MarketDataProperties.Consumption settings;
    private final HttpClient http;
    private final MeterRegistry meters;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong lastFrameNanos = new AtomicLong(System.nanoTime());
    private volatile Thread supervisor;
    private volatile Session session;
    private volatile boolean connected;
    private volatile long disconnectedSinceNanos = System.nanoTime();

    public ExchangeWebSocketClient(
            URI endpoint,
            ObjectMapper mapper,
            Clock clock,
            MarketDataProperties.Consumption settings,
            MeterRegistry meters) {
        this.endpoint = endpoint;
        this.mapper = mapper;
        this.clock = clock;
        this.settings = settings;
        this.meters = meters;
        this.http = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();

        Gauge.builder("marketdata.feed.connected", () -> connected ? 1 : 0)
                .description("1 while a socket to the exchange is open")
                .register(meters);
        Gauge.builder("marketdata.feed.last.frame.age", () -> connected
                        ? (System.nanoTime() - lastFrameNanos.get()) / 1e9 : 0)
                .description("Seconds since the open socket last delivered anything, heartbeats included")
                .baseUnit("seconds")
                .register(meters);
    }

    /**
     * Consumes until {@link #stop()}, reconnecting as needed.
     *
     * @param resumePoint read on every attempt, so a reconnect resumes from what is stored now
     * @param gate awaited before every attempt
     */
    public void start(QuoteSink onQuote, Supplier<Checkpoint> resumePoint, Gate gate) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        supervisor = Thread.ofVirtual()
                .name("marketdata-consumption")
                .start(() -> supervise(onQuote, resumePoint, gate));
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Session current = session;
        if (current != null) {
            current.end(Reason.CLOSED);
        }
        Thread thread = supervisor;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(Duration.ofSeconds(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Closes the current socket on purpose. The supervisor reconnects — through the gate, and from
     * the stored checkpoint — without backing off.
     */
    public void disconnect(Reason reason) {
        Session current = session;
        if (current != null) {
            current.end(reason);
        }
    }

    public boolean isConnected() {
        return connected;
    }

    /** How long there has been no open socket; zero while connected. */
    public Duration disconnectedFor() {
        return connected ? Duration.ZERO : Duration.ofNanos(System.nanoTime() - disconnectedSinceNanos);
    }

    // ------------------------------------------------------------------ connection lifecycle

    private void supervise(QuoteSink onQuote, Supplier<Checkpoint> resumePoint, Gate gate) {
        int failures = 0;
        while (running.get()) {
            Reason ended;
            Session current = null;
            CompletableFuture<WebSocket> handshake = null;
            try {
                // Paused for back-pressure: stay disconnected until the writer has caught up.
                gate.awaitOpen();
                if (!running.get()) {
                    break;
                }
                Checkpoint resume = resumePoint.get();
                current = new Session(onQuote);

                log.info("Connecting to the exchange (resume from {})",
                        resume.isPresent() ? resume.eventTime() : "now");
                handshake = http.newWebSocketBuilder()
                        .connectTimeout(CONNECT_TIMEOUT)
                        .buildAsync(uriFor(resume), current);
                WebSocket ws = handshake.get(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                session = current;
                connected = true;
                touch();
                connects("success");

                ended = awaitEnd(current, ws);
                if (current.framesReceived > 0) {
                    failures = 0;
                }
            } catch (InterruptedException e) {
                abandon(current, handshake);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                abandon(current, handshake);
                if (running.get()) {
                    log.warn("Exchange connection attempt failed: {}",
                            NestedExceptionUtils.getMostSpecificCause(e).toString());
                }
                connects("failure");
                ended = Reason.CONNECT_FAILED;
            } finally {
                if (connected) {
                    disconnectedSinceNanos = System.nanoTime();
                }
                connected = false;
                session = null;
            }

            if (!running.get()) {
                break;
            }
            meters.counter("marketdata.feed.disconnects", "reason", ended.tag()).increment();
            Duration delay = ended.requested ? Duration.ZERO : backoff(failures++);
            if (!pause(delay)) {
                break;
            }
        }
        log.info("Exchange consumption stopped");
    }

    /**
     * A handshake given up on — timed out, or interrupted by stop() — can still complete later. Its
     * session is ended first, so any frame it delivers is ignored, and the socket is aborted as soon
     * as it exists, so nothing is left connected without a supervisor.
     */
    private static void abandon(Session session, CompletableFuture<WebSocket> handshake) {
        if (session != null) {
            session.end(Reason.CLOSED);
        }
        if (handshake != null) {
            handshake.whenComplete((ws, error) -> {
                if (ws != null) {
                    ws.abort();
                }
            });
        }
    }

    /** Blocks until the socket fails, closes, goes silent, or is ended on purpose. */
    private Reason awaitEnd(Session current, WebSocket ws) throws InterruptedException {
        try {
            while (running.get()) {
                if (current.ended.await(STALL_CHECK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)) {
                    log.warn("Exchange connection ended: {}", current.detail);
                    return current.reason;
                }
                long silentNanos = System.nanoTime() - lastFrameNanos.get();
                if (silentNanos > settings.stallTimeout().toNanos()) {
                    log.warn("Exchange delivered nothing for {}s; treating the socket as dead",
                            TimeUnit.NANOSECONDS.toSeconds(silentNanos));
                    return Reason.STALL;
                }
            }
            return Reason.CLOSED;
        } finally {
            // Always, whatever ended it: a socket left half-open would keep delivering into a
            // session nobody is supervising any more.
            ws.abort();
        }
    }

    /**
     * Exponential, capped, with "equal jitter": half the step is fixed, half random. The fixed half
     * keeps a floor under the delay; the random half keeps instances from retrying in lockstep.
     */
    Duration backoff(int failures) {
        long initial = settings.reconnectInitialDelay().toMillis();
        long cap = settings.reconnectMaxDelay().toMillis();
        long step = Math.min(cap, initial << Math.min(failures, 20));
        long half = step / 2;
        return Duration.ofMillis(half + ThreadLocalRandom.current().nextLong(half + 1));
    }

    /** @return false if interrupted, meaning shutdown */
    private boolean pause(Duration delay) {
        if (delay.isZero()) {
            return true;
        }
        log.info("Reconnecting to the exchange in {} ms", delay.toMillis());
        try {
            Thread.sleep(delay.toMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void connects(String outcome) {
        meters.counter("marketdata.feed.connects", "outcome", outcome).increment();
    }

    /** Omits the parameter on a cold start, which the contract reads as "from now". */
    private URI uriFor(Checkpoint resumePoint) {
        String checkpoint = resumePoint.toQueryParameter();
        if (checkpoint == null) {
            return endpoint;
        }
        String separator = endpoint.getQuery() == null ? "?" : "&";
        return URI.create(endpoint + separator + "checkpoint_timestamp="
                + URLEncoder.encode(checkpoint, StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------ frames

    /**
     * The exchange's wire format. Unknown fields are ignored so a new field upstream is not an
     * outage here.
     */
    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    private record Frame(
            String type,
            String isin,
            long sequence,
            BigDecimal bid,
            BigDecimal ask,
            BigDecimal bidSize,
            BigDecimal askSize,
            String currency,
            Instant timestamp,
            String code,
            String message) {}

    private void handleFrame(String payload, QuoteSink onQuote) throws InterruptedException {
        Frame frame;
        try {
            frame = mapper.readValue(payload, Frame.class);
        } catch (Exception e) {
            frames("unparseable");
            log.warn("Ignoring unparseable frame: {}", truncate(payload));
            return;
        }
        String type = frame.type() == null ? "quote" : frame.type();
        switch (type) {
            case "quote" -> {
                Quote quote;
                try {
                    quote = toQuote(frame);
                } catch (RuntimeException e) {
                    // Dropped here, where it costs one sequence number, rather than failing every
                    // batch it would land in. The hole it leaves is replayed, then written off and
                    // alerted on if the exchange keeps sending the same bad quote.
                    frames("invalid");
                    log.error("Rejecting invalid quote {}: {}", frame.sequence(), e.getMessage());
                    return;
                }
                frames("quote");
                onQuote.accept(quote);
            }
            // Its only job is to make silence mean something; the transport already reset the timer.
            case "heartbeat" -> frames("heartbeat");
            case "error" -> {
                frames("error");
                meters.counter("marketdata.feed.exchange.errors",
                        "code", String.valueOf(frame.code())).increment();
                log.error("Exchange reported {}: {}", frame.code(), frame.message());
            }
            default -> {
                frames("unknown");
                log.debug("Ignoring frame of unknown type: {}", frame.type());
            }
        }
    }

    private void frames(String type) {
        meters.counter("marketdata.feed.frames", "type", type).increment();
    }

    private Quote toQuote(Frame frame) {
        return new Quote(
                Isin.of(frame.isin()),
                frame.sequence(),
                frame.bid(),
                frame.ask(),
                frame.bidSize(),
                frame.askSize(),
                frame.currency() == null ? "USD" : frame.currency(),
                frame.timestamp(),
                clock.instant());
    }

    private void touch() {
        lastFrameNanos.set(System.nanoTime());
    }

    /** One connection's listener state, so a late frame cannot join the next connection's message. */
    private final class Session implements WebSocket.Listener {

        private final CountDownLatch ended = new CountDownLatch(1);
        private final StringBuilder partial = new StringBuilder();
        private final QuoteSink onQuote;
        private volatile Reason reason = Reason.CLOSED;
        private volatile String detail = "closed";
        private volatile long framesReceived;

        private Session(QuoteSink onQuote) {
            this.onQuote = onQuote;
        }

        /** First reason wins: the close that follows an abort is a consequence, not a cause. */
        void end(Reason why) {
            end(why, why.tag());
        }

        private synchronized void end(Reason why, String text) {
            if (ended.getCount() == 0) {
                return;
            }
            reason = why;
            detail = text;
            ended.countDown();
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            if (ended.getCount() == 0) {
                // Ended on purpose; anything still arriving on this socket is replayed later.
                return null;
            }
            touch();
            partial.append(data);
            if (last) {
                framesReceived++;
                String payload = partial.toString();
                partial.setLength(0);
                try {
                    handleFrame(payload, onQuote);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    end(Reason.CLOSED, "interrupted");
                    return null;
                } catch (RuntimeException e) {
                    log.warn("Could not handle a frame: {}", e.toString());
                }
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String text) {
            end(Reason.CLOSED, "closed " + statusCode + (text == null || text.isBlank() ? "" : " " + text));
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            end(Reason.ERROR, "error: " + error);
        }
    }

    private static String truncate(String payload) {
        return payload.length() <= 200 ? payload : payload.substring(0, 200) + "...";
    }
}
