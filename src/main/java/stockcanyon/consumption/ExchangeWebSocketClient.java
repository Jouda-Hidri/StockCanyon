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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import stockcanyon.Checkpoint;
import stockcanyon.Isin;
import stockcanyon.MarketDataProperties;
import stockcanyon.Quote;

/**
 * Consumes {@code GET /quotes?checkpoint_timestamp=}. The brief's fallback mechanism, in three
 * parts:
 *
 * <ol>
 *   <li><b>Resume from a checkpoint</b>, re-read on every attempt, so the exchange replays what was
 *       published while the socket was down.
 *   <li><b>Reconnect</b> after a fixed delay.
 *   <li><b>Detect a stalled socket.</b> A wedged peer sends no error and no close — TCP is
 *       satisfied and the connection just goes quiet. Only a stall timer catches that, which is
 *       why the exchange sends heartbeats.
 * </ol>
 *
 * <p>One supervisor loop drives reconnection, not the socket callbacks: an error and a close fire
 * for the same failure, and acting on both opens two live sockets.
 */
public class ExchangeWebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(ExchangeWebSocketClient.class);

    private static final Duration STALL_CHECK_INTERVAL = Duration.ofSeconds(1);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private final URI endpoint;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final MarketDataProperties.Consumption settings;
    private final HttpClient http;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong lastFrameNanos = new AtomicLong();
    private volatile Thread supervisor;
    private volatile WebSocket socket;
    private volatile boolean connected;

    public ExchangeWebSocketClient(
            URI endpoint,
            ObjectMapper mapper,
            Clock clock,
            MarketDataProperties.Consumption settings) {
        this.endpoint = endpoint;
        this.mapper = mapper;
        this.clock = clock;
        this.settings = settings;
        this.http = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                // Virtual threads: the frame handler blocks while it writes to the database.
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    /**
     * Consumes until {@link #stop()}, reconnecting as needed.
     *
     * @param onQuote receives each quote; may block, and the socket is not read while it does
     * @param onIdle called on a heartbeat, so a quiet market still gets a tick
     * @param resumePoint read on every attempt, so a reconnect resumes from what is stored now
     */
    public void start(Consumer<Quote> onQuote, Runnable onIdle, Supplier<Checkpoint> resumePoint) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        supervisor = Thread.ofVirtual()
                .name("marketdata-consumption")
                .start(() -> supervise(onQuote, onIdle, resumePoint));
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        WebSocket current = socket;
        if (current != null) {
            current.abort();
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

    public boolean isConnected() {
        return connected;
    }

    // ------------------------------------------------------------------ connection lifecycle

    private void supervise(Consumer<Quote> onQuote, Runnable onIdle, Supplier<Checkpoint> resumePoint) {
        while (running.get()) {
            try {
                Checkpoint resume = resumePoint.get();
                Session session = new Session(onQuote, onIdle);

                log.info("Connecting to the exchange (resume from {})",
                        resume.isPresent() ? resume.eventTime() : "now");
                WebSocket ws = http.newWebSocketBuilder()
                        .connectTimeout(CONNECT_TIMEOUT)
                        .buildAsync(uriFor(resume), session)
                        .get(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                socket = ws;
                connected = true;
                touch();

                awaitFailure(session, ws);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (running.get()) {
                    log.warn("Exchange connection attempt failed: {}", rootMessage(e));
                }
            } finally {
                connected = false;
                socket = null;
            }

            if (!running.get() || !pauseBeforeRetry()) {
                break;
            }
        }
        log.info("Exchange consumption stopped");
    }

    /** Blocks until the socket fails, closes, or goes silent past the stall timeout. */
    private void awaitFailure(Session session, WebSocket ws) throws InterruptedException {
        while (running.get()) {
            if (session.closed.await(STALL_CHECK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Exchange connection ended: {}", session.reason);
                return;
            }
            long silentNanos = System.nanoTime() - lastFrameNanos.get();
            if (silentNanos > settings.stallTimeout().toNanos()) {
                log.warn("Exchange delivered nothing for {}s; treating the socket as dead",
                        TimeUnit.NANOSECONDS.toSeconds(silentNanos));
                ws.abort();
                return;
            }
        }
    }

    /**
     * Waits before retrying, so a refusing exchange is not hammered in a tight loop.
     *
     * <p>A fixed delay. Exponential backoff with jitter would be the production upgrade — it
     * matters once several instances can lose the exchange at the same moment and retry in
     * lockstep — but it is not what makes consumption gap-free, so it is left out here.
     *
     * @return false if interrupted, meaning shutdown
     */
    private boolean pauseBeforeRetry() {
        log.info("Reconnecting to the exchange in {}", settings.reconnectDelay());
        try {
            Thread.sleep(settings.reconnectDelay().toMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
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

    private void handleFrame(String payload, Consumer<Quote> onQuote, Runnable onIdle) {
        JsonNode node;
        try {
            node = mapper.readTree(payload);
        } catch (Exception e) {
            log.warn("Ignoring unparseable frame: {}", truncate(payload));
            return;
        }
        switch (node.path("type").asText("quote")) {
            case "quote" -> onQuote.accept(toQuote(node));
            // The stall timer is already reset by the transport layer; the idle tick is what lets
            // a partial batch be written when the market goes quiet.
            case "heartbeat" -> onIdle.run();
            case "error" -> log.error("Exchange reported {}: {}",
                    node.path("code").asText("UNKNOWN"), node.path("message").asText());
            default -> log.debug("Ignoring frame of unknown type: {}", truncate(payload));
        }
    }

    private Quote toQuote(JsonNode node) {
        return new Quote(
                Isin.of(node.get("isin").asText()),
                node.path("sequence").asLong(-1),
                decimal(node, "bid"),
                decimal(node, "ask"),
                decimal(node, "bidSize"),
                decimal(node, "askSize"),
                node.path("currency").asText("USD"),
                Instant.parse(node.get("timestamp").asText()),
                clock.instant());
    }

    private void touch() {
        lastFrameNanos.set(System.nanoTime());
    }

    /** One connection's listener state, so a late frame cannot join the next connection's message. */
    private final class Session implements WebSocket.Listener {

        private final CountDownLatch closed = new CountDownLatch(1);
        private final StringBuilder partial = new StringBuilder();
        private final Consumer<Quote> onQuote;
        private final Runnable onIdle;
        private volatile String reason = "closed";

        private Session(Consumer<Quote> onQuote, Runnable onIdle) {
            this.onQuote = onQuote;
            this.onIdle = onIdle;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            touch();
            partial.append(data);
            if (last) {
                String payload = partial.toString();
                partial.setLength(0);
                try {
                    handleFrame(payload, onQuote, onIdle);
                } catch (RuntimeException e) {
                    log.warn("Could not handle a frame: {}", e.toString());
                }
            }
            // Only now, after the quote was accepted: while saturated we request nothing, the TCP
            // window closes, and the exchange slows instead of this service shedding quotes.
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPing(WebSocket webSocket, java.nio.ByteBuffer message) {
            touch();
            webSocket.sendPong(message);
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPong(WebSocket webSocket, java.nio.ByteBuffer message) {
            touch();
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String text) {
            reason = "closed " + statusCode + (text == null || text.isBlank() ? "" : " " + text);
            closed.countDown();
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            reason = "error: " + error;
            closed.countDown();
        }
    }

    private static BigDecimal decimal(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : new BigDecimal(value.asText());
    }

    private static String truncate(String payload) {
        return payload.length() <= 200 ? payload : payload.substring(0, 200) + "...";
    }

    private static String rootMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.toString();
    }
}
