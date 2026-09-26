package stockcanyon.simulator;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

/**
 * Serves {@code /exchange/quotes?checkpoint_timestamp=}.
 *
 * <p>Absent or {@code null} means "from now". Otherwise the stream opens with everything published
 * at or after that instant, then continues live with no seam.
 */
@Component
@ConditionalOnProperty(prefix = "marketdata.simulator", name = "enabled", havingValue = "true")
public class SimulatedExchangeHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(SimulatedExchangeHandler.class);
    private static final int MAX_FRAMES_PER_READ = 256;
    private static final Duration READ_WAIT = Duration.ofMillis(500);

    private final QuoteLog quoteLog;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, Thread> pumps = new ConcurrentHashMap<>();
    private final AtomicInteger toSkip = new AtomicInteger();

    public SimulatedExchangeHandler(QuoteLog quoteLog, ObjectMapper mapper, Clock clock) {
        this.quoteLog = quoteLog;
        this.mapper = mapper;
        this.clock = clock;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String raw = checkpointParameter(session);
        long cursor;
        try {
            cursor = raw == null ? quoteLog.tailCursor() : quoteLog.cursorAtOrAfter(parseCheckpoint(raw));
        } catch (QuoteLog.EvictedException e) {
            // Refuse loudly: fast-forwarding to the live edge would hand them an undetectable gap.
            refuse(session, "CHECKPOINT_TOO_OLD", e.getMessage());
            return;
        } catch (DateTimeParseException e) {
            refuse(session, "INVALID_CHECKPOINT", "cannot parse checkpoint_timestamp: " + raw);
            return;
        }

        log.info("Exchange session {} subscribed (checkpoint={}, replaying {} message(s))",
                session.getId(), raw == null ? "now" : raw, quoteLog.published() - cursor);

        sessions.put(session.getId(), session);
        pumps.put(session.getId(),
                Thread.ofVirtual().name("exchange-feed-" + session.getId()).start(() -> pump(session, cursor)));
    }

    private void pump(WebSocketSession session, long startCursor) {
        long cursor = startCursor;
        try {
            while (session.isOpen() && !Thread.currentThread().isInterrupted()) {
                QuoteLog.Batch batch = quoteLog.read(cursor, MAX_FRAMES_PER_READ, READ_WAIT);
                if (batch.messages().isEmpty()) {
                    // A quiet market is not a dead socket; this lets the stall detector tell them apart.
                    send(session, Map.of("type", "heartbeat", "timestamp", clock.instant().toString()));
                    continue;
                }
                for (ExchangeMessage message : batch.messages()) {
                    if (toSkip.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                        continue;
                    }
                    send(session, message);
                }
                cursor = batch.nextCursor();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (QuoteLog.EvictedException e) {
            log.warn("Exchange session {} fell behind the retained window: {}",
                    session.getId(), e.getMessage());
            refuse(session, "CONSUMER_TOO_SLOW", e.getMessage());
        } catch (Exception e) {
            if (session.isOpen()) {
                log.warn("Exchange session {} pump failed: {}", session.getId(), e.toString());
            }
        }
    }

    private void send(WebSocketSession session, Object payload) throws Exception {
        session.sendMessage(new TextMessage(mapper.writeValueAsString(payload)));
    }

    private void refuse(WebSocketSession session, String code, String message) {
        try {
            send(session, Map.of("type", "error", "code", code, "message", message));
            session.close(CloseStatus.NOT_ACCEPTABLE.withReason(code));
        } catch (Exception e) {
            log.debug("Could not refuse exchange session {}: {}", session.getId(), e.toString());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
        Thread pump = pumps.remove(session.getId());
        if (pump != null) {
            pump.interrupt();
        }
    }

    /**
     * Drops every subscriber, simulating an exchange-side outage.
     *
     * <p>Fault injection: a real exchange will not do this on request, so without it the recovery path
     * could only be asserted about, never demonstrated.
     */
    public int disconnectAll() {
        int dropped = 0;
        for (WebSocketSession session : List.copyOf(sessions.values())) {
            try {
                // Abnormal, not a polite goodbye, so the consumer must resume from its checkpoint.
                session.close(CloseStatus.SERVICE_RESTARTED);
                dropped++;
            } catch (Exception e) {
                log.debug("Could not close exchange session {}: {}", session.getId(), e.toString());
            }
        }
        return dropped;
    }

    /**
     * Silently skips the next {@code count} live messages, as a lossy link would, while keeping them
     * in the log so a replay still delivers them.
     *
     * <p>Fault injection for the one failure the socket cannot report: a hole in the sequence with
     * the connection perfectly healthy.
     */
    public void skipNext(int count) {
        toSkip.addAndGet(count);
    }

    private static String checkpointParameter(WebSocketSession session) {
        if (session.getUri() == null) {
            return null;
        }
        String raw = UriComponentsBuilder.fromUri(session.getUri())
                .build().getQueryParams().getFirst("checkpoint_timestamp");
        if (raw == null || raw.isBlank()) {
            return null;
        }
        // getQueryParams() returns values still percent-encoded; decoding is a separate step.
        String value = UriUtils.decode(raw, StandardCharsets.UTF_8);
        return "null".equalsIgnoreCase(value) ? null : value;
    }

    private static Instant parseCheckpoint(String raw) {
        try {
            return Instant.parse(raw);
        } catch (DateTimeParseException e) {
            try {
                return Instant.ofEpochMilli(Long.parseLong(raw.trim()));
            } catch (NumberFormatException ignored) {
                throw e;
            }
        }
    }
}
