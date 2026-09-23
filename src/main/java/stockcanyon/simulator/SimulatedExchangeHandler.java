package stockcanyon.simulator;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Serves {@code /exchange/quotes?checkpoint_timestamp=}, the endpoint described in the
 * requirements.
 *
 * <p>An absent, blank or {@code null} checkpoint means "stream from now". Otherwise the value is an
 * ISO-8601 instant (or epoch milliseconds) and the stream opens with everything published at or
 * after it, then continues live with no seam between the two.
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
            // The consumer asked for data no longer held. Refusing loudly is the only honest
            // answer: silently fast-forwarding to the live edge would hand them a gap they had no
            // way of detecting, which is precisely the failure the checkpoint exists to prevent.
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
                    // A quiet instant is not a dead socket. Saying so lets the consumer's stall
                    // detector tell the two apart instead of reconnecting whenever a market is slow.
                    send(session, Map.of("type", "heartbeat", "timestamp", clock.instant().toString()));
                    continue;
                }
                for (ExchangeMessage message : batch.messages()) {
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
     * Drops every live subscriber, simulating an exchange-side outage.
     *
     * <p>Fault injection, and the reason the simulator earns its place: a real exchange will not
     * drop your connection on request, so without this the recovery path can only be asserted
     * about, never demonstrated.
     */
    public int disconnectAll() {
        int dropped = 0;
        for (WebSocketSession session : List.copyOf(sessions.values())) {
            try {
                // An abnormal status rather than a polite goodbye, so the consumer has to treat it
                // as a failure and resume from its own checkpoint.
                session.close(CloseStatus.SERVICE_RESTARTED);
                dropped++;
            } catch (Exception e) {
                log.debug("Could not close exchange session {}: {}", session.getId(), e.toString());
            }
        }
        return dropped;
    }

    public int liveSessions() {
        return sessions.size();
    }

    private static String checkpointParameter(WebSocketSession session) {
        var uri = session.getUri();
        if (uri == null || uri.getQuery() == null) {
            return null;
        }
        for (String pair : uri.getQuery().split("&")) {
            int equals = pair.indexOf('=');
            String key = equals < 0 ? pair : pair.substring(0, equals);
            if (!"checkpoint_timestamp".equals(key)) {
                continue;
            }
            String value = equals < 0 ? ""
                    : URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8);
            return value.isBlank() || "null".equalsIgnoreCase(value) ? null : value;
        }
        return null;
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
