package stockcanyon.storage;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

import stockcanyon.Checkpoint;

/**
 * Persists how far the feed has been consumed.
 *
 * <p>Nothing here starts its own transaction, and that is the point: {@link #save} must commit
 * together with the quotes it describes, so the caller owns the transaction boundary and this
 * class only contributes a statement to it. A checkpoint that committed independently could
 * advance past quotes whose insert then rolled back — precisely the gap the design exists to
 * prevent.
 */
public class CheckpointRepository {

    /** One row, one feed. The constant keeps the schema honest about that. */
    private static final String FEED = "exchange";

    private static final String UPSERT = """
            INSERT INTO ingest_checkpoint (feed, event_time, sequence, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT ON CONSTRAINT ingest_checkpoint_pk DO UPDATE SET
                event_time = EXCLUDED.event_time,
                sequence   = EXCLUDED.sequence,
                updated_at = EXCLUDED.updated_at
            WHERE ingest_checkpoint.event_time < EXCLUDED.event_time
               OR (ingest_checkpoint.event_time = EXCLUDED.event_time
                   AND ingest_checkpoint.sequence < EXCLUDED.sequence)
            """;

    private final JdbcTemplate jdbc;

    public CheckpointRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** The resume point, or {@link Checkpoint#none()} on a cold start. */
    public Checkpoint load() {
        List<Checkpoint> rows = jdbc.query(
                "SELECT event_time, sequence, updated_at FROM ingest_checkpoint WHERE feed = ?",
                (rs, rowNum) -> new Checkpoint(
                        rs.getTimestamp("event_time").toInstant(),
                        rs.getLong("sequence"),
                        rs.getTimestamp("updated_at").toInstant()),
                FEED);
        return rows.isEmpty() ? Checkpoint.none() : rows.getFirst();
    }

    /**
     * Advances the checkpoint, never retreats it.
     *
     * <p>A batch written out of order during replay would otherwise rewind the checkpoint and make
     * the next reconnect replay ground already covered — not incorrect, but an unbounded amount of
     * repeated work that grows the longer the service runs.
     */
    public void save(Instant eventTime, long sequence, Instant updatedAt) {
        jdbc.update(UPSERT, FEED, Timestamp.from(truncate(eventTime)), sequence,
                Timestamp.from(updatedAt));
    }

    /**
     * Rounds the checkpoint down to the microsecond PostgreSQL can actually store.
     *
     * <p>{@code TIMESTAMPTZ} holds microseconds while {@code Instant} holds nanoseconds, so
     * something has to give up the remainder — and which way decides whether this service can lose
     * data. Rounding to nearest would sometimes land the checkpoint a few hundred nanoseconds
     * <em>after</em> the quote it marks, and since the exchange resumes at the first message at or
     * after the checkpoint, every quote in that sub-microsecond window would be skipped: a real
     * gap, small, intermittent, and invisible to anything but the sequence check.
     *
     * <p>Truncating always moves the checkpoint earlier instead. The cost is at most one extra
     * microsecond of replay, which deduplication absorbs.
     */
    private static Instant truncate(Instant eventTime) {
        return eventTime.truncatedTo(ChronoUnit.MICROS);
    }
}
