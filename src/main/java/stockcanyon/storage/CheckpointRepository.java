package stockcanyon.storage;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import stockcanyon.Checkpoint;

/**
 * Persists how far the feed has been consumed.
 *
 * <p>Starts no transaction of its own: {@link #save} must commit with the quotes it describes, so
 * the caller owns the boundary. A checkpoint committing independently could advance past quotes
 * whose insert then rolled back.
 */
public class CheckpointRepository {

    /** One row, one feed. */
    private static final String FEED = "exchange";

    private static final String UPSERT = """
            INSERT INTO ingest_checkpoint (feed, event_time, sequence, updated_at)
            VALUES (:feed, :eventTime, :sequence, :updatedAt)
            ON CONFLICT ON CONSTRAINT ingest_checkpoint_pk DO UPDATE SET
                event_time = EXCLUDED.event_time,
                sequence   = EXCLUDED.sequence,
                updated_at = EXCLUDED.updated_at
            WHERE ingest_checkpoint.event_time < EXCLUDED.event_time
               OR (ingest_checkpoint.event_time = EXCLUDED.event_time
                   AND ingest_checkpoint.sequence < EXCLUDED.sequence)
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public CheckpointRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** The resume point, or {@link Checkpoint#none()} on a cold start. */
    public Checkpoint load() {
        List<Checkpoint> rows = jdbc.query(
                "SELECT event_time, sequence, updated_at FROM ingest_checkpoint WHERE feed = :feed",
                new MapSqlParameterSource("feed", FEED),
                (rs, rowNum) -> new Checkpoint(
                        rs.getTimestamp("event_time").toInstant(),
                        rs.getLong("sequence"),
                        rs.getTimestamp("updated_at").toInstant()));
        return rows.isEmpty() ? Checkpoint.none() : rows.getFirst();
    }

    /**
     * Advances the checkpoint, never retreats it.
     *
     * <p>Otherwise an out-of-order batch would rewind it and make the next reconnect replay ground
     * already covered — unbounded repeated work that grows the longer the service runs.
     */
    public void save(Instant eventTime, long sequence, Instant updatedAt) {
        jdbc.update(UPSERT, new MapSqlParameterSource()
                .addValue("feed", FEED)
                .addValue("eventTime", Timestamp.from(truncate(eventTime)))
                .addValue("sequence", sequence)
                .addValue("updatedAt", Timestamp.from(updatedAt)));
    }

    /**
     * Rounds down to the microsecond PostgreSQL stores.
     *
     * <p>{@code Instant} holds nanoseconds. Rounding to nearest would sometimes land the checkpoint
     * <em>after</em> the quote it marks, and since the exchange resumes at the first message at or
     * after it, everything in that sub-microsecond window would be skipped — a real gap, intermittent
     * and invisible except to the sequence check. Truncating costs at most one microsecond of replay.
     */
    private static Instant truncate(Instant eventTime) {
        return eventTime.truncatedTo(ChronoUnit.MICROS);
    }
}
