package stockcanyon.storage;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * The fencing token for ingestion: a term, incremented by every new leader.
 *
 * <p>Election (the lock) is what makes one instance <em>try</em> to write. This is what makes the
 * database refuse the others. They are separate because a lock only tells the holder what was true
 * when it last checked.
 */
public class LeadershipRepository {

    private static final String FEED = "exchange";

    private static final String CLAIM = """
            INSERT INTO ingest_leader (feed, term, holder, granted_at)
            VALUES (:feed, 1, :holder, :now)
            ON CONFLICT ON CONSTRAINT ingest_leader_pk DO UPDATE SET
                term       = ingest_leader.term + 1,
                holder     = EXCLUDED.holder,
                granted_at = EXCLUDED.granted_at
            RETURNING term
            """;

    /**
     * FOR SHARE, so a successor's CLAIM waits for this transaction to finish: either the successor
     * increments first and this check fails, or this transaction commits first and its write was
     * still legitimate. There is no interleaving where a stale write lands after a newer term.
     */
    private static final String CURRENT = """
            SELECT term FROM ingest_leader WHERE feed = :feed FOR SHARE
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public LeadershipRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Takes over the feed. Returns the new term, which every subsequent write must carry. */
    public long claim(String holder, Instant now) {
        Long term = jdbc.queryForObject(CLAIM, new MapSqlParameterSource()
                .addValue("feed", FEED)
                .addValue("holder", holder)
                .addValue("now", Timestamp.from(now)), Long.class);
        if (term == null) {
            throw new IllegalStateException("Claiming the feed returned no term");
        }
        return term;
    }

    /**
     * Refuses the enclosing transaction unless {@code term} is still current. Must run inside the
     * transaction whose writes it guards.
     */
    public void assertCurrent(long term) {
        List<Long> rows = jdbc.queryForList(CURRENT, new MapSqlParameterSource("feed", FEED), Long.class);
        long current = rows.isEmpty() ? 0 : rows.getFirst();
        if (current != term) {
            throw new FencedException(term, current);
        }
    }

    /** A newer leader exists. The write must be abandoned, not retried. */
    public static final class FencedException extends RuntimeException {

        public FencedException(long term, long current) {
            super("term " + term + " was superseded by term " + current);
        }
    }
}
