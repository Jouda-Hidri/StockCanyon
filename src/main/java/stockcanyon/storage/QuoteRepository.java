package stockcanyon.storage;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import stockcanyon.Isin;
import stockcanyon.Quote;

/**
 * Stores quotes and reads the latest one back.
 *
 * <p>Two tables: {@code quote} is the append-only history, {@code latest_quote} one row per
 * instrument. Serving "latest" from the history would mean {@code ORDER BY ... LIMIT 1} over a
 * table growing by millions of rows a day.
 *
 * <p>Plain JDBC, not JPA: the hot path needs a conflict-ignoring multi-row insert and a
 * conditional upsert, both of which an ORM gets in the way of.
 */
public class QuoteRepository {

    /** The primary key is what makes consumption idempotent: a replayed quote conflicts and is dropped. */
    private static final String INSERT_HISTORY = """
            INSERT INTO quote (isin, event_time, sequence, bid, ask, bid_size, ask_size,
                               currency, received_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT ON CONSTRAINT quote_pk DO NOTHING
            """;

    /**
     * The WHERE guard is why a recovery cannot corrupt the read path.
     *
     * <p>Replay delivers quotes out of order; without it, last-write-wins would leave the latest quote
     * showing a price from minutes ago. In SQL rather than in Java so it is also correct under
     * concurrency — the upsert locks the row.
     */
    private static final String UPSERT_LATEST = """
            INSERT INTO latest_quote (isin, event_time, sequence, bid, ask, bid_size, ask_size,
                                      currency, received_time, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT ON CONSTRAINT latest_quote_pk DO UPDATE SET
                event_time    = EXCLUDED.event_time,
                sequence      = EXCLUDED.sequence,
                bid           = EXCLUDED.bid,
                ask           = EXCLUDED.ask,
                bid_size      = EXCLUDED.bid_size,
                ask_size      = EXCLUDED.ask_size,
                currency      = EXCLUDED.currency,
                received_time = EXCLUDED.received_time,
                updated_at    = EXCLUDED.updated_at
            WHERE latest_quote.event_time < EXCLUDED.event_time
               OR (latest_quote.event_time = EXCLUDED.event_time
                   AND latest_quote.sequence < EXCLUDED.sequence)
            """;

    private static final String COLUMNS =
            "isin, event_time, sequence, bid, ask, bid_size, ask_size, currency, received_time";

    private final JdbcTemplate jdbc;

    public QuoteRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Appends a batch, discarding anything already stored. */
    public void insertHistory(List<Quote> quotes) {
        if (!quotes.isEmpty()) {
            jdbc.batchUpdate(INSERT_HISTORY, new QuoteBatch(quotes, null));
        }
    }

    /**
     * Moves each instrument's latest quote forward.
     *
     * <p>{@code quotes} must hold at most one entry per ISIN: PostgreSQL rejects an
     * {@code ON CONFLICT DO UPDATE} touching one row twice in a command. See
     * {@code QuoteBatch.coalesceLatest}.
     */
    public void upsertLatest(Collection<Quote> quotes, Instant updatedAt) {
        if (!quotes.isEmpty()) {
            jdbc.batchUpdate(UPSERT_LATEST, new QuoteBatch(List.copyOf(quotes), updatedAt));
        }
    }

    /** The latest quote for an instrument — what the API serves. */
    public Optional<Quote> findLatest(Isin isin) {
        return jdbc.query("SELECT " + COLUMNS + " FROM latest_quote WHERE isin = ?",
                MAPPER, isin.value()).stream().findFirst();
    }

    public long countStoredQuotes() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM quote", Long.class);
        return count == null ? 0 : count;
    }

    public long countInstruments() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM latest_quote", Long.class);
        return count == null ? 0 : count;
    }

    private static final RowMapper<Quote> MAPPER = (rs, rowNum) -> new Quote(
            Isin.of(rs.getString("isin")),
            rs.getLong("sequence"),
            rs.getBigDecimal("bid"),
            rs.getBigDecimal("ask"),
            rs.getBigDecimal("bid_size"),
            rs.getBigDecimal("ask_size"),
            rs.getString("currency"),
            instant(rs, "event_time"),
            instant(rs, "received_time"));

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    /** Binds a quote to either statement; they share a column order. */
    private record QuoteBatch(List<Quote> quotes, Instant updatedAt)
            implements BatchPreparedStatementSetter {

        @Override
        public void setValues(PreparedStatement ps, int i) throws SQLException {
            Quote q = quotes.get(i);
            ps.setString(1, q.isin().value());
            ps.setTimestamp(2, Timestamp.from(q.eventTime()));
            ps.setLong(3, q.sequence());
            setDecimal(ps, 4, q.bid());
            setDecimal(ps, 5, q.ask());
            setDecimal(ps, 6, q.bidSize());
            setDecimal(ps, 7, q.askSize());
            ps.setString(8, q.currency());
            ps.setTimestamp(9, Timestamp.from(q.receivedTime()));
            if (updatedAt != null) {
                ps.setTimestamp(10, Timestamp.from(updatedAt));
            }
        }

        @Override
        public int getBatchSize() {
            return quotes.size();
        }

        private static void setDecimal(PreparedStatement ps, int index, BigDecimal value)
                throws SQLException {
            if (value == null) {
                ps.setNull(index, java.sql.Types.NUMERIC);
            } else {
                ps.setBigDecimal(index, value);
            }
        }
    }
}
