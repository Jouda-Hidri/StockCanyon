package stockcanyon.storage;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import stockcanyon.Isin;
import stockcanyon.Quote;

/**
 * Stores quotes and reads the latest one back.
 *
 * <p>Two tables: {@code quote} is the append-only history, {@code latest_quote} one row per
 * instrument. Serving "latest" from the history would mean an {@code ORDER BY ... LIMIT 1} over a
 * table growing by millions of rows a day.
 *
 * <p>Plain JDBC, not JPA: the hot path needs a conflict-ignoring multi-row insert and a conditional
 * upsert, both of which an ORM gets in the way of.
 */
public class QuoteRepository {

    /** The primary key is what makes consumption idempotent: a replayed quote conflicts and is dropped. */
    private static final String INSERT_HISTORY = """
            INSERT INTO quote (isin, event_time, sequence, bid, ask, bid_size, ask_size,
                               currency, received_time)
            VALUES (:isin, :eventTime, :sequence, :bid, :ask, :bidSize, :askSize,
                    :currency, :receivedTime)
            ON CONFLICT ON CONSTRAINT quote_pk DO NOTHING
            """;

    /**
     * The WHERE guard is why a recovery cannot corrupt the read path.
     *
     * <p>Replay delivers quotes out of order; without it, last-write-wins would leave the latest
     * quote showing a price from minutes ago. In SQL rather than in Java so it is also correct
     * under concurrency — the upsert locks the row.
     */
    private static final String UPSERT_LATEST = """
            INSERT INTO latest_quote (isin, event_time, sequence, bid, ask, bid_size, ask_size,
                                      currency, received_time, updated_at)
            VALUES (:isin, :eventTime, :sequence, :bid, :ask, :bidSize, :askSize,
                    :currency, :receivedTime, :updatedAt)
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

    private final NamedParameterJdbcTemplate jdbc;

    public QuoteRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Appends a batch, discarding anything already stored. */
    public void insertHistory(List<Quote> quotes) {
        if (!quotes.isEmpty()) {
            jdbc.batchUpdate(INSERT_HISTORY, parameters(quotes, null));
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
            jdbc.batchUpdate(UPSERT_LATEST, parameters(quotes, updatedAt));
        }
    }

    /** The latest quote for an instrument — what the API serves. */
    public Optional<Quote> findLatest(Isin isin) {
        return jdbc.query("SELECT " + COLUMNS + " FROM latest_quote WHERE isin = :isin",
                new MapSqlParameterSource("isin", isin.value()), MAPPER).stream().findFirst();
    }

    public long countStoredQuotes() {
        return count("SELECT count(*) FROM quote");
    }

    public long countInstruments() {
        return count("SELECT count(*) FROM latest_quote");
    }

    private long count(String sql) {
        Long value = jdbc.queryForObject(sql, new MapSqlParameterSource(), Long.class);
        return value == null ? 0 : value;
    }

    /** Both statements name the same parameters; {@code updatedAt} is null for the history insert. */
    private static SqlParameterSource[] parameters(Collection<Quote> quotes, Instant updatedAt) {
        return quotes.stream()
                .map(quote -> new MapSqlParameterSource()
                        .addValue("isin", quote.isin().value())
                        .addValue("eventTime", Timestamp.from(quote.eventTime()))
                        .addValue("sequence", quote.sequence())
                        .addValue("bid", quote.bid())
                        .addValue("ask", quote.ask())
                        .addValue("bidSize", quote.bidSize())
                        .addValue("askSize", quote.askSize())
                        .addValue("currency", quote.currency())
                        .addValue("receivedTime", Timestamp.from(quote.receivedTime()))
                        .addValue("updatedAt", updatedAt == null ? null : Timestamp.from(updatedAt)))
                .toArray(SqlParameterSource[]::new);
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
}
