package stockcanyon.storage;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.postgresql.PGConnection;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import stockcanyon.Quote;

/**
 * The ingestion service's datastore: the quote history, and the outbox that publishes each batch.
 *
 * <p>No latest quote is kept here. Each batch publishes the newest quote it holds for every
 * instrument in it, and the distribution service — the only owner of latest-quote state — accepts
 * it only if it is newer than what it has. Replays therefore publish old prices again, on purpose:
 * deciding what is old is the distribution service's job, done once, in one place.
 *
 * <p>A batch is loaded once with {@code COPY} into a transaction-scoped staging table, and both the
 * history and the outbox are written from there by SQL. {@code COPY} is PostgreSQL's bulk path —
 * one stream, no per-row statement — but it cannot express {@code ON CONFLICT}, which idempotent
 * replay depends on. Staging gets both.
 */
public class QuoteRepository {

    /**
     * Temporary, so it is per connection and never visible to another session; emptied at commit,
     * so a pooled connection never carries one batch's rows into the next. Created lazily on each
     * connection the first time it writes. Not {@code LIKE quote}, which would copy NOT NULL
     * constraints that belong to the target, not to the staging step.
     */
    private static final String CREATE_STAGING = """
            CREATE TEMP TABLE IF NOT EXISTS quote_staging (
                isin          VARCHAR(12),
                event_time    TIMESTAMPTZ,
                sequence      BIGINT,
                bid           NUMERIC(20, 8),
                ask           NUMERIC(20, 8),
                bid_size      NUMERIC(24, 8),
                ask_size      NUMERIC(24, 8),
                currency      VARCHAR(3),
                received_time TIMESTAMPTZ
            ) ON COMMIT DELETE ROWS
            """;

    private static final String COPY_STAGING = """
            COPY quote_staging (isin, event_time, sequence, bid, ask, bid_size, ask_size,
                                currency, received_time)
            FROM STDIN WITH (FORMAT csv)
            """;

    /** The primary key is what makes consumption idempotent: a replayed quote conflicts and is dropped. */
    private static final String INSERT_HISTORY = """
            INSERT INTO quote_history (isin, event_time, sequence, bid, ask, bid_size, ask_size,
                               currency, received_time)
            SELECT isin, event_time, sequence, bid, ask, bid_size, ask_size, currency, received_time
            FROM quote_staging
            ON CONFLICT ON CONSTRAINT quote_history_pk DO NOTHING
            """;

    /**
     * The event. Instants are rendered in UTC with a {@code Z}, at the microsecond precision they
     * are stored with, so the consumer parses one fixed format.
     */
    private static final String PAYLOAD = """
            jsonb_build_object(
                    'isin', isin, 'currency', currency,
                    'bid', bid, 'ask', ask, 'bidSize', bid_size, 'askSize', ask_size,
                    'sequence', sequence,
                    'eventTime', to_char(event_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                    'receivedTime', to_char(received_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))
            """;

    /**
     * One event per instrument in the batch: its newest quote there.
     *
     * <p>Coalesced because the consumer only needs the latest, and an instrument that printed 1400
     * times in 200ms should cost one event, not 1400. Not filtered against anything already
     * published: a replayed batch publishes its old prices again, and the distribution service's
     * newer-only write discards them. That keeps a single owner for "what is the latest quote".
     */
    private static final String PUBLISH_NEWEST = """
            INSERT INTO outbox (aggregatetype, aggregateid, type, payload)
            SELECT 'Quote', isin, 'LatestQuote', """ + PAYLOAD + """
            FROM (
                SELECT DISTINCT ON (isin) *
                FROM quote_staging
                ORDER BY isin, event_time DESC, sequence DESC
            ) newest
            """;

    /**
     * Every instrument's newest stored quote, for rebuilding the downstream store from scratch.
     * DISTINCT ON walks the primary key, which leads with the ISIN. A full pass over the history:
     * an operator action, not a hot path.
     */
    private static final String RESEED = """
            INSERT INTO outbox (aggregatetype, aggregateid, type, payload)
            SELECT 'Quote', isin, 'LatestQuote', """ + PAYLOAD + """
            FROM (
                SELECT DISTINCT ON (isin) *
                FROM quote_history
                ORDER BY isin, event_time DESC, sequence DESC
            ) newest
            """;

    /**
     * Same transaction as the inserts above. The rows only ever need to exist in the WAL, and
     * deleting them here keeps the table empty without a sweeper job.
     */
    private static final String CLEAR_OUTBOX = "DELETE FROM outbox";

    private final NamedParameterJdbcTemplate jdbc;
    private final DataSource dataSource;

    public QuoteRepository(NamedParameterJdbcTemplate jdbc, DataSource dataSource) {
        this.jdbc = jdbc;
        this.dataSource = dataSource;
    }

    /**
     * @param inserted rows newly added to the history; the rest were replayed duplicates
     * @param published events written to the outbox, one per instrument in the batch
     */
    public record WriteResult(int inserted, int published) {

        static final WriteResult NOTHING = new WriteResult(0, 0);
    }

    /**
     * Appends a batch to the history, discarding anything already stored, and publishes each
     * instrument's newest quote in it through the outbox.
     *
     * <p>Must run inside a transaction: the staging table empties at commit, and the caller's
     * checkpoint and fence check have to commit or roll back with these rows — the outbox events
     * included.
     */
    public WriteResult writeBatch(List<Quote> quotes) {
        if (quotes.isEmpty()) {
            return WriteResult.NOTHING;
        }
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("writeBatch must run inside a transaction");
        }
        jdbc.getJdbcTemplate().execute(CREATE_STAGING);
        copyIntoStaging(quotes);
        int inserted = jdbc.getJdbcTemplate().update(INSERT_HISTORY);
        int published = jdbc.getJdbcTemplate().update(PUBLISH_NEWEST);
        jdbc.getJdbcTemplate().update(CLEAR_OUTBOX);
        return new WriteResult(inserted, published);
    }

    /**
     * Whether the CDC reader's replication slot exists and still has the WAL it needs. Readable by
     * any role: {@code pg_replication_slots} needs no privilege.
     */
    public boolean replicationSlotReady(String slot) {
        Long ready = jdbc.queryForObject("""
                SELECT count(*) FROM pg_replication_slots
                WHERE slot_name = :slot AND (wal_status IS NULL OR wal_status <> 'lost')
                """, new MapSqlParameterSource("slot", slot), Long.class);
        return ready != null && ready > 0;
    }

    /**
     * Publishes every instrument's newest stored quote again.
     *
     * <p>For when the downstream copy is lost in a way replay cannot fix: the Debezium replication
     * slot was dropped, or the topic was deleted. Always safe, because the distribution service
     * only accepts a quote newer than the one it holds, so events it already has are no-ops.
     *
     * <p>Must run inside a transaction.
     *
     * @return events published
     */
    public int reseedOutbox() {
        int published = jdbc.getJdbcTemplate().update(RESEED);
        jdbc.getJdbcTemplate().update(CLEAR_OUTBOX);
        return published;
    }

    /** Streams the batch as CSV over the transaction's own connection. */
    private void copyIntoStaging(List<Quote> quotes) {
        // The transaction's connection, not a fresh one from the pool: the staging table is
        // per-session and the rows must commit or roll back with everything else.
        Connection connection = DataSourceUtils.getConnection(dataSource);
        try {
            connection.unwrap(PGConnection.class).getCopyAPI()
                    .copyIn(COPY_STAGING, new StringReader(toCsv(quotes)));
        } catch (SQLException | IOException e) {
            throw new DataAccessResourceFailureException("COPY into quote_staging failed", e);
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }

    /**
     * No field can contain a comma, quote or newline — {@code Quote} admits only check-digit-valid
     * ISINs, three-letter currencies, bounded numbers and instants — so nothing needs escaping. An empty unquoted field is
     * NULL in CSV mode, which is what a one-sided book needs.
     *
     * <p>Instants go as ISO-8601 with full nanoseconds and PostgreSQL rounds them to microseconds.
     * Deterministically, so a replayed quote gets the same key as the original and conflicts.
     */
    static String toCsv(List<Quote> quotes) {
        StringBuilder csv = new StringBuilder(quotes.size() * 128);
        for (Quote quote : quotes) {
            csv.append(quote.isin().value()).append(',')
                    .append(quote.eventTime()).append(',')
                    .append(quote.sequence()).append(',');
            decimal(csv, quote.bid()).append(',');
            decimal(csv, quote.ask()).append(',');
            decimal(csv, quote.bidSize()).append(',');
            decimal(csv, quote.askSize()).append(',');
            csv.append(quote.currency()).append(',')
                    .append(quote.receivedTime()).append('\n');
        }
        return csv.toString();
    }

    private static StringBuilder decimal(StringBuilder csv, BigDecimal value) {
        return value == null ? csv : csv.append(value.toPlainString());
    }

}
