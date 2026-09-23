-- Market data store.
--
-- Two quote tables, because the write path and the read path want opposite things. Quotes arrive
-- as an unbounded append-only stream and are asked for, overwhelmingly, as "the latest one for
-- this ISIN". Serving that from the history would mean an ORDER BY ... LIMIT 1 over a table that
-- grows by millions of rows a day, so the current quote is maintained separately as exactly one
-- row per instrument.

-- Full history: append-only, never updated.
--
-- The primary key is what makes consumption idempotent. Recovery after a disconnect necessarily
-- replays messages the service already holds, because the exchange can only rewind to a timestamp
-- and several quotes can share one instant. Rather than trying to make delivery exactly-once,
-- which is not achievable over a reconnecting socket, the write is made repeatable: re-delivering
-- a quote conflicts on this key and is discarded.
CREATE TABLE quote (
    isin          VARCHAR(12)  NOT NULL,
    event_time    TIMESTAMPTZ  NOT NULL,
    sequence      BIGINT       NOT NULL,
    bid           NUMERIC(20, 8),
    ask           NUMERIC(20, 8),
    bid_size      NUMERIC(24, 8),
    ask_size      NUMERIC(24, 8),
    currency      VARCHAR(3)   NOT NULL,
    received_time TIMESTAMPTZ  NOT NULL,
    CONSTRAINT quote_pk PRIMARY KEY (isin, event_time, sequence)
);

COMMENT ON TABLE quote IS
    'Append-only quote history. The primary key deduplicates replayed messages after a recovery.';

-- The leading column of the primary key is the ISIN, so a range scan over one instrument's history
-- is already covered and needs no second index. An index on event_time alone is deliberately
-- absent: nothing queries across all instruments by time, and it would be written to on every
-- insert on the hot path.
--
-- At production volume this table would be partitioned by event_time, which turns retention into
-- detaching a partition rather than a DELETE that has to vacuum behind itself.

-- Latest quote per instrument: what the API serves.
CREATE TABLE latest_quote (
    isin          VARCHAR(12)  NOT NULL,
    event_time    TIMESTAMPTZ  NOT NULL,
    sequence      BIGINT       NOT NULL,
    bid           NUMERIC(20, 8),
    ask           NUMERIC(20, 8),
    bid_size      NUMERIC(24, 8),
    ask_size      NUMERIC(24, 8),
    currency      VARCHAR(3)   NOT NULL,
    received_time TIMESTAMPTZ  NOT NULL,
    updated_at    TIMESTAMPTZ  NOT NULL,
    CONSTRAINT latest_quote_pk PRIMARY KEY (isin)
);

COMMENT ON TABLE latest_quote IS
    'One row per instrument. Writes are guarded so a replayed quote cannot move it backwards.';

-- How far the feed has been consumed and durably stored.
--
-- Written in the same transaction as the quotes it covers, which is the entire gap-free guarantee:
-- a crash between the two would otherwise either lose quotes (checkpoint committed first) or
-- silently advance past them.
CREATE TABLE ingest_checkpoint (
    feed        TEXT         NOT NULL,
    event_time  TIMESTAMPTZ  NOT NULL,
    sequence    BIGINT       NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL,
    CONSTRAINT ingest_checkpoint_pk PRIMARY KEY (feed)
);
