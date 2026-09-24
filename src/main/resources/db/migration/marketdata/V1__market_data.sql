-- Append-only history.
--
-- The primary key is what makes consumption idempotent: recovery replays messages already stored,
-- because the exchange can only rewind to a timestamp and several quotes can share one instant.
-- Delivery stays at-least-once and the write is made repeatable instead.
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

-- The ISIN leads the primary key, so one instrument's history is already covered; an index on
-- event_time alone is deliberately absent, since nothing queries across instruments by time and it
-- would be written on every insert. At volume this table would be partitioned by event_time.

-- Latest quote per instrument: what the API serves. Kept separate because serving it from the
-- history would mean ORDER BY ... LIMIT 1 over a table growing by millions of rows a day.
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

-- How far the feed has been consumed. Written in the same transaction as the quotes it covers,
-- which is the entire gap-free guarantee: a crash between the two would otherwise lose quotes or
-- silently advance past them.
--
-- A separate row rather than derived from quote (max(sequence) would equal it today), because the
-- checkpoint must outlive the data it certifies: retention that prunes old history must not move
-- the resume position backwards. It is also the only "newest overall" lookup, and the quote PK
-- leads with isin -- deriving it would need a global index paid on every insert.
CREATE TABLE ingest_checkpoint (
    feed        TEXT         NOT NULL,
    event_time  TIMESTAMPTZ  NOT NULL,
    sequence    BIGINT       NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL,
    CONSTRAINT ingest_checkpoint_pk PRIMARY KEY (feed)
);
