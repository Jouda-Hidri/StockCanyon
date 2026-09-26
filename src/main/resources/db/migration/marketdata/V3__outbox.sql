-- The transactional outbox: how a latest-quote change leaves this service's database.
--
-- Rows are inserted in the same transaction as the quotes, the checkpoint and the fence check, and
-- deleted again before it commits. The table is always empty; what matters is the INSERT in the
-- write-ahead log, which Debezium reads through the publication below and turns into one Kafka
-- message per row (the Outbox Event Router ignores the DELETE). So an event exists exactly when
-- its transaction committed -- never for a rollback, never missing for a commit -- with no second
-- write to Kafka that could fail on its own.
--
-- Column names are the Outbox Event Router's defaults: aggregateid becomes the Kafka key, which
-- partitions by ISIN and keeps each instrument's events in order.
CREATE TABLE outbox (
    id            UUID  NOT NULL DEFAULT gen_random_uuid(),
    aggregatetype TEXT  NOT NULL,
    aggregateid   TEXT  NOT NULL,
    type          TEXT  NOT NULL,
    payload       JSONB NOT NULL,
    CONSTRAINT outbox_pk PRIMARY KEY (id)
);

-- Created here rather than by Debezium, so the connector's database user needs no ownership of
-- the table (publication.autocreate.mode=disabled). Requires wal_level=logical to carry changes;
-- without it PostgreSQL still creates the publication and warns.
CREATE PUBLICATION marketdata_outbox FOR TABLE outbox;
