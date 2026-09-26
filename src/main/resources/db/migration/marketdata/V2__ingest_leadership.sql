-- Leader election for ingestion. The table Spring Integration's JdbcLockRegistry expects, verbatim
-- from spring-integration-jdbc's schema-postgresql.sql. Exactly one instance holds the row for the
-- feed; the others wait for it to expire.
CREATE TABLE int_lock (
    lock_key     CHAR(36)     NOT NULL,
    region       VARCHAR(100) NOT NULL,
    client_id    CHAR(36),
    created_date TIMESTAMP    NOT NULL,
    CONSTRAINT int_lock_pk PRIMARY KEY (lock_key, region)
);

-- The fencing token. The lock decides who *should* write; this decides whose writes are *accepted*.
--
-- A leader can lose its lock without knowing -- a thirty-second GC pause, a partition -- and wake up
-- mid-flush after another instance has taken over. Each new leader increments the term, and every
-- flush checks its own term under a share lock in the same transaction as the data, so a deposed
-- leader's write is refused by the database rather than trusted to a flag in its own memory.
CREATE TABLE ingest_leader (
    feed        TEXT         NOT NULL,
    term        BIGINT       NOT NULL,
    holder      TEXT         NOT NULL,
    granted_at  TIMESTAMPTZ  NOT NULL,
    CONSTRAINT ingest_leader_pk PRIMARY KEY (feed)
);
