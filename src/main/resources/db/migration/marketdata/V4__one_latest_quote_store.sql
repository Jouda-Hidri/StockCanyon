-- The ingestion service no longer keeps a latest quote per instrument. The distribution service's
-- store is the only one: it receives the newest quote per instrument from every batch through the
-- outbox, and it alone decides whether that is newer than what it holds. A second copy here would
-- be the same state kept twice, by two services, with two chances to disagree.
DROP TABLE latest_quote;

-- The name the design uses: this is the history, and nothing else is.
ALTER TABLE quote RENAME TO quote_history;
ALTER TABLE quote_history RENAME CONSTRAINT quote_pk TO quote_history_pk;
