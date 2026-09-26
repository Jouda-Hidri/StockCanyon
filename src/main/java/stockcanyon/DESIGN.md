# Design

## 1. Overview

```
 Exchange ─ws─▶ INGESTION ──one transaction──▶ PostgreSQL: quote_history · checkpoint · outbox
                                                            │ WAL (Debezium / Kafka Connect)
                                                            ▼
                                     Kafka marketdata.latest-quote (compacted, key = ISIN)
                                                            │
 internal services ─GET /quotes/{isin}/latest─▶ DISTRIBUTION ◀┘ newer-only write ─▶ Redis
```

| Service | Code | Owns |
| --- | --- | --- |
| Ingestion | `consumption/`, `storage/` | PostgreSQL |
| Distribution | `distribution/` | Redis |
| (Simulated exchange) | `simulator/` | nothing |

One jar; each process enables one role (`marketdata.{consumption,distribution,simulator}.enabled`).
Neither service reads the other's store. The requirement everything serves: consume **without
gaps**, across failures.

## 2. Ingestion flow

```
 socket thread ──put──▶ IngestQueue (50 000) ──drain 2000 / 200ms──▶ writer thread
                          ≥ 40 000: close socket                      │
                          ≤ 10 000: reopen from checkpoint            ▼
 BEGIN
   SELECT term FROM ingest_leader FOR SHARE          -- fenced? abort
   COPY quote_staging FROM STDIN                     -- temp, ON COMMIT DELETE ROWS
   INSERT INTO quote_history ... ON CONFLICT DO NOTHING
   INSERT INTO outbox SELECT DISTINCT ON (isin) ...  -- newest per ISIN in the batch
   DELETE FROM outbox                                -- rows only need to reach the WAL
   UPSERT ingest_checkpoint = contiguous prefix
 COMMIT        failed? retry the same batch with backoff; never dropped
```

- **Resume:** every connection attempt reads the checkpoint from the database and connects with
  `?checkpoint_timestamp=`; the exchange replays from there.
- **Back-pressure:** at the high watermark the socket is *closed*, not just left unread. A push
  stream has no request to refuse (no `503`); not reading only fills TCP buffers on both sides until
  the exchange drops us. Closing leaves the backlog in the exchange's replay log. TCP flow control
  remains the last resort if the queue fills completely.
- **Reconnect:** exponential backoff with jitter (500ms → 30s); none when we closed on purpose. A
  stall timer (15s; heartbeats make silence meaningful) catches sockets that are open but dead.

## 3. Gap-free guarantee

**Enforced** by the transaction: data, event and checkpoint commit together, so a crash cannot land
between them. **Checked** by the exchange's sequence number:

| Arrives (P = contiguous prefix, H = newest seen) | Verdict |
| --- | --- |
| `≤ P` | DUPLICATE (replay overlap; the PK drops it) |
| `P+1`, no hole open | IN_ORDER |
| inside an open hole | BACKFILL |
| `> H+1` | GAP: hole `H+1 .. seq-1` opens |
| `≤ P` but newer in time than anything seen | RESET (exchange renumbered) |

The checkpoint is **P**, never H: given 40, 41, 44 it stays at 41. If the hole is still open after
2s, the socket is closed on purpose — the exchange has no "resend 42", so reconnecting from 41 *is*
the request. (TCP cannot reorder one socket, so a hole means the exchange skipped the message; the
2s only allows for an exchange that sends out of order.) Each replay gets 2s once its data is
actually being read, not merely requested, so a backlog cannot exhaust the attempts. After 3 that do
not fill it, the hole is written off and alerted on. `quotesMissing` = open + written off.

Malformed quotes (bad currency, values beyond the column precision) are rejected at the socket,
costing one sequence number, never a batch. Every ambiguity resolves toward a duplicate: the exchange rewinds inclusively,
`PRIMARY KEY (isin, event_time, sequence)` drops replays, and the checkpoint is truncated, not
rounded, to PostgreSQL's microseconds.

Assumed of the exchange, to confirm against a real one: the rewind is inclusive, the checkpoint is
monotonic, and there is a sequence number.

## 4. Outbox

A direct Kafka send from ingestion would be a second write that can fail or succeed independently
of the commit. The outbox row is part of the transaction; Debezium reads it from the WAL
(publication `marketdata_outbox`), and the Outbox Event Router emits one message per row, key =
`aggregateid` (ISIN). An event exists exactly when its transaction committed.

**The distribution service alone decides what is newer.** Ingestion keeps no latest state and
publishes each batch's newest quote per ISIN, so replays publish old prices again. Redis applies:

```lua
if stored and (et < stored_et or (et == stored_et and seq <= stored_seq)) then return 0 end
HSET key et seq quote
```

So at-least-once delivery is enough everywhere: Debezium restarts, consumer redelivery, replay.
The consumer's position lives in Redis, written in the same MULTI/EXEC as the prices it covers;
Redis replicates asynchronously, so a failover loses position and data *together*, and the consumer
replays from what Redis holds (checked before every batch, and on every partition assignment).
Compaction stays correct: a replay runs forward to the live edge, so each ISIN's last message ends
up its newest again.

Event: `{"isin","currency","bid","ask","bidSize","askSize","sequence","eventTime","receivedTime"}`,
instants in UTC µs; unknown fields ignored.

| Connector setting | Why |
| --- | --- |
| `expand.json.payload=false`, `StringConverter` | exact JSON text; expansion turns `numeric` into doubles |
| `publication.autocreate.mode=disabled` | publication created by migration V3 |
| `snapshot.mode=no_data`, `tombstones.on.delete=false` | the outbox is always empty; its DELETE is not a tombstone |
| `heartbeat.interval.ms=10000` | keeps the slot moving so WAL can be recycled |
| topic created explicitly, compacted; no auto-create (broker and consumer) | an auto-created topic is `delete`, which loses quiet instruments' latest |
| `slot.failover=true` (PostgreSQL 17) | the slot survives failover (§6) |

| Lost | Recovery |
| --- | --- |
| Slot not created yet | ingestion writes nothing until it exists (`require-cdc-slot`) |
| Debezium down | resumes from its slot; retained WAL capped by `max_slot_wal_keep_size` |
| Replication slot invalidated | `POST /actuator/outboxreseed` re-publishes every ISIN's newest quote |
| Redis writes (failover) or all data (wipe) | automatic: replay from Redis's own position, or from the topic's start |
| Redis unreachable | API answers 503 + `Retry-After`; the consumer retries |

## 5. API

`GET /api/v1/marketdata/quotes/{isin}/latest` (distribution): 200; 400 malformed ISIN (check digit
validated); 404 never quoted; 503 store unavailable. `ageMillis` includes the trip through Kafka.
`GET /api/v1/marketdata/status` (ingestion): leader, queue, holes, `quotesMissing`.

## 6. Election and HA

**Election:** Spring Integration `LockRegistryLeaderInitiator` over a `JdbcLockRegistry` row (TTL
10s). **Fencing:** each leader increments `ingest_leader.term`; every write transaction starts with
`SELECT term ... FOR SHARE` and aborts if the term is not ours, so a leader that paused past its lock
cannot write. A run that ends on its own (fenced, failed) yields the lock rather than hold it while
ingesting nothing. Shutdown drains the queue before releasing the lock.

| | JDBC row (used) | K8s Lease | Advisory lock | etcd / Consul |
| --- | --- | --- | --- | --- |
| Extra infra | none | K8s only | none | a cluster |
| Partition-safe | yes | yes | **no** (freed only when the server notices) | yes |
| Survives DB failover | yes (replicated) | n/a | **no** (in memory) | n/a |
| Clock-sensitive | yes (client stamps) | low | no | no |
| Stops ingestion when | DB down (already fatal) | API server down | DB down | etcd down |

Any lock still needs the fence in PostgreSQL; no library can enforce it inside our transaction.

| Component | HA |
| --- | --- |
| Ingestion PostgreSQL | CloudNativePG, 3 instances, **synchronous** replication (the writer trusts a COMMIT); PostgreSQL 17 failover slots kept on standbys for Debezium |
| Kafka | Strimzi, 3 KRaft nodes, RF 3, `min.insync.replicas=2` |
| Kafka Connect | 2 workers, 1 task |
| Redis | 3 nodes + 3 Sentinels; rebuildable from the topic |

## 7. Scaling

Ingestion is one writer (one feed, one sequence); standbys add failover, not throughput. The queue
smooths bursts; the exchange's replay window is the real buffer, and outliving it yields a loud
`CHECKPOINT_TOO_OLD`. Distribution scales on reads: every pod serves Redis, and 12 partitions spread
the writes. Redis load follows instruments that moved, not the quote rate.

## 8. Monitoring

`/actuator/prometheus`; alerts in `deploy/k8s/monitoring.yaml`.

| Area | Key metrics |
| --- | --- |
| Feed | `marketdata_feed_{connected,disconnects_total,exchange_errors_total}` |
| Gaps | `marketdata_sequence_{gaps,lost,outstanding}`, `marketdata_checkpoint_lag_seconds` |
| Queue | `marketdata_queue_{depth,paused,high_watermark_trips_total}` |
| DB | `marketdata_db_write_seconds`, `_retries_total`, `hikaricp_*` |
| Pipeline | `marketdata_outbox_published_total`, `marketdata_projection_{events_total,staleness_seconds}`, consumer lag, slot WAL |
| Leadership | `marketdata_leader`, `_term` |
| API / JVM | `http_server_requests_seconds_*`, `jvm_*`, `process_cpu_usage` |

The `ingestion` health component is excluded from liveness/readiness: restarting the leader would
not fix the exchange or the database.

## 9. Not implemented

- **History retention:** `quote_history` grows forever; partition by `event_time` once a retention
  period is decided.
- **Every tick on Kafka:** events are the newest per ISIN per batch.
- **Topic consumers without their own newer-only check:** would need Debezium exactly-once plus a
  publisher-side filter.

## 10. Verification

| Test | Proves |
| --- | --- |
| `MarketDataRecoveryTest` | disconnect → nothing missing, contiguous history, replay path used |
| `MarketDataFaultTest` | skipped messages backfilled; refused writes retried, queue closes the socket, nothing lost; leadership churn during start-up |
| `StorageTest` | COPY, fencing, election handover, outbox as seen in the WAL |
| `LatestQuoteStoreTest`, `LatestQuoteProjectorTest` | newer-only rule; position stored with data; replay after a Redis wipe or lossy failover |
| `QuoteTest`, `SequenceTrackerTest`, `IngestQueueTest` | boundary validation, gap verdicts, watermarks |
| `MarketDataApiTest` | 200/400/404/503; no database in the distribution service |
| `OutboxEndToEndTest` | PostgreSQL → Debezium → Kafka → Redis → API; replays refused; topic compacted |

The simulated exchange stands in for the unspecified real one: no public feed offers
`checkpoint_timestamp` replay, and none drops connections on request.
