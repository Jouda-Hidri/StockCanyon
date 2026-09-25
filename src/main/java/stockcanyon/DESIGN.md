# Market data service — design and flow

Three packages, one per feature in the brief:

| Package | Brief |
| --- | --- |
| `consumption/` | *"Consumes messages from the Stock Exchange using a WebSocket connection... includes a fallback mechanism to ensure uninterrupted message consumption without gaps."* |
| `storage/` | *"Store the consumed data in a reliable database for future retrieval."* |
| `distribution/` | *"Expose the consumed data to other internal services through API."* |

Plus `simulator/`, which is **not part of the service** — see [§7](#7-why-there-is-a-simulator).

The hard part is one clause: consume **without any gaps**, across network failures. Reconnecting is
easy; reconnecting without losing what was published while you were away is the problem the whole
design is arranged around.

---

## 1. Layout

```
  marketdata/
    Isin.java              shared model -- validated, check digit included
    Quote.java             shared model
    Checkpoint.java        shared model -- how far we have consumed
    MarketDataConfig       wiring
    MarketDataProperties   settings

    consumption/           FEATURE 1
      ExchangeWebSocketClient   reconnect loop + stall timer -- hand-written:
                                no mainstream WS client auto-reconnects
                                (protocol = JDK, parsing = Jackson)
      QuoteBatch                domain rules over JDK collectors: which quote
                                wins, what key defines a duplicate
      SequenceTracker           ~15 lines of gap verdicts; no lib knows them
      QuoteConsumer             batching (Reactor bufferTimeout is the lib
                                shape; not worth the paradigm here) + the
                                transaction (Spring TransactionTemplate)

    storage/               FEATURE 2
      QuoteRepository           quote (history) + latest_quote (projection)
      CheckpointRepository      ingest_checkpoint

    distribution/          FEATURE 3
      QuoteController           GET /quotes/{isin}/latest, GET /status
      QuoteResponse             wire DTO

    simulator/             NOT the service -- stands in for the exchange
```

---

## 2. Components

```
                    STOCK EXCHANGE
                    ws://.../quotes?checkpoint_timestamp=
                            |
                            |  WebSocket frames
                            v
  +----------------------------------------------------------------+
  |  consumption/                                                   |
  |                                                                 |
  |    ExchangeWebSocketClient                                      |
  |      . connect / reconnect with backoff                         |
  |      . stall detection (a silent socket has no error)           |
  |      . parse frame -> Quote                                     |
  |              |                                                  |
  |              v                                                  |
  |    QuoteConsumer -------> SequenceTracker                       |
  |         |                   gap detection                       |
  |         |  batches every 200ms or 2000 quotes, then writes      |
  |         |  synchronously -- no queue, no writer thread          |
  +---------|------------------------------------------------------+
            |
            |  ONE TRANSACTION: quote + latest_quote + checkpoint
            v
  +----------------------------------------------------------------+
  |  storage/                                                       |
  |                                                                 |
  |    QuoteRepository        quote         (append-only history)   |
  |                           latest_quote  (one row per instrument)|
  |    CheckpointRepository   ingest_checkpoint                     |
  +----------------------------------------------------------------+
            |
            v
  +----------------------------------------------------------------+
  |  distribution/                                                  |
  |                                                                 |
  |    QuoteController                                              |
  |      GET /api/v1/marketdata/quotes/{isin}/latest                |
  |      GET /api/v1/marketdata/status                              |
  +----------------------------------------------------------------+
```

### The one loop that matters

Everything above is a straight line except this, and it is what makes recovery possible — the
resume position travels **from the database back to the socket**:

```
    ingest_checkpoint  (table)
            |
            |  read on EVERY connection attempt
            v
    ?checkpoint_timestamp=2026-09-23T14:20:36.744618Z
            |
            v
      STOCK EXCHANGE  -- replays from there, then continues live
            |
            |  quotes
            v
      QuoteConsumer  -- commits data AND checkpoint together
            |
            +---------> back to ingest_checkpoint
```

---

## 3. The consumption flow, step by step

```
 1  QuoteConsumer.start()
      |
      +--> exchange.start(this, checkpoints::load)
                                        |
                                        +-- a SUPPLIER, not a value.
                                            Re-read on every attempt, so a
                                            reconnect resumes from what is on
                                            disk NOW, not from where this
                                            process started.

 2  supervisor loop                        ExchangeWebSocketClient.supervise()
      |
      +--> Checkpoint resume = resumePoint.get()
      |
      +--> uriFor(resume)
      |       +-- present -> /quotes?checkpoint_timestamp=<T>
      |       +-- absent  -> /quotes           (cold start = "from now")
      |
      +--> buildAsync(uri, session)          JDK HttpClient WebSocket
      |
      +--> awaitFailure(...)   blocks until close, error, or stall

 3  a frame arrives                         ExchangeWebSocketClient.Session
      |
      +--> touch()                  reset the stall timer
      +--> partial.append(data)     frames can be fragmented
      +--> handleFrame(...)         MAY BLOCK on the write below
      +--> webSocket.request(1)     ONLY NOW, after acceptance

 4  frame -> Quote
      |
      +-- "quote"     -> sink.accept(toQuote(node))
      +-- "heartbeat" -> ignored; its only job is to make silence mean something
      +-- "error"     -> logged (CHECKPOINT_TOO_OLD / CONSUMER_TOO_SLOW)

 5  accumulate                              QuoteConsumer.accept()
      |
      +--> pending.add(quote)
      +--> flush when 2000 quotes or 200ms have passed
           (a heartbeat also ticks this, so a quiet market still flushes)

 6  write                                   QuoteConsumer.flush()
      |
      +--> dedupeByKey()            drop replayed rows sharing a primary key
      +--> coalesceLatest()         at most one quote per ISIN
      +--> highWaterMark()          newest by (event_time, sequence)
      |
      +--> BEGIN
      |      INSERT INTO quote ... ON CONFLICT DO NOTHING
      |      UPSERT latest_quote      (guarded: only if newer)
      |      UPSERT ingest_checkpoint
      |    COMMIT
```

The write is synchronous, on the socket's own thread. `request(1)` comes after the quote has been
accepted, so while a batch is being written no further frames are requested and the exchange is
simply not read. A queue and a writer thread would only have smoothed that — nothing the brief
asks for — at the cost of a second thread, a shutdown drain and the bugs that come with both.

---

## 4. The gap-free guarantee

### Why it is enforceable

```
  +----------------------------------------------------+
  |  ONE TRANSACTION                                   |
  |                                                    |
  |    INSERT quote               the data             |
  |    UPSERT latest_quote        the projection       |
  |    UPSERT ingest_checkpoint   <-- the position     |
  |                                   describing it    |
  +----------------------------------------------------+

  There is no window between "the quotes are durable" and
  "the position describing them is durable".
```

| Crash point | Result |
| --- | --- |
| Between writing quotes and saving the checkpoint | Impossible — there is no between |
| After the commit | The checkpoint names exactly what survived; resume misses nothing |
| Mid-batch, transaction rolls back | Checkpoint unchanged, so the same window is replayed |

### Why it is *checkable*

The transaction makes gaps unlikely. It does not make them **observable** — and "no messages were
lost" is not something the consumer can see. A service can reconnect flawlessly, log nothing
alarming, show a healthy connection and a climbing quote count, and still have missed ten thousand
messages. Nothing in that picture contradicts the loss.

The exchange's **sequence number** is what makes it observable: a counter issued with every message,
incrementing by exactly one. A hole is proof of loss; no hole is proof there was none.

```
                        first message
                              |
                              v
                           [FIRST]
                              |
                              v
        +---------------> [IN_ORDER] <--------------+
        |                     |                     |
        |              seq == last + 1              |
        |                     |                     |
   seq <= last                |            seq > last + 1
        |                     |                     |
        v                     |                     v
   [DUPLICATE]                |                  [GAP]
   replay overlap;            |                  real loss;
   count it, do NOT           |                  count the missing,
   move the high-water        |                  DO advance the mark
   mark                       |                     |
        |                     |                     |
        +---------------------+---------------------+
```

The checkpoint is the **contiguous prefix**, not the newest sequence seen. Given 40, 41, 43 the safe
position is 41 — resuming there makes the exchange replay 42 and 43, and the primary key discards
43 as a duplicate. Checkpointing 43 would detect the hole and then make it permanent, because
nothing would ever ask for 42 again. Quotes are still written as they arrive; only the position
lags.

```
   received:   40  41  __  43  44
                       ^
                       hole
   written:    40  41      43  44     all of it -- idempotent
   checkpoint: 41                     resume here, replay 42..44
```

The prefix needs a bound. If the exchange ever legitimately skips a number, a strict rule stalls
forever: the position never passes the hole, every reconnect replays more, and the held-ahead set
grows without limit. After 10 000 pending sequences the hole is written off and recorded.

`quotesMissing` on `GET /status` is that count. It is the number that answers the requirement.

### Recovery, on a timeline

```
  t+0s   streaming normally ....... checkpoint advancing with every commit
  t+10s  X  connection dies -- OR goes silent with no close frame at all
  t+25s  stall timer fires after 15s of silence
            ^ the only thing that catches a wedged-but-open socket
  t+25s  wait the fixed reconnect delay
  t+26s  read the checkpoint FRESH from Postgres  ->  eventTime=T, sequence=N
  t+26s  connect  ?checkpoint_timestamp=T
  t+26s  exchange replays from T, then continues live -- no seam
  t+27s  messages <= N arrive again  ->  discarded by PRIMARY KEY
         messages >  N  ->  exactly what would otherwise have been lost
```

### What to ask the exchange before trusting any of this

Three questions decide whether the scheme above is sound. All are about the contract, not the code:

1. **Is the boundary `>=` or `>`?** Inclusive means resuming at the checkpoint re-delivers the
   boundary quotes and loses none. Exclusive means anything sharing that instant is skipped.
2. **Is `checkpoint_timestamp` a stream cursor that happens to look like a time, or literally the
   quote's event time?** A cursor is monotonic and the design is safe. An event time is not
   necessarily monotonic, and then ordering, ties and late arrivals all become correctness
   concerns.
3. **Is there a sequence number?** Recovery and gap *detection* are different problems. A timestamp
   checkpoint recovers from failures you noticed; only a counter proves nothing was missed
   silently.

This implementation assumes inclusive, ordered, and sequenced — which the simulated exchange is.
Against a real feed those are the first things to confirm.

---

## 5. Duplicates are the deliberate outcome

`checkpoint_timestamp` takes an **instant**, and an instant is not a unique address — several quotes
can share one, especially under load. Resuming is therefore approximate, and there are only two ways
to be wrong:

```
   quotes at the boundary instant T:

        ... q47  q48  q49   q50  q51  q52 ...
                       ^     ^    ^
                       |-- all stamped T --|
                       |
                  checkpoint = T

   resume AFTER T   ->  q50, q51, q52 skipped ....... A GAP
   resume AT/BEFORE ->  q50, q51, q52 re-sent ....... duplicates
```

Every ambiguity resolves toward the duplicate, because a duplicate is a problem the database solves
for free and a gap is one nothing can reconstruct. That appears in three places:

1. **The exchange rewinds inclusively** — `QuoteLog.cursorAtOrAfter`.
2. **`PRIMARY KEY (isin, event_time, sequence)`** — a replayed message conflicts with itself and is
   dropped. Delivery stays at-least-once and the *write* is made idempotent, rather than attempting
   exactly-once delivery, which is not achievable over a reconnecting socket.
3. **The checkpoint is truncated, not rounded.** PostgreSQL stores microseconds, `Instant` holds
   nanoseconds. Rounding would sometimes land the checkpoint *after* the quote it marks; truncation
   always moves it earlier. Cost: at most one microsecond of replay.

---

## 6. The three failure modes a reconnect must handle

```
  (a) socket errors or closes
        -> onError / onClose fires -> reconnect after backoff

  (b) socket stays OPEN and goes SILENT                    <-- the hard one
        -> no error, no close. TCP is perfectly satisfied.
           A dropped route or wedged peer simply never speaks again.
        -> caught only by the stall timer (15s)
        -> this is WHY the exchange sends heartbeats:
           they make silence mean something

  (c) peer accepts, then immediately drops
        -> the fixed retry delay bounds the loop either way
```

Retries wait a fixed second, so a refusing exchange is not hammered in a tight loop. That is all the
brief needs; see *Deliberately not implemented* below for when it stops being enough.

Reconnection is driven from **one supervisor loop**, not from socket callbacks — an error and a close
for the same failure each fire a callback, and acting on both opens two live sockets, which stays
invisible until it doubles every quote in the store.

---

## 7. Why there is a simulator

The brief says *"Utilize the provided Stock Exchange API endpoint, `/quotes?checkpoint_timestamp=`"*.
No endpoint was actually provided, so `simulator/` implements that contract in order to have
something to consume. Without it nothing runs.

It earns its place twice over:

- **No public market data feed offers `checkpoint_timestamp` replay.** Alpaca, Yahoo, Finnhub and
  Binance all stream from the moment you subscribe. Replay *is* the fallback mechanism, so a real
  feed could not exercise it.
- **No real exchange will drop your connection on request.**
  `SimulatedExchangeHandler.disconnectAll()` is what lets the recovery path be *demonstrated* rather
  than merely asserted about.

**If you have the real endpoint**, point `marketdata.exchange-url` at it and set
`marketdata.simulator.enabled=false`. Nothing in `consumption/`, `storage/` or `distribution/`
changes — the simulator sits on the far side of a WebSocket, not at a seam inside the service.

Internally, replay and live streaming are the same operation: a reader holds a cursor and pulls
forward, and one starting in the past simply has further to travel. That removes a race the obvious
"replay the backlog, then subscribe" design has, where quotes published between the end of the
replay and the start of the subscription fall into the seam. It appears only under load, which is
the worst time to find it.

---

## 8. Uneven arrival rates

The brief notes one instrument may print X times a second while another prints Y, either possibly
zero. In the simulated feed the busiest instrument outpaces the quietest by roughly 100:1.

```
   one flush -- 2000 accumulated quotes
   +-------------------------+
   |  AAPL  x 1400           |
   |  MSFT  x  480           |
   |  NVDA  x    1           |
   +-------------------------+
             |
             +--------------------------+
             |                          |
             v                          v
    INSERT INTO quote          UPSERT latest_quote
    2000 rows                  3 rows
    (full history kept)        (one per instrument that MOVED --
                                whether it moved once or 1400 times)
```

A flush costs one latest-quote write per instrument that **moved**. The quiet instrument is
unaffected either way, which is the property that matters: a busy neighbour must not delay it.

Coalescing is also a **correctness requirement**, not only an optimisation. PostgreSQL rejects an
`ON CONFLICT DO UPDATE` that would touch one row twice in a single command, so an uncoalesced batch
holding two quotes for the same ISIN fails the entire write.

---

## 9. Storage

```
  quote                          latest_quote               ingest_checkpoint
  -------------------------      --------------------       --------------------
  isin          -+               isin        PK             feed          PK
  event_time     +- PRIMARY      event_time                 event_time
  sequence      -+   KEY         sequence                   sequence
  bid, ask                       bid, ask                   updated_at
  bid_size, ask_size             bid_size, ask_size
  currency                       currency
  received_time                  received_time
                                 updated_at
  append-only,
  never updated                  exactly one row
                                 per instrument
```

Two quote tables because the write path and the read path want opposite things. Quotes arrive as an
unbounded append-only stream and are asked for almost entirely as "the latest one for this ISIN" —
serving that from the history would mean `ORDER BY ... LIMIT 1` over a table growing by millions of
rows a day.

`latest_quote` carries a guard so a replay cannot move it backwards:

```sql
WHERE latest_quote.event_time < EXCLUDED.event_time
   OR (latest_quote.event_time = EXCLUDED.event_time
       AND latest_quote.sequence < EXCLUDED.sequence)
```

Without it, last-write-wins would leave the latest quote showing a price from several minutes ago
with nothing to indicate it. Evaluating the comparison in SQL rather than in the service also makes
it correct under concurrency — the row is locked by the upsert itself, so two writers cannot both
read an old value and both conclude they are newer.

Plain JDBC rather than JPA: the two operations that matter are a multi-row insert that ignores
conflicts and a conditional upsert, both things an ORM gets in the way of.

Why Postgres and not a document store, given there are no joins? Because joins were never the
criterion — atomicity scope is. The invariant spans three tables in one transaction; MongoDB's
natural atomicity unit is one document, and its multi-document transactions are the exception path
(snapshot isolation, WriteConflict retries) that we would be running on every flush. The guarded
upsert is also native here, where Mongo's filter-plus-upsert has a documented duplicate-key race.
And the data is maximally relational: fixed columns, flat, no nesting — there is no document.

---

## 10. Distribution

| Endpoint | Purpose |
| --- | --- |
| `GET /api/v1/marketdata/quotes/{isin}/latest` | **The endpoint the brief asks for.** |
| `GET /api/v1/marketdata/status` | Consumption state, including `quotesMissing` |

```
  GET /quotes/{isin}/latest
        |
        v
   valid ISIN?  (length, charset, CHECK DIGIT)
        |
    no  +--> 400   a transposed pair still matches the pattern but
        |          names a different instrument, or none
   yes  |
        v
   SELECT FROM latest_quote
        |          |
     found       none
        |          |
        v          v
       200        404
```

404 rather than an empty 200 is deliberate: a caller that cannot distinguish "no price" from "a price
of nothing" will eventually treat one as the other, and in a pricing path that is the expensive kind
of mistake. A malformed ISIN is 400 rather than 404 because the two need different client behaviour —
one will never succeed however often it is retried, the other may resolve on its own.

ISINs are validated including the **check digit**, not merely matched against a pattern. A transposed
pair of characters still matches `[A-Z]{2}[A-Z0-9]{9}[0-9]` and still looks like an identifier — but
it names a different instrument, or none.

Read-only. Quotes enter from the exchange and nowhere else, so there is no write endpoint to secure,
rate-limit or make idempotent.

---

## 11. Settings that matter

| Property | Default | Why |
| --- | --- | --- |
| `consumption.max-batch-size` | 2 000 | Bounds one transaction |
| `consumption.flush-interval` | 200ms | Latency/throughput dial — bounds staleness, sets how much a commit amortises |
| `consumption.stall-timeout` | 15s | Must exceed the heartbeat interval, or a quiet market reads as a dead socket |
| `consumption.reconnect-delay` | 1s | Fixed wait between reconnect attempts |
| `simulator.skew` | 1.1 | Zipf exponent — busiest instrument ~100x the quietest |

---

## 12. Scaling boundary

The service persists quotes synchronously while consuming the stream, so ingestion throughput is
coupled to database write throughput. That is deliberate and it is simple, and it holds while the
feed is slower than the database.

It stops holding when either of two things is true, and both are numbers to measure rather than
guess:

- sustained exchange throughput exceeds what the database can commit, or
- the database outage we must tolerate exceeds the exchange's replay retention.

Until then, a bounded in-memory buffer is the wrong answer: at 100k quotes/s a quote costs roughly
400 bytes, so buffering buys `size / 100k` seconds — about 1.2 GB to ride out thirty seconds, 24 GB
for ten minutes. You cannot heap your way out of a slow database.

What holds the data instead is the exchange. Stop consuming, let the connection fail, and reconnect
from the checkpoint: the replay window *is* the buffer, and memory stays bounded. If the outage
outlives that window the reconnect is refused with `CHECKPOINT_TOO_OLD` — a detected gap rather than
a silent one, which is the point of refusing loudly.

Past that boundary the next component is a durable log we own, and the checkpoint changes meaning
with it: it would certify the last quote durably appended to the log, not the last committed to the
database. Consumers updating the latest quote and the history then run independently behind it.
Note the cost — appending to the log and advancing the exchange checkpoint are no longer one
transaction, so that edge falls back to append-then-checkpoint plus idempotency.

Why not Kafka *as* the database, given the schema is already Kafka-shaped (history = topic,
latest_quote = compacted topic, checkpoint = __consumer_offsets)? Because the required API is a
point read by key, and Kafka reads only by (partition, offset) — serving it means materializing a
state store, i.e. rebuilding a database on top. And because a log cannot reject a duplicate: the
whole prefer-duplicates recovery strategy stays cheap only while the store is idempotent via the
primary key. Kafka enters as the log beside the database, never instead of it.

A cheaper step usually comes first: the two writes have opposite requirements and need not share a
transaction. `latest_quote` is small, must be fresh, and coalescing already makes it scale with the
number of instruments rather than the quote rate. `quote` history is enormous and nothing reads it
synchronously. Splitting them often removes the problem without adding a system.

---

## 13. Deliberately not implemented

Everything below is a real improvement that the brief does not ask for. Named rather than built, so
the code stays the size of the problem. Note what is already provided and therefore not written
here: the WebSocket protocol and its automatic Pong replies (JDK), transactions and the atomic
upsert (PostgreSQL), connection pooling (Hikari), migrations (Flyway), restart and health probes
(Kubernetes, actuator). What is left to implement is the part specific to this problem — where the
transaction boundary goes, what the upsert predicate says, and what the service does when it is no
longer the leader.

- **A bounded queue and a writer thread.** Today a write stalls the socket for its duration. A
  bounded `ArrayBlockingQueue` plus a writer thread would absorb arrival bursts. Bounded, not
  unbounded: a full queue blocks the producer and pushes back on the exchange, whereas an unbounded
  one absorbs the backlog into the heap until the process dies with all of it. Add it when writes
  stop keeping up — it buys smoothing, not correctness.
- **Exponential backoff with jitter.** The fixed delay is fine for one instance. Jitter starts to
  matter when several instances lose the exchange at the same moment and retry in lockstep, turning
  a brief hiccup into a sustained one.
- **Electing the writer rather than configuring it.** One process consumes today because it is the
  only one deployed. Election itself is provided — a Kubernetes `Lease`, etcd, Consul, or Spring
  Integration's `LockRegistry` — so the part to write is not the algorithm but what the service does
  with it, and that part is easy to get wrong. A boolean "am I leader?" check is not enough: the
  holder can pass the check, pause for thirty seconds, and write after a new leader took over. The
  rejection has to happen at the database, via a **fencing token** — a monotonically increasing term
  from the election primitive (etcd's revision, ZooKeeper's zxid, or a column incremented under a
  row lock), carried on every write and refused if lower than the one already stored, in the same
  transaction as the data.

  Worth noting that split-brain would be benign here for correctness, because every write is
  idempotent and ordered: history deduplicates on the primary key, `latest_quote` is a max-register,
  and the checkpoint has its own monotonic guard. Fencing would buy resource protection and safety
  for any future sink that is *not* commutative — publishing to a topic, notifying a service.
  `pg_try_advisory_lock` is the tempting shortcut and the wrong one: it is session-scoped, so it
  releases exactly when the holder is partitioned and still believes it holds it, and it carries no
  term to fence with.
- **Partitioning `quote` by `event_time`**, so retention is detaching a partition rather than a
  `DELETE` that has to vacuum behind itself.
- **A push channel** (SSE, or publishing alongside the database write) for consumers that want every
  tick rather than the current value. They poll today.

One limit that is not a choice: an outage longer than the exchange's replay window cannot be
recovered. The exchange answers `CHECKPOINT_TOO_OLD` rather than silently fast-forwarding, so the
consumer learns about it instead of inheriting an undetectable gap.

---

## 14. How the guarantee is verified

`MarketDataRecoveryTest` runs against a real PostgreSQL (Testcontainers), severs the connection
mid-stream, and asserts:

- `quotesMissing == 0`
- stored sequences form an unbroken run: `count(DISTINCT sequence) = max - min + 1`
- `duplicatesDiscarded > 0` — proving recovery went through the **replay path** rather than quietly
  resuming at the live edge and leaving a hole behind it
- `latest_quote` never regressed for any instrument
- arrival rates really are uneven, so the coalescing path is exercised under the conditions it exists
  for

The same thing by hand, against the Compose stack:

```sh
docker compose up --build
docker compose pause exchange && sleep 20 && docker compose unpause exchange
curl -s localhost:8099/api/v1/marketdata/status | jq '.feed.quotesMissing'   # 0
```

`pause` sends `SIGSTOP`, so the socket stays **open and silent** rather than closing — which exercises
failure mode (b) above, the one only the stall timer catches.

Measured on that stack: across a 20-second outage the consumer's position advanced 28 060 -> 36 018
— 7 958 messages it was disconnected for — with `quotesMissing = 0` and 20 duplicates discarded,
which is what shows it returned through the replay path rather than at the live edge. The stored
sequences were contiguous end to end: 72 913 rows, 72 913 distinct, span 72 913.
