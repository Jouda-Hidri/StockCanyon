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
      ExchangeWebSocketClient   connect, reconnect, backoff, stall detect, parse
      QuoteBatch                coalescing + deduplication
      SequenceTracker           gap detection
      QuoteConsumer             batches, and the transaction tying data to checkpoint

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

A `DUPLICATE` must **not** move the high-water mark backwards. If it did, the next genuine gap would
be measured from the rewound position and reported as far larger than it was.

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
| `consumption.enabled` | true | Off on read replicas, so exactly one process writes |
| `simulator.skew` | 1.1 | Zipf exponent — busiest instrument ~100x the quietest |

---

## 12. Deliberately not implemented

Everything below is a real improvement that the brief does not ask for. Named rather than built, so
the code stays the size of the problem:

- **A bounded queue and a writer thread.** Today a write stalls the socket for its duration. A
  bounded `ArrayBlockingQueue` plus a writer thread would absorb arrival bursts. Bounded, not
  unbounded: a full queue blocks the producer and pushes back on the exchange, whereas an unbounded
  one absorbs the backlog into the heap until the process dies with all of it. Add it when writes
  stop keeping up — it buys smoothing, not correctness.
- **Exponential backoff with jitter.** The fixed delay is fine for one instance. Jitter starts to
  matter when several instances lose the exchange at the same moment and retry in lockstep, turning
  a brief hiccup into a sustained one.
- **Electing the writer rather than configuring it.** `consumption.enabled` decides who consumes. A
  PostgreSQL advisory lock on a dedicated connection would elect one and release it automatically
  when that process dies — the standby then reads the checkpoint and continues, with the usual
  overlap that deduplication already handles.
- **Partitioning `quote` by `event_time`**, so retention is detaching a partition rather than a
  `DELETE` that has to vacuum behind itself.
- **A push channel** (SSE, or publishing alongside the database write) for consumers that want every
  tick rather than the current value. They poll today.

One limit that is not a choice: an outage longer than the exchange's replay window cannot be
recovered. The exchange answers `CHECKPOINT_TOO_OLD` rather than silently fast-forwarding, so the
consumer learns about it instead of inheriting an undetectable gap.

---

## 13. How the guarantee is verified

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

Measured on that stack: sequence advanced 8469 -> 18289 across a 20-second outage (9 820 messages
issued while disconnected), and the consumer stored 9 821 — every one of them, plus a single boundary
duplicate. `quotesMissing = 0`, stored sequences contiguous.
