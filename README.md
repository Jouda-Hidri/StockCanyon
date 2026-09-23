# Stockcanyon — Market Data Service

Consumes a price feed from a Stock Exchange over a WebSocket, stores it, and exposes the latest
quote by ISIN to other internal services.

Three packages, one per feature in the brief:

| Package | Feature |
| --- | --- |
| [`consumption/`](src/main/java/stockcanyon/consumption) | Consume messages over a WebSocket, with a fallback mechanism that keeps consumption gap-free |
| [`storage/`](src/main/java/stockcanyon/storage) | Store the consumed data in a reliable database |
| [`distribution/`](src/main/java/stockcanyon/distribution) | Expose the data to other internal services through an API |

**[`DESIGN.md`](src/main/java/stockcanyon/DESIGN.md)** has the diagrams: component layout, the
consumption flow, the recovery timeline, the gap-detection state machine and the data model.

---

## Run it

```sh
docker compose up --build
```

Three containers: PostgreSQL, an `exchange` playing the Stock Exchange, and `marketdata` consuming
it. Two of them are the same image — the exchange and the consumer are one application with
different properties — so the consumer talks to something across a real network connection rather
than to an object in its own heap.

```sh
# The endpoint the brief asks for
curl -s localhost:8099/api/v1/marketdata/quotes/US0378331005/latest | jq

# Consumption state, including whether anything was missed
curl -s localhost:8099/api/v1/marketdata/status | jq
```

Requires Java 21 and Docker. To run without Compose you need a PostgreSQL to point at:

```sh
./gradlew bootRun --args='--marketdata.simulator.enabled=true'
```

---

## The requirement that shapes everything

> Ensure the application consumes messages continuously **without any gaps**, even in the event of
> network interruptions or failures.

Reconnecting is easy. Reconnecting without losing what was published while you were away is the
problem. It is **enforced** in one place and **checked** in another.

**Enforced by the checkpoint.** Every flush writes the quotes, moves each instrument's latest
quote, and advances the stored checkpoint — *all three in one transaction*. There is no window
between "the quotes are durable" and "the position describing them is durable", so a crash cannot
land between them. On reconnect that checkpoint goes back to the exchange, which replays from
there.

**Checked by the sequence number.** The transaction makes gaps unlikely; it does not make them
observable. A service can reconnect flawlessly, log nothing alarming, show a healthy connection and
a climbing quote count, and still have missed ten thousand messages — nothing in that picture
contradicts the loss. What makes it observable is the exchange's sequence counter: a hole is proof
of loss, and the absence of one is proof there was none. `quotesMissing` on `/status` is that
count.

### See it hold

```sh
docker compose pause exchange && sleep 20 && docker compose unpause exchange
curl -s localhost:8099/api/v1/marketdata/status | jq '.feed.quotesMissing'   # 0
```

`pause` sends `SIGSTOP`, so the socket stays **open and silent** rather than closing — the hardest
of the three failure modes, and the only one a stall timer can catch.

Measured on that stack: sequence advanced 8469 → 18289 across a 20-second outage (9 820 messages
issued while disconnected) and the consumer stored 9 821 — every one of them, plus a single
boundary duplicate.

---

## API

| Endpoint | Purpose |
| --- | --- |
| `GET /api/v1/marketdata/quotes/{isin}/latest` | The latest quote for an instrument |
| `GET /api/v1/marketdata/status` | Consumption state, including `quotesMissing` |
| `GET /actuator/health`, `/actuator/prometheus` | Liveness and metrics |

```json
{
  "isin": "US0378331005",
  "currency": "USD",
  "bid": 225.9604,
  "ask": 226.0508,
  "mid": 226.0056,
  "sequence": 21457,
  "eventTime": "2026-09-23T14:21:54.887004129Z",
  "ageMillis": 65
}
```

A malformed ISIN is `400` and an unknown one is `404`, because the two need different client
behaviour: one will never succeed however often it is retried, the other may resolve on its own.
ISINs are validated including the **check digit** — a transposed pair of characters still matches
`[A-Z]{2}[A-Z0-9]{9}[0-9]` and still looks like an identifier, but it names a different instrument,
or none.

---

## Why there is a simulator

The brief says *"Utilize the provided Stock Exchange API endpoint, `/quotes?checkpoint_timestamp=`"*.
No endpoint came with the task, so [`simulator/`](src/main/java/stockcanyon/simulator) implements
that contract in order to have something to consume. It is **not part of the service**, and it
earns its place twice over:

- **No public feed offers `checkpoint_timestamp` replay.** Alpaca, Yahoo, Finnhub and Binance all
  stream from the moment you subscribe. Replay *is* the fallback mechanism, so a real feed could
  not exercise it.
- **No real exchange will drop your connection on request.** Without fault injection the recovery
  path can only be asserted about, never demonstrated.

If you have the real endpoint, point `marketdata.exchange-url` at it and set
`marketdata.simulator.enabled=false`. Nothing in the three service packages changes — the simulator
sits on the far side of a WebSocket, not at a seam inside the service.

---

## Tests

```sh
./gradlew test
```

`MarketDataRecoveryTest` runs against a real PostgreSQL via Testcontainers (so Docker must be
running), severs the connection mid-stream, and asserts that `quotesMissing` is zero, that the
stored sequences form an unbroken run across the disconnect, and that duplicates were discarded —
which is what shows recovery went through the *replay* path rather than quietly resuming at the
live edge and leaving a hole behind it.

---

## Deploy

`docker compose up --build` locally; [`fly.toml`](fly.toml) for Fly.io, where the exchange and the
consumer share one machine. See the header of that file for the Postgres attach step — Fly writes
`DATABASE_URL` in libpq form, which the PostgreSQL JDBC driver does not accept.
