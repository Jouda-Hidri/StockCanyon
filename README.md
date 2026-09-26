# Stockcanyon — Market Data Service

Consumes a Stock Exchange WebSocket feed without gaps, stores it, and serves the latest quote per
ISIN. Two services, each with its own store, joined by a transactional outbox:

```
 Exchange ─ws─▶ INGESTION ──one transaction──▶ PostgreSQL: quote_history · checkpoint · outbox
                                                            │ Debezium / Kafka Connect
                                                            ▼
                                     Kafka marketdata.latest-quote (compacted, key = ISIN)
                                                            │
 internal services ─GET /quotes/{isin}/latest─▶ DISTRIBUTION ◀┘ newer-only write ─▶ Redis
```

Design and trade-offs: [`DESIGN.md`](src/main/java/stockcanyon/DESIGN.md).

## Run

```sh
docker compose up --build
curl -s localhost:8099/api/v1/marketdata/quotes/US0378331005/latest | jq   # distribution
curl -s localhost:8100/api/v1/marketdata/status | jq                        # ingestion
```

Requires Java 21 and Docker. The simulated exchange keeps its history in memory; after recreating it,
ingestion reports `CHECKPOINT_TOO_OLD` (as it should). Start clean with `docker compose down -v`.

Recovery demo:

```sh
docker compose pause exchange && sleep 20 && docker compose unpause exchange
curl -s localhost:8100/api/v1/marketdata/status | jq '.feed.quotesMissing'   # 0
```

## API

| Endpoint | Service | |
| --- | --- | --- |
| `GET /api/v1/marketdata/quotes/{isin}/latest` | distribution | 200 · 400 malformed · 404 unknown · 503 store down |
| `GET /api/v1/marketdata/status` | ingestion | leader, queue, holes, `quotesMissing` |
| `POST /actuator/outboxreseed` | ingestion | re-publish every ISIN's newest quote |
| `/actuator/health`, `/prometheus` | both | health, metrics |

## Test

```sh
./gradlew test   # Docker required (Testcontainers)
```

Includes an end-to-end test through real PostgreSQL, Debezium, Kafka and Redis.

## Deploy

`kubectl apply -k deploy/k8s` — needs the CloudNativePG (1.27+), Strimzi (1.2) and OpsTree
redis-operator (0.26) operators; Prometheus Operator optional.

| Piece | Shape |
| --- | --- |
| Ingestion | 2 pods, one elected leader, fenced writes |
| Ingestion DB | CloudNativePG PostgreSQL 17, 3 instances, synchronous |
| Kafka + Debezium | Strimzi: 3 brokers, 2 Connect workers, outbox connector |
| Distribution | 3–20 pods, CPU-autoscaled, no database credentials |
| Distribution DB | Redis, 3 nodes + 3 Sentinels |
