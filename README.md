# pg-loadgen

A Go-based PostgreSQL load generator that hammers a database with a realistic mixed workload — inserts, reads, joins, updates, and deletes — using large JSONB payloads to stress heap I/O and Toast storage.

Designed to run as multiple replicas on Kubernetes or Docker Compose, with each pod exposing a Prometheus `/metrics` endpoint.

---

## Features

- **Mixed workload** — configurable percentage split across INSERT, READ (simple), READ (join), UPDATE, DELETE
- **Fat rows** — JSONB payloads of 8–16 KB per event row; 4–8 KB per session row (stresses Toast storage and MVCC)
- **Ring buffer targeting** — shared in-process circular buffer of recently inserted session UUIDs; no `ORDER BY random()` full scans
- **Micro-mutation engine** — 100 pre-rendered payload templates mutated on every use; busts Postgres buffer pool deduplication
- **Advisory lock migration** — exactly one pod runs DDL at startup; others wait passively
- **Prometheus metrics** — ops counter, latency histogram (p50–p99), active workers gauge, pool stats via custom collector
- **30s rolling summary** — printed to stdout every 30 seconds with per-op counts, rates, and latency percentiles
- **Graceful shutdown** — SIGTERM drains in-flight ops before exit
- **Horizontal scaling** — stateless pods; scale via `replicas` in Compose or `kubectl scale`

---

## Quick Start (Docker Compose)

```bash
git clone <repo-url>
cd pg-loadgen

docker compose up --build
```

Postgres data is persisted locally in `./pgdata/`. To start fresh:

```bash
docker compose down
rm -rf ./pgdata
docker compose up --build
```

---

## Configuration

All settings are environment variables. Set them in `docker-compose.yml` or as K8s ConfigMap entries.

| Variable | Default | Description |
|---|---|---|
| `PG_DSN` | `postgres://loadgen:loadgen@localhost:5432/loadtest?sslmode=disable` | Postgres connection string |
| `WORKERS` | `20` | Goroutines per pod |
| `CREATE_INDEXES` | `false` | `true` creates B-tree indexes on scalar columns; `false` = PK-only baseline |
| `RING_SIZE` | `10000` | Capacity of the shared session UUID ring buffer |
| `WRITE_PCT` | `35` | % of ops that are INSERT transactions |
| `READ_SIMPLE_PCT` | `20` | % of ops that are simple event reads |
| `READ_JOIN_PCT` | `20` | % of ops that are 3-table JOIN reads |
| `UPDATE_PCT` | `15` | % of ops that are UPDATE + audit (rewrites JSONB) |
| `DELETE_PCT` | `10` | % of ops that are batch event deletes |
| `MIN_PAYLOAD_KB` | `8` | Minimum size of `events.payload` JSONB |
| `MAX_PAYLOAD_KB` | `16` | Maximum size of `events.payload` JSONB |
| `DELETE_BATCH_SIZE` | `50` | Rows deleted per DELETE op |
| `DELETE_OLDER_THAN_MINS` | `10` | (informational) |
| `THINK_TIME_MS` | `0` | Sleep between ops per worker; `0` = full throttle |
| `METRICS_PORT` | `9090` | Port for `/metrics`, `/healthz`, `/readyz` |
| `RUN_DURATION_SECS` | `0` | Run duration in seconds; `0` = run forever |
| `SUMMARY_INTERVAL_SECS` | `30` | How often to print the rolling summary to stdout |
| `LOG_LEVEL` | `info` | Log verbosity |

> All percentage vars (`WRITE_PCT`, `READ_SIMPLE_PCT`, `READ_JOIN_PCT`, `UPDATE_PCT`, `DELETE_PCT`) must sum to exactly 100.

---

## 30-Second Summary

Every `SUMMARY_INTERVAL_SECS` seconds the load generator prints a summary to stdout:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  30s summary [21:10:50 | +30s elapsed]
  total    1,842 ops   61.4 ops/s   errors: 3
  ┌──────────────┬────────┬─────────┬────────┬────────┬────────┐
  │ op           │  count │  ops/s  │ p50 ms │ p95 ms │ p99 ms │
  ├──────────────┼────────┼─────────┼────────┼────────┼────────┤
  │ insert       │    645 │   21.5  │   12.3 │   45.2 │  112.4 │
  │ read_simple  │    368 │   12.3  │    2.1 │    8.7 │   22.3 │
  │ read_join    │    368 │   12.3  │    5.4 │   18.2 │   44.1 │
  │ update       │    276 │    9.2  │   15.7 │   52.3 │  134.2 │
  │ delete       │    185 │    6.2  │    8.9 │   31.4 │   78.6 │
  └──────────────┴────────┴─────────┴────────┴────────┴────────┘
  pool  acquired=18  idle=2  total=20  max=25  │  workers=20
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

Stats are collected per-worker with no lock on the hot path, then merged at print time.

---

## Prometheus Metrics

Each pod exposes metrics at `http://<pod>:9090/metrics`.

| Metric | Type | Labels |
|---|---|---|
| `pgloadgen_ops_total` | Counter | `op`, `status` |
| `pgloadgen_op_duration_seconds` | Histogram | `op` |
| `pgloadgen_workers_active` | Gauge | — |
| `pgloadgen_pool_acquired_conns` | Gauge | — |
| `pgloadgen_pool_idle_conns` | Gauge | — |
| `pgloadgen_pool_total_conns` | Gauge | — |
| `pgloadgen_pool_max_conns` | Gauge | — |

Health endpoints: `GET /healthz` (liveness), `GET /readyz` (readiness).

---

## Scaling

| Goal | Action |
|---|---|
| More total concurrency | Increase `replicas` in Compose / `kubectl scale deployment/pg-loadgen --replicas=N` |
| More concurrency per pod | Increase `WORKERS` |
| More write pressure | Increase `WRITE_PCT`, decrease others (must sum to 100) |
| More I/O pressure | Increase `MAX_PAYLOAD_KB` |
| Simulate paced load | Set `THINK_TIME_MS=50` |
| Max stress | `THINK_TIME_MS=0`, `WORKERS=50`, replicas=10 |
| Compare indexed vs raw throughput | Restart pods with `CREATE_INDEXES=true` (indexes built live on existing data) |

---

## Kubernetes

```bash
# Apply manifests
kubectl apply -f k8s/

# Create the DSN secret
kubectl create secret generic pg-loadgen-secret \
  --from-literal=PG_DSN='postgres://user:pass@host:5432/dbname?sslmode=disable'

# Scale
kubectl scale deployment/pg-loadgen --replicas=5
```

The `k8s/deployment.yaml` includes liveness and readiness probes. `k8s/service.yaml` exposes port 9090 for Prometheus scraping.

---

## Schema

Three tables with realistic foreign keys and unindexed JSONB columns (intentional — GIN indexes on 8–16 KB blobs generate excessive WAL write-amplification):

- **`sessions`** — 4–8 KB `metadata` JSONB
- **`events`** — 8–16 KB `payload` JSONB, FK to `sessions`
- **`audit_log`** — 2–4 KB `diff` JSONB, tracks all mutations

Schema creation is idempotent (`CREATE TABLE IF NOT EXISTS`) and race-safe via `pg_try_advisory_lock` — only one pod runs DDL even when all replicas start simultaneously.

---

## Development

```bash
# Build
go build ./...

# Vet
go vet ./...

# Run locally (needs Postgres on localhost:5432)
PG_DSN=postgres://loadgen:loadgen@localhost:5432/loadtest?sslmode=disable \
  WORKERS=5 RUN_DURATION_SECS=60 ./pg-loadgen
```

Dependencies: `github.com/jackc/pgx/v5`, `github.com/prometheus/client_golang`, `github.com/google/uuid`. All vendored in `vendor/`.
