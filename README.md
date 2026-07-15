# pgstorm

A Go-based PostgreSQL load generator that hammers a database with a realistic mixed workload — INSERT, READ, JOIN, UPDATE, DELETE, and IP-range reads — using large JSONB payloads to stress **heap I/O**, **Toast storage**, and **MVCC dead tuple accumulation**.

Most Postgres load generators just fire INSERTs. pgstorm is specifically designed to exercise the parts of Postgres that matter most in production: autovacuum lag, Toast fragmentation, WAL amplification, and checkpoint pressure. Each replica exposes a Prometheus `/metrics` endpoint so you can observe everything in real time.

**Supported Postgres versions:** 14, 15, 16, 17

---

## Table of Contents

- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Schema](#schema)
- [Configuration](#configuration)
- [Metrics Reference](#metrics-reference)
- [What to Watch](#what-to-watch)
- [Running Multiple Replicas](#running-multiple-replicas)

---

## Quick Start

**Prerequisites:** Docker, Docker Compose

```bash
git clone https://github.com/haithamoon/pgstorm
cd pgstorm
docker compose up --build
```

The load generator starts immediately once Postgres is healthy. `docker compose up` also brings up the full monitoring stack (Prometheus, Grafana, and postgres-exporter), so metrics are observable out of the box:

- **Grafana** — http://localhost:3000 (login `admin` / `admin`); dashboards are auto-provisioned
- **Prometheus** — http://localhost:9091

The `loadgen` replicas deliberately do **not** publish a host port — Prometheus scrapes them directly over the Compose network via Docker DNS service discovery. To confirm metrics are flowing, check that the `pg-loadgen` targets are `up`:

```bash
curl 'http://localhost:9091/api/v1/targets?state=active'
```

To hit a raw `/metrics` endpoint directly (on `localhost:9090` by default, or whatever `METRICS_PORT` you set), run a single instance locally instead of via Compose (see the build-and-run command below) — the `loadgen` container image is `FROM scratch` and has no shell for `docker compose exec`.

To wipe all data and start fresh:

```bash
docker compose down && rm -rf ./pgdata && docker compose up --build
```

To run with indexes enabled and observe the difference in query plans and index scan rates:

```bash
CREATE_INDEXES=true docker compose up --build
```

To build and run locally against an existing Postgres instance:

```bash
go build ./...
PG_DSN="postgres://user:pass@localhost:5432/mydb?sslmode=disable" WORKERS=5 ./pgstorm
```

To run the unit tests (no database required):

```bash
go test ./...
```

With the race detector:

```bash
go test -race ./...
```

The integration tests in `db/` require a live Postgres and opt in via a build tag:

```bash
PG_DSN="postgres://user:pass@localhost:5432/mydb?sslmode=disable" \
  go test -tags integration ./db/...
```

---

## How It Works

Each worker goroutine runs a continuous loop: pick an operation according to the configured percentages, execute it against Postgres, record latency and outcome, repeat.

A **ring buffer** of recently inserted session UUIDs is shared across all workers. UPDATE, DELETE, and READ operations sample from it rather than using `ORDER BY random()`, which avoids full table scans and keeps the workload targeting real data.

**Operations and default mix:**

| Operation | Env var | Default | Description |
|---|---|---|---|
| `insert` | `WRITE_PCT` | 35% | BEGIN → sessions row + 1–3 events rows + audit_log row → COMMIT |
| `read_simple` | `READ_SIMPLE_PCT` | 15% | Fetch the 20 most recent events for a ring-sampled session |
| `read_join` | `READ_JOIN_PCT` | 20% | 3-table join across sessions, events, and audit_log filtered by severity |
| `update` | `UPDATE_PCT` | 15% | `SELECT FOR UPDATE SKIP LOCKED` → rewrite session metadata JSONB → audit_log row |
| `delete` | `DELETE_PCT` | 10% | Delete a batch of the oldest events for a ring-sampled session |
| `read_by_ip` | `READ_IP_PCT` | 5% | B-tree range scan on `events.source_ip` within a deterministic /24 subnet |

All six percentages must sum to exactly 100.

**Payload design:**

Every JSONB value contains realistic-looking fields: HTTP request and response headers and bodies, stack traces, tags, numeric metrics, and nested context. Request and response bodies are base64-encoded random bytes — high entropy content that Postgres's pglz compressor cannot deflate — ensuring every large value exercises real Toast I/O rather than compressed storage. Payload sizes are controlled by `MIN_PAYLOAD_KB` and `MAX_PAYLOAD_KB`.

**Payload sizes per table:**

| Table | Column | Size |
|---|---|---|
| `sessions` | `metadata` | 4–8 KB (fixed) |
| `events` | `payload` | `MIN_PAYLOAD_KB`–`MAX_PAYLOAD_KB` (default 8–16 KB) |
| `audit_log` | `diff` | 2–4 KB (fixed) |

A configurable share of writes (`TOAST_PCT`, default 20%) produce **large** JSONB values that exceed Postgres's ~2 KB Toast threshold and store out-of-line; the rest are **small** (<2 KB) and stay inline. This mirrors a realistic mixed workload rather than forcing every row to TOAST. Set `TOAST_PCT=100` to make every write TOAST (the previous always-out-of-line behavior), or `TOAST_PCT=0` for all-inline.

---

## Schema

Three tables are created automatically on first run using `CREATE TABLE IF NOT EXISTS`. Schema creation is race-safe via `pg_try_advisory_lock` — exactly one replica runs DDL, the others wait and proceed once the schema is ready.

```sql
sessions (
    id          UUID PRIMARY KEY,
    user_id     UUID NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    ended_at    TIMESTAMPTZ,
    region      TEXT NOT NULL,
    metadata    JSONB NOT NULL,   -- 4–8 KB
    status      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
)

events (
    id          UUID PRIMARY KEY,
    session_id  UUID NOT NULL REFERENCES sessions(id),
    event_type  TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload     JSONB NOT NULL,   -- 8–16 KB by default
    severity    TEXT NOT NULL,
    trace_id    TEXT NOT NULL,
    source_ip   INET,             -- random from 192.168.0.0/16
    created_at  TIMESTAMPTZ NOT NULL
)

audit_log (
    id          UUID PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   UUID NOT NULL,
    action      TEXT NOT NULL,
    changed_by  UUID NOT NULL,
    changed_at  TIMESTAMPTZ NOT NULL,
    diff        JSONB NOT NULL,   -- 2–4 KB
    checksum    TEXT NOT NULL
)
```

When `CREATE_INDEXES=true`, 8 additional B-tree indexes are created:

| Index | Table | Columns |
|---|---|---|
| `idx_sessions_user_id` | sessions | user_id |
| `idx_sessions_status_created` | sessions | status, created_at DESC |
| `idx_events_session_id` | events | session_id |
| `idx_events_occurred_at` | events | occurred_at DESC |
| `idx_events_severity_occurred` | events | severity, occurred_at DESC |
| `idx_events_source_ip` | events | source_ip |
| `idx_audit_log_entity_id` | audit_log | entity_id, changed_at DESC |
| `idx_audit_log_changed_at` | audit_log | changed_at DESC |

Indexes can be created on a live database with existing data. Postgres builds them under concurrent load, which is itself a useful scenario to observe.

---

## Configuration

All configuration is via environment variables.

### Connection

| Variable | Default | Description |
|---|---|---|
| `PG_DSN` | `postgres://loadgen:loadgen@localhost:5432/loadtest?sslmode=disable` | Postgres connection string |

### Workload

pgstorm runs one **workload profile** per process, selected by `PROFILE`. A profile owns
its schema, its operation set, and the default op mix. The default `oltp-jsonb` profile is
the mixed JSONB workload documented here; the profile seam exists so other PostgreSQL
capabilities (e.g. vector search, queue patterns) can be added as additional profiles. The
`*_PCT` variables below configure the `oltp-jsonb` op mix and must sum to 100.

| Variable | Default | Description |
|---|---|---|
| `PROFILE` | `oltp-jsonb` | Workload profile to run (currently only `oltp-jsonb`) |
| `WORKERS` | `20` | Number of concurrent worker goroutines per replica |
| `WRITE_PCT` | `35` | % of operations that are INSERT transactions |
| `READ_SIMPLE_PCT` | `15` | % of operations that are simple event reads |
| `READ_JOIN_PCT` | `20` | % of operations that are 3-table join reads |
| `UPDATE_PCT` | `15` | % of operations that are session UPDATEs |
| `DELETE_PCT` | `10` | % of operations that are batch event deletes |
| `READ_IP_PCT` | `5` | % of operations that are source_ip range reads |
| `THINK_TIME_MS` | `0` | Sleep between operations per worker (ms); `0` = full throttle. Dial aggregate load with replica count × `WORKERS` |
| `RUN_DURATION_SECS` | `0` | Stop after N seconds; `0` = run forever |

> `WRITE_PCT + READ_SIMPLE_PCT + READ_JOIN_PCT + UPDATE_PCT + DELETE_PCT + READ_IP_PCT` must equal 100. The process exits at startup if they do not.

### Payload Size

| Variable | Default | Description |
|---|---|---|
| `MIN_PAYLOAD_KB` | `8` | Minimum `events.payload` size in KB |
| `MAX_PAYLOAD_KB` | `16` | Maximum `events.payload` size in KB (large writes only) |
| `TOAST_PCT` | `20` | Percentage of writes whose JSONB payload is large enough to TOAST (store out-of-line); the rest stay small/inline. `100` = always TOAST (legacy), `0` = always inline. Applies to `events.payload`, `sessions.metadata`, `audit_log.diff` |
| `READ_PAYLOAD` | `false` | Include `events.payload` in `read_simple` / `read_by_ip` so those reads detoast and transfer the JSONB, exercising TOAST *reads* (by default only `read_join` reads the payload) |

### Schema

| Variable | Default | Description |
|---|---|---|
| `CREATE_INDEXES` | `false` | Create B-tree indexes on startup (safe to enable on existing data) |
| `RING_SIZE` | `10000` | Capacity of the shared session UUID ring buffer |
| `DELETE_BATCH_SIZE` | `50` | Maximum events deleted per DELETE operation |
| `USER_POOL_SIZE` | `10000` | Bounded pool of distinct `sessions.user_id` owners (drawn uniformly per insert); gives realistic 1:N user→session cardinality instead of a unique user per row |
| `ACTOR_POOL_SIZE` | `100` | Bounded pool of distinct `audit_log.changed_by` actors (drawn uniformly per audit write) |
| `SCHEMA_POLL_MS` | `500` | How often follower replicas poll for schema readiness (ms) |

### Observability

| Variable | Default | Description |
|---|---|---|
| `METRICS_PORT` | `9090` | Port the Go process listens on for `/metrics`, `/healthz`, `/readyz` |
| `SUMMARY_INTERVAL_SECS` | `30` | How often to print the per-op summary to stdout |
| `INDEX_STATS_INTERVAL_SECS` | `30` | How often to poll Postgres for table and index stats |
| `SHUTDOWN_TIMEOUT_SECS` | `5` | Grace period for the HTTP server to drain on shutdown |

---

## Metrics Reference

All metrics are prefixed with `pgloadgen_`. The `/metrics` endpoint also exposes Go runtime and process metrics from the default Prometheus registry.

### Operation Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pgloadgen_ops_total` | Counter | `op`, `status` | Total operations completed; `status` is `ok` or `error` |
| `pgloadgen_op_duration_seconds` | Histogram | `op` | Operation latency; buckets at 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500 ms |
| `pgloadgen_workers_active` | Gauge | — | Number of operations currently in flight |

### Connection Pool

| Metric | Type | Description |
|---|---|---|
| `pgloadgen_pool_acquired_conns` | Gauge | Connections currently checked out by workers |
| `pgloadgen_pool_idle_conns` | Gauge | Idle connections waiting in the pool |
| `pgloadgen_pool_total_conns` | Gauge | Total open connections (acquired + idle) |
| `pgloadgen_pool_max_conns` | Gauge | Pool capacity (`WORKERS + 5`) |
| `pgloadgen_pool_acquire_count_total` | Counter | Cumulative successful connection acquisitions |
| `pgloadgen_pool_empty_acquire_count_total` | Counter | Acquisitions that had to **wait** for a free connection — this wait is charged to op latency, so a rising value means client-side pool contention (not server slowness) |
| `pgloadgen_pool_canceled_acquire_count_total` | Counter | Acquisitions cancelled by context before obtaining a connection |
| `pgloadgen_pool_acquire_duration_seconds_total` | Counter | Cumulative time spent waiting to acquire a connection (seconds) |

### Table Stats *(always collected)*

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pgloadgen_table_size_bytes` | Gauge | `table` | Heap size in bytes (excludes Toast and indexes) |
| `pgloadgen_table_live_tuples` | Gauge | `table` | Estimated live row count from `pg_stat_user_tables` |
| `pgloadgen_table_dead_tuples` | Gauge | `table` | Estimated dead row count — proxy for MVCC bloat |
| `pgloadgen_table_mod_since_analyze` | Gauge | `table` | Rows modified since last analyze; high value means stale planner stats |
| `pgloadgen_table_autovacuum_total` | Counter | `table` | Autovacuum runs observed since pod start |
| `pgloadgen_table_autoanalyze_total` | Counter | `table` | Autoanalyze runs observed since pod start |

### Index Stats *(only when `CREATE_INDEXES=true`)*

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pgloadgen_index_size_bytes` | Gauge | `index`, `table` | Index size in bytes |
| `pgloadgen_index_scans_total` | Counter | `index`, `table` | Index scans observed since pod start |

All 11 indexes (8 explicit + 3 primary keys) are tracked automatically by querying `pg_stat_user_indexes` — no hardcoded index names.

### Checkpoint and bgwriter Stats

Sourced from `pg_stat_bgwriter` on PG14–16 and split across `pg_stat_bgwriter` + `pg_stat_checkpointer` on PG17+. The version is detected automatically at startup.

| Metric | Type | Description |
|---|---|---|
| `pgloadgen_bgwriter_checkpoints_timed_total` | Counter | Checkpoints triggered by `checkpoint_timeout` |
| `pgloadgen_bgwriter_checkpoints_req_total` | Counter | Checkpoints triggered by WAL segment demand |
| `pgloadgen_bgwriter_buffers_checkpoint_total` | Counter | Shared buffers written during checkpoints |
| `pgloadgen_bgwriter_buffers_clean_total` | Counter | Shared buffers written by the background writer |
| `pgloadgen_bgwriter_buffers_backend_total` | Counter | Shared buffers written directly by backends *(PG14–16 only)* |
| `pgloadgen_bgwriter_checkpoint_write_seconds_total` | Counter | Time spent writing files during checkpoints |
| `pgloadgen_bgwriter_checkpoint_sync_seconds_total` | Counter | Time spent syncing files during checkpoints |

### WAL Stats *(PG14+ required)*

| Metric | Type | Description |
|---|---|---|
| `pgloadgen_wal_bytes_total` | Counter | Total WAL bytes generated |
| `pgloadgen_wal_records_total` | Counter | Total WAL records generated |
| `pgloadgen_wal_fpi_total` | Counter | Full-page images written to WAL |
| `pgloadgen_wal_buffers_full_total` | Counter | Times WAL was flushed because WAL buffers were full |

---

## What to Watch

These PromQL expressions surface the most important Postgres health signals during a load test.

**Throughput and error rate:**
```promql
rate(pgloadgen_ops_total{status="ok"}[1m])
rate(pgloadgen_ops_total{status="error"}[1m])
```

**Latency percentiles by operation:**
```promql
histogram_quantile(0.99, rate(pgloadgen_op_duration_seconds_bucket[1m]))
histogram_quantile(0.50, rate(pgloadgen_op_duration_seconds_bucket[1m]))
```

**MVCC dead tuple accumulation** — rising dead tuples with infrequent autovacuum means bloat is building faster than it is being reclaimed:
```promql
pgloadgen_table_dead_tuples
rate(pgloadgen_table_autovacuum_total[5m])
```

**WAL write amplification** — how many bytes of WAL each write generates, and full-page image spikes after each checkpoint:
```promql
rate(pgloadgen_wal_bytes_total[1m])
rate(pgloadgen_wal_fpi_total[1m])
```

**Checkpoint pressure** — `checkpoints_req` should be near zero; a non-zero rate means WAL is filling up faster than `checkpoint_timeout`:
```promql
rate(pgloadgen_bgwriter_checkpoints_req_total[5m])
```

**Backend buffer writes** *(PG14–16)* — backends forced to write dirty buffers directly is a sign the bgwriter cannot keep up:
```promql
rate(pgloadgen_bgwriter_buffers_backend_total[1m])
```

**Index utilisation** *(requires `CREATE_INDEXES=true`)*:
```promql
rate(pgloadgen_index_scans_total[1m])
pgloadgen_index_size_bytes
```

**Connection pool saturation:**
```promql
pgloadgen_pool_acquired_conns / pgloadgen_pool_max_conns
```

---

## Running Multiple Replicas

pgstorm is safe to run as multiple replicas against the same database. Advisory lock migration ensures exactly one replica runs DDL at startup; the others wait passively until the schema is ready.

To scale up in Docker Compose:

```bash
docker compose up --build --scale loadgen=3
```

Replicas do not publish host ports. The bundled Prometheus discovers every replica automatically through Docker DNS service discovery on the `loadgen` service name (see `monitoring/prometheus/prometheus.yml`), so `--scale loadgen=N` is picked up without any config changes. Each replica still serves `/metrics` on container port 9090 within the Compose network.

Health endpoints available on every replica:

| Endpoint | Description |
|---|---|
| `GET /healthz` | Liveness — returns 200 once the HTTP server is up |
| `GET /readyz` | Readiness — returns 200 once workers have started |
| `GET /metrics` | Prometheus metrics |

---

## License

pgstorm is licensed under the **GNU Affero General Public License v3.0** (`AGPL-3.0-only`).

Copyright (C) 2026 Haitham Gadelrab. See the [LICENSE](LICENSE) file for the full text.
