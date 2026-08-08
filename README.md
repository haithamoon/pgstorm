# pgstorm

A Go-based PostgreSQL load generator that hammers a database with a realistic mixed workload — INSERT, READ, JOIN, UPDATE, DELETE, and IP-range reads — using large JSONB payloads to stress **heap I/O**, **Toast storage**, and **MVCC dead tuple accumulation**.

Most Postgres load generators just fire INSERTs. pgstorm is specifically designed to exercise the parts of Postgres that matter most in production: autovacuum lag, Toast fragmentation, WAL amplification, and checkpoint pressure. Each replica exposes a Prometheus `/metrics` endpoint so you can observe everything in real time.

![pgstorm Grafana dashboard showing throughput, latency percentiles, table and dead-tuple growth, WAL rates, checkpoint activity and wait events](docs/images/dashboard.png)

The bundled Grafana dashboard, auto-provisioned by `docker compose up`. Client-side throughput and latency sit alongside the server-side internals — WAL bytes and full-page images, live vs dead tuples, checkpoint write/sync timing, buffer writes and wait events — so cause and effect land on the same timeline.

**Supported Postgres versions:** 14, 15, 16, 17

---

## Table of Contents

- [Why not just pgbench?](#why-not-just-pgbench)
- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Schema](#schema)
- [Configuration](#configuration)
- [Run results](#run-results)
- [Methodology](#methodology)
- [Limitations](#limitations)
- [Metrics Reference](#metrics-reference)
- [What to Watch](#what-to-watch)
- [Running Multiple Replicas](#running-multiple-replicas)
- [License](#license)

---

## Why not just pgbench?

Often you should. `pgbench` ships with Postgres, everyone knows it, and its numbers are comparable against decades of published results. If you are sizing hardware or evaluating managed-Postgres providers, use `pgbench`.

pgstorm answers a different question. `pgbench`'s default TPC-B workload uses four tables of roughly 100-byte rows — deliberately, because that isolates transaction throughput. The side effect is that several things which actually degrade production databases never happen: rows that small never reach the ~2 KB TOAST threshold, so out-of-line storage is never exercised, and the default script contains no `DELETE`.

|  | pgbench (default TPC-B) | pgstorm |
|---|---|---|
| Question it answers | "How many TPS can this box do?" | "Why does my database degrade over time?" |
| Row size | ~100 bytes | 8–16 KB JSONB (20% of writes by default, tunable via `TOAST_PCT`) |
| TOAST | Never — rows sit far below the threshold | By design, and controllable |
| Payload compressibility | n/a | Incompressible base64, so pglz cannot quietly keep values inline |
| Deletes | None in the default script | Batched deletes, driving real dead-tuple churn |
| Server internals | Not exposed — you instrument separately | `pg_stat_wal`, `pg_stat_bgwriter`/`pg_stat_checkpointer`, `pg_stat_user_tables`, `pg_stat_activity` exported to Prometheus |
| Row selection | Uniform random | Ring buffer of recent IDs — no `ORDER BY random()` scans |
| What results compare | Your database against everyone else's | Your database against itself, before and after a change |

**Reach for `pgbench`** when you want a comparable TPS number.

**Reach for pgstorm** when you are chasing TOAST amplification, sizing `max_wal_size`, tuning autovacuum, or testing a checkpoint theory — questions that need a workload which actually produces bloat and out-of-line storage.

One honest caveat: `pgbench -f` accepts custom scripts, so a determined user could reproduce much of pgstorm's *workload*. The harder part to replicate is the observability — pgstorm exports WAL bytes/records/FPI, dead tuples, autovacuum counts, checkpoint pressure and wait events **alongside** client-side latency, so cause and effect land on one timeline instead of two.

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

The `loadgen` replicas deliberately do **not** publish a host port — Prometheus scrapes them directly over the Compose network via Docker DNS service discovery. To confirm metrics are flowing, check that the `pgstorm` targets are `up`:

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
go build -o pgstorm .
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
| `RESULT_JSON_PATH` | *(empty)* | Write a whole-run result document here when the run ends. Empty disables it. See [Run results](#run-results) |
| `INDEX_STATS_INTERVAL_SECS` | `30` | How often to poll Postgres for table and index stats |
| `SHUTDOWN_TIMEOUT_SECS` | `5` | Grace period for the HTTP server to drain on shutdown |

---

## Run results

The stdout summary is per-window and discarded as it goes, so by default nothing about a run outlives the process. Set `RESULT_JSON_PATH` to write a single self-describing document when the run ends:

```bash
RUN_DURATION_SECS=900 RESULT_JSON_PATH=results/run.json docker compose up --build
```

It records the whole run — not the last window — alongside the settings that produced it, so a result stays interpretable after the fact and two runs can be compared field by field:

```json
{
  "profile": "oltp-jsonb",
  "started_at": "2026-08-08T11:38:16.543009Z",
  "ended_at": "2026-08-08T11:39:16.548733Z",
  "duration_seconds": 60.005367334,
  "config": {
    "workers": 20, "toast_pct": 20, "min_payload_kb": 8, "max_payload_kb": 16,
    "op_weights": { "insert": 35, "read_join": 20, "read_simple": 15, "update": 15, "delete": 10, "read_by_ip": 5 }
  },
  "totals": { "ops": 109711, "errors": 0, "ops_per_sec": 1828.35 },
  "operations": [
    {
      "op": "delete",
      "count": 10897,
      "errors": 0,
      "ops_per_sec": 181.6,
      "latency": {
        "min_ms": 0.337, "mean_ms": 10.266, "p50_ms": 7.948,
        "p95_ms": 26.535, "p99_ms": 40.271, "p999_ms": 74.159, "max_ms": 147.229
      }
    }
  ]
}
```

Latency figures are computed from every observation rather than from histogram buckets, so they are real measured values with no interpolation and no upper bound — a p99 of 45 s is reported as 45 s, not clipped.

**Latency covers successful operations only.** A failure returns in microseconds without touching data, so counting it as a latency sample would pull the percentiles down exactly as the database got worse — a broken "after" run could read as a latency improvement. Failures are still fully reported in `errors`; they are separated, not hidden. Two consequences:

- `latency` is **omitted entirely** for an op where every attempt failed (`count == errors`). There is nothing to summarise, and an all-zero block would read as "instant".
- `ops_per_sec` counts **attempts**, failures included. A database that is rejecting connections will show a *high* rate next to a high error count, because failing is fast.

A clean run should report `errors: 0`. Treat any non-zero count as something to explain before trusting the rest of the document — if a meaningful share of attempts failed, the two runs you are comparing did different amounts of work.

The document also brackets the run with the size of the tracked tables, because pgstorm grows the database as it runs:

```json
"dataset": {
  "start": { "total_bytes": 1216348160, "live_tuples": 402118, "table_bytes": { "events": 903168000, "sessions": 221347840, "audit_log": 91832320 } },
  "end":   { "total_bytes": 1522860032, "live_tuples": 498233, "table_bytes": { "events": 1132675072, "sessions": 275480576, "audit_log": 114704384 } },
  "growth_bytes": 306511872
}
```

Sizes come from `pg_total_relation_size`, so they include TOAST and index storage. Without them a result cannot be compared against another, because the two runs were not measuring the same database. The key is omitted entirely if the size could not be read, so a missing measurement never reads as an empty database.

It also records the server it ran against, since which Postgres and how it is configured moves the numbers more than most of pgstorm's own settings:

```json
"server": {
  "version": "16.14 (Debian 16.14-1.pgdg13+1)",
  "settings": {
    "shared_buffers": "16384 8kB",
    "max_wal_size": "1024 MB",
    "checkpoint_timeout": "300 s",
    "synchronous_commit": "on",
    "full_page_writes": "on",
    "default_toast_compression": "pglz",
    "autovacuum": "on"
  }
}
```

A curated set of about fifteen settings, not everything `pg_settings` knows — a full dump is several hundred rows and buries the ones that matter. `default_toast_compression` is worth noting: pgstorm's payloads are built to defeat pglz, so a server using lz4 exercises TOAST differently than the design assumes.

**Credentials are never written.** `PG_DSN` contains the database password and is deliberately excluded from the document, along with any setting that could carry a connection string. A test enforces this so it cannot regress.

Finally, every summary window is kept as a series, so you can see *when* during a run something changed rather than only the average:

```json
"timeseries": [
  { "start_seconds": 0,  "end_seconds": 10, "ops": 407, "ops_per_sec": 40.7,
    "operations": [ { "op": "read_join", "count": 81, "ops_per_sec": 8.1, "p50_ms": 92.4, "p95_ms": 310.6, "p99_ms": 541.5 } ] },
  { "start_seconds": 10, "end_seconds": 20, "ops": 466, "ops_per_sec": 46.6, "operations": [ ... ] }
]
```

This is where throughput sagging as the dataset grows, or latency stepping up when autovacuum starts work, becomes visible without standing up Prometheus. Each entry carries count, errors, rate and p50/p95/p99 per operation — the same figures the console prints. The interval counts sum exactly to `totals`.

`errors` is omitted from a window where it is zero, which is most of them. Where it is present and equals `count`, that op failed outright for the whole window and its percentiles are `0` because there was nothing to measure — the adjacent counts are what tell you so.

Notes:

- **Opt-in.** Leave `RESULT_JSON_PATH` empty (the default) and nothing is written.
- **One file per process.** Running `--scale loadgen=N` gives each replica its own view, so point each at a distinct path; the totals are per-replica, not cluster-wide.
- **Written after the workers stop**, so it includes the trailing window since the last printed summary. A run shorter than one `SUMMARY_INTERVAL_SECS` prints nothing but still produces a complete result.
- **Written atomically** via a temp file and rename, so a reader never sees a partial document. A write failure is logged and does not fail the run.

---

## Methodology

pgstorm is built for one loop: **measure your database, change something, measure again, and see whether it actually helped.** These are the things that decide whether your "after" is honestly comparable to your "before".

**Fix the duration.** Set `RUN_DURATION_SECS` and use the same value on both sides. A 5-minute run and a 20-minute run are not comparable — pgstorm grows the database as it runs, so the longer one ends up working against more data. Leaving it at `0` (run until stopped) means the length is whenever you happened to press Ctrl-C; pgstorm warns if you do that while writing a result.

**Wipe between runs.** `docker compose down && rm -rf ./pgdata && docker compose up --build`. Otherwise the second run starts on top of the first one's data and is slower for that reason alone, not because of anything you changed.

**Change one thing per round.** Tune `shared_buffers`, or the op mix, or `TOAST_PCT` — not several at once, or you cannot attribute the difference to any of them.

**Repeat each side.** One run against one run is noise. Run each configuration two or three times and compare the ranges; do not chase a few percent.

**Do not let the client be the bottleneck.** If pgstorm and Postgres are fighting for the same cores, you are measuring your laptop, not your database. Check the generator is not CPU-saturated, or run it on a separate machine.

**Diff the two result files.** This is the point of `RESULT_JSON_PATH`. The `config` and `server` blocks record what you changed — including the actual Postgres settings in force — and `totals` plus the per-op `latency` record what it bought you:

```bash
RESULT_JSON_PATH=results/before.json docker compose up --build
# ...tune postgres, wipe pgdata...
RESULT_JSON_PATH=results/after.json  docker compose up --build

diff <(jq -S . results/before.json) <(jq -S . results/after.json)
```

Check `stopped_by` in both while you are there. If one says `signal`, that run was cut short and its numbers cover less time than you think.

---

## Limitations

Stated plainly, because a benchmark whose weaknesses are hidden is worth less than one whose weaknesses are known.

**Reported percentiles are optimistic under load.** pgstorm is a *closed-loop* generator: a fixed pool of workers each block on their operation before issuing the next. When the server stalls, pgstorm issues fewer operations, so the stall is under-sampled and p95/p99 look better than a user would experience. This is called coordinated omission. Adding replicas raises concurrency but does not remove it — only an open-loop generator that records intended issue time would. **Treat pgstorm's tail latencies as a lower bound.**

**How long failures took is not recorded anywhere.** Latency covers successful operations only — mixing failures in is strictly worse, since a flood of microsecond errors makes a degrading server look faster. But the information is genuinely lost: a `statement_timeout` firing at 30 s shows up only as a bump in `errors`, not as a slow observation. If you are diagnosing timeouts, read the error count and the Postgres logs, not the latency block.

**The workload is synthetic.** Payloads are generated JSON with incompressible bodies, sized to exercise TOAST. They resemble production data in shape and size, not in content or access distribution.

**There is no think time by default.** Workers issue their next operation immediately (`THINK_TIME_MS=0`), which is a stress pattern, not a simulation of user behaviour.

**Results are per-process.** Under `--scale loadgen=N` each replica writes its own result covering its own operations. There is no cluster-wide aggregation; the `dataset` block is the shared database and will look identical across replicas.

**There is no scale factor.** Unlike `pgbench -s`, pgstorm cannot preload to a target size — the dataset is a by-product of how long the run lasted rather than something you set. This is a deliberate omission, not a gap waiting to be filled. The practical consequence is that a longer run is working against more data than a shorter one, which is why fixing `RUN_DURATION_SECS` and wiping between runs matters, and why every result records its `dataset` block.

**Numbers are not comparable across different machines.** pgstorm is for comparing a database against itself before and after a change. Two results from different hardware, or different Postgres instances, are not measuring the same thing — the tool makes no attempt to normalise for that.

---

## Metrics Reference

All metrics are prefixed with `pgstorm_`. The `/metrics` endpoint also exposes Go runtime and process metrics from the default Prometheus registry.

### Operation Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pgstorm_ops_total` | Counter | `op`, `status` | Total operations completed; `status` is `ok` or `error` |
| `pgstorm_ops_skipped_total` | Counter | `op` | Operations that did **no** database work and were skipped — cold-start empty ring, or `FOR UPDATE SKIP LOCKED` contention. Tracked separately so they don't count as ~0 ms successes and distort latency/throughput |
| `pgstorm_op_duration_seconds` | Histogram | `op` | Latency of **successful** operations; buckets at 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000 ms. Failures are excluded (they return in microseconds and would drag the quantiles down as the server degrades), so `_count` does **not** equal `sum(pgstorm_ops_total)` — take the error rate from `ops_total{status="error"}`. During a total outage the Grafana latency panels go blank rather than to zero, because nothing succeeded |
| `pgstorm_workers_active` | Gauge | — | Number of operations currently in flight |

### Connection Pool

| Metric | Type | Description |
|---|---|---|
| `pgstorm_pool_acquired_conns` | Gauge | Connections currently checked out by workers |
| `pgstorm_pool_idle_conns` | Gauge | Idle connections waiting in the pool |
| `pgstorm_pool_total_conns` | Gauge | Total open connections (acquired + idle) |
| `pgstorm_pool_max_conns` | Gauge | Pool capacity (`WORKERS + 5`) |
| `pgstorm_pool_acquire_count_total` | Counter | Cumulative successful connection acquisitions |
| `pgstorm_pool_empty_acquire_count_total` | Counter | Acquisitions that had to **wait** for a free connection — this wait is charged to op latency, so a rising value means client-side pool contention (not server slowness) |
| `pgstorm_pool_canceled_acquire_count_total` | Counter | Acquisitions cancelled by context before obtaining a connection |
| `pgstorm_pool_acquire_duration_seconds_total` | Counter | Cumulative time spent waiting to acquire a connection (seconds) |

### Table Stats *(always collected)*

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pgstorm_table_size_bytes` | Gauge | `table` | Heap size in bytes (excludes Toast and indexes) |
| `pgstorm_table_live_tuples` | Gauge | `table` | Estimated live row count from `pg_stat_user_tables` |
| `pgstorm_table_dead_tuples` | Gauge | `table` | Estimated dead row count — proxy for MVCC bloat |
| `pgstorm_table_mod_since_analyze` | Gauge | `table` | Rows modified since last analyze; high value means stale planner stats |
| `pgstorm_table_autovacuum_total` | Counter | `table` | Autovacuum runs observed since pod start |
| `pgstorm_table_autoanalyze_total` | Counter | `table` | Autoanalyze runs observed since pod start |

### Index Stats *(only when `CREATE_INDEXES=true`)*

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pgstorm_index_size_bytes` | Gauge | `index`, `table` | Index size in bytes |
| `pgstorm_index_scans_total` | Counter | `index`, `table` | Index scans observed since pod start |

All 11 indexes (8 explicit + 3 primary keys) are tracked automatically by querying `pg_stat_user_indexes` — no hardcoded index names.

### Checkpoint and bgwriter Stats

Sourced from `pg_stat_bgwriter` on PG14–16 and split across `pg_stat_bgwriter` + `pg_stat_checkpointer` on PG17+. The version is detected automatically at startup.

| Metric | Type | Description |
|---|---|---|
| `pgstorm_bgwriter_checkpoints_timed_total` | Counter | Checkpoints triggered by `checkpoint_timeout` |
| `pgstorm_bgwriter_checkpoints_req_total` | Counter | Checkpoints triggered by WAL segment demand |
| `pgstorm_bgwriter_buffers_checkpoint_total` | Counter | Shared buffers written during checkpoints |
| `pgstorm_bgwriter_buffers_clean_total` | Counter | Shared buffers written by the background writer |
| `pgstorm_bgwriter_buffers_backend_total` | Counter | Shared buffers written directly by backends *(PG14–16 only)* |
| `pgstorm_bgwriter_checkpoint_write_seconds_total` | Counter | Time spent writing files during checkpoints |
| `pgstorm_bgwriter_checkpoint_sync_seconds_total` | Counter | Time spent syncing files during checkpoints |

### WAL Stats *(PG14+ required)*

| Metric | Type | Description |
|---|---|---|
| `pgstorm_wal_bytes_total` | Counter | Total WAL bytes generated |
| `pgstorm_wal_records_total` | Counter | Total WAL records generated |
| `pgstorm_wal_fpi_total` | Counter | Full-page images written to WAL |
| `pgstorm_wal_buffers_full_total` | Counter | Times WAL was flushed because WAL buffers were full |

---

## What to Watch

These PromQL expressions surface the most important Postgres health signals during a load test.

**Throughput and error rate:**
```promql
rate(pgstorm_ops_total{status="ok"}[1m])
rate(pgstorm_ops_total{status="error"}[1m])
```

**Latency percentiles by operation:**
```promql
histogram_quantile(0.99, rate(pgstorm_op_duration_seconds_bucket[1m]))
histogram_quantile(0.50, rate(pgstorm_op_duration_seconds_bucket[1m]))
```

**MVCC dead tuple accumulation** — rising dead tuples with infrequent autovacuum means bloat is building faster than it is being reclaimed:
```promql
pgstorm_table_dead_tuples
rate(pgstorm_table_autovacuum_total[5m])
```

**WAL write amplification** — how many bytes of WAL each write generates, and full-page image spikes after each checkpoint:
```promql
rate(pgstorm_wal_bytes_total[1m])
rate(pgstorm_wal_fpi_total[1m])
```

**Checkpoint pressure** — `checkpoints_req` should be near zero; a non-zero rate means WAL is filling up faster than `checkpoint_timeout`:
```promql
rate(pgstorm_bgwriter_checkpoints_req_total[5m])
```

**Backend buffer writes** *(PG14–16)* — backends forced to write dirty buffers directly is a sign the bgwriter cannot keep up:
```promql
rate(pgstorm_bgwriter_buffers_backend_total[1m])
```

**Index utilisation** *(requires `CREATE_INDEXES=true`)*:
```promql
rate(pgstorm_index_scans_total[1m])
pgstorm_index_size_bytes
```

**Connection pool saturation:**
```promql
pgstorm_pool_acquired_conns / pgstorm_pool_max_conns
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

pgstorm is licensed under the **Apache License 2.0** (`Apache-2.0`).

Copyright (C) 2026 Haitham Gadelrab. See the [LICENSE](LICENSE) file for the full text.

Apache-2.0 was chosen deliberately. A tool a DBA cannot get approved is a tool they cannot use, and many organisations have blanket policies against copyleft licences regardless of whether the obligations would ever actually apply. Permissive licensing keeps pgstorm usable inside them without a legal review.
