# pgstorm — Claude Code Context

## What this is

A Go-based PostgreSQL load generator that stresses **heap I/O**, **Toast storage**, and **MVCC dead tuple accumulation** via a realistic mixed workload (INSERT, READ, JOIN, UPDATE, DELETE, IP-range read) with large JSONB payloads. Runs as multiple replicas on Docker Compose or Kubernetes. Each replica exposes a Prometheus `/metrics` endpoint.

**Supported Postgres versions:** 14, 15, 16, 17

---

## Current status (update this section every session)

<!-- UPDATE THIS SECTION AT THE END OF EVERY SESSION -->

**Last updated:** 2026-07-10
**Active branch:** `main`

### In progress
- **All of P0/P1/P2 are complete** (the last P2 item, "raise payload cardinality", was closed as won't-do — see `CODE-REVIEW.md` rationale). Comprehensive unit tests added and coverage verified (config 100%, workload 97.1%). **The only remaining work is P3** — `pgvector` and message-queue benchmark profiles — which is **blocked on a design decision** (which profile first) and would include the `workload/profiles/` subpackage reorg. Backlog detail in `CODE-REVIEW.md` (git-ignored); P3 design in `docs/rfc-workload-profiles.md`.

### Known open issues
- None currently. (The old empty `metrics/wait_events.go` stub was deleted; wait-event logic lives in `pg_stats.go`.)

### Recently completed
- **Test-coverage pass (2026-07-11):** added unit tests across the new/under-tested code — profile registry + OLTP accessors, weight resolution, `RunWorker`/`runOp` (fake profile), rate limiter, executor DB paths (mock pool/tx, happy + error + skip-locked), payload edges, config validation, `RecordOp`. config 100%, workload 97.1%; remaining gaps are DB-bound (integration/e2e-covered).
- **Closed-loop rate limiting (2026-07-11, `0bdd495`):** `TARGET_RATE_PER_SEC` caps the per-replica op rate via a shared token-bucket `RateLimiter` (`workload/ratelimit.go`, elapsed-time accrual, 100ms burst); 0 = unlimited (default). Per-replica (N replicas → N× at the DB), warns if `THINK_TIME_MS` also set. Live-verified across 50/200/1000 ops/s.
- **Pluggable workload profiles (2026-07-10):** refactored the fixed schema + 6-op workload into a `Profile` interface + registry; current workload is the default `oltp-jsonb` profile, selected via `PROFILE`. Op weights now resolve generically (`workload.ResolveWeights`, sum==100); `db/schema.go` runs a profile's `db.Schema` DDL; table/index stat loops are parameterized by tracked tables. Behavior-preserving (unit tests green, live-PG e2e created 3 tables + 11 indexes with the correct op mix). As-built simplifications and the eventual `workload/profiles/` layout are documented in `docs/rfc-workload-profiles.md`.
- **P2 small items (2026-07-10, `b9e62c9`):** `percentile()` now interpolates within the histogram bucket (Prometheus-style) instead of snapping to the upper bound; opt-in `READ_PAYLOAD` makes `read_simple`/`read_by_ip` detoast+read `events.payload` (query variants precomputed as constants); deleted the empty `metrics/wait_events.go` stub.
- **P1 review fixes (2026-07-10):** removed dead `LOG_LEVEL` config (`5f20e69`); corrected the false "Toast deduplication" rationale in CLAUDE.md (`1c55240`); moved `ready.Store(true)` to after workers spawn so `/readyz` is honest (`a089d43`); balanced the `WorkersActive` gauge via `defer Dec()` in a new `runOp()` helper — deliberately no `recover()`, since a review confirmed it would mask systematic panics (`a089d43`); corrected the "no mutex on hot path" note (`b78939a`).
- **P0 review fixes (2026-07-10):**
  - **P0 #1** (`ebeae90`): Prometheus was scraping nothing — targets were hardcoded to a typo'd/stale project prefix. Switched `monitoring/prometheus/prometheus.yml` to Docker DNS service discovery on the `loadgen` service; fixed README Quick Start (observe via Prometheus `:9091`/Grafana `:3000`, loadgen has no host port) and removed the false "randomly assigned host port" claim.
  - **P0 #2** (`9eaae92`): `audit_log.diff` never TOASTed — its `strings.Repeat("x")` pad compressed to ~0.8% and stayed inline. Now pads with `randomBase64Exact` (base64 of random bytes; incompressible under pglz/lz4) so it stores out-of-line; also fixed an integer-truncation edge that emitted an empty pad for padLen 1–3.
  - **P0 #3** (`162e22e`): `read_by_ip` inet comparison — verified NOT a bug (uncast query works across all pgx v5 exec modes on live PG16). Kept explicit `$1::inet`/`$2::inet` casts as defensive hardening.
- Prometheus + Grafana added to Docker Compose (`monitoring/` directory); dashboards auto-provisioned on startup
- `postgres_exporter` (v0.16.0) sidecar added; separate PostgreSQL Grafana dashboard provisioned
- Wait Event analysis: `pgloadgen_wait_events_active` GaugeVec in `metrics/pg_stats.go`; polls `pg_stat_activity WHERE wait_event IS NOT NULL`; `Reset()` each tick; shares `INDEX_STATS_INTERVAL_SECS` tick
- Stop hook added to `.claude/settings.json` — reminds to update CLAUDE.md when uncommitted changes exist
- Loadgen scaled to 2 replicas × 2 workers; host port mapping removed; Prometheus discovers replicas via Docker DNS service discovery (see P0 #1)

---

## Module structure

```
pgstorm/
├── main.go                   — selects profile, resolves weights, wires everything; graceful shutdown
├── config/config.go          — generic env-var config (op weights validated in workload.ResolveWeights)
├── db/
│   ├── pool.go               — pgxpool setup (MaxConns = Workers+5)
│   └── schema.go             — generic advisory-lock migration runner; executes a profile's db.Schema
├── workload/
│   ├── profile.go            — Profile/Executor/OpDef interfaces + profile registry
│   ├── weights.go            — ResolveWeights (env→weights, sum==100) + weighted SelectOp
│   ├── oltp.go               — OLTPProfile (default "oltp-jsonb"); owns the sessions/events/audit_log db.Schema
│   ├── ring.go               — shared circular buffer of recently inserted session UUIDs
│   ├── payload.go            — two template pools (100 each); micro-mutation engine
│   ├── ops.go                — oltp-jsonb's 6 DB operations (oltpExecutor)
│   ├── worker.go             — RunWorker goroutine (drives a Profile); per-worker *rand.Rand
│   ├── ratelimit.go          — shared token-bucket RateLimiter (TARGET_RATE_PER_SEC; nil = unlimited)
│   └── stats.go              — per-worker stats; 30s rolling summary; fixed-bucket histograms
└── metrics/
    ├── metrics.go            — OpsTotal (Counter), OpDuration (Histogram), WorkersActive (Gauge)
    ├── pool_collector.go     — custom Collector; calls pool.Stat() only at scrape time
    ├── index_stats.go        — table stats (always) + index stats (CREATE_INDEXES=true only)
    └── pg_stats.go           — bgwriter + WAL + wait events; delta tracking; PG14–17 support
```

---

## Key design decisions

### Workload profiles
A **profile** (`workload.Profile`) owns a schema (`db.Schema`), an op set with env-var-driven default weights (`OpDef`), and a per-worker `Executor` factory. Profiles register themselves via `init()` into a package registry; `main` selects one by the `PROFILE` env var (default `oltp-jsonb`) and resolves op weights with `workload.ResolveWeights` (validates sum==100). The runner (`worker.go`, `stats.go`), migration (`db/schema.go`), and table/index stat loops are all profile-agnostic. **As-built simplifications** (see `docs/rfc-workload-profiles.md`): the interface, registry, and the single `oltp-jsonb` profile currently live in the `workload` package (no `workload/profiles/` subpackage yet — that move happens when profile #2 lands); only op weights moved to the resolver, other profile knobs stay in flat `config.Config`.

### Payload pools
`workload/payload.go` has **two** template pools, not one:
- `sessionTemplatePool` — 4–8 KB (for `sessions.metadata`)
- `eventTemplatePool` — 8–16 KB (for `events.payload`)

`GetMutatedPayload(minKB)` picks the pool based on `minKB <= 4`. The `_pad` field brings each template to target size at init. `trace_id` is mutated (16 hex chars) on every use so each written value is byte-unique. (Postgres has no cross-row Toast *deduplication* to defeat — that earlier rationale was wrong. The real effect is that the 100-template pool never re-writes byte-identical payloads, keeping buffer-pool/cache pressure realistic.)

`audit_log.diff` is generated by `buildAuditDiff` and padded to 2–4 KB. All three values exceed Postgres's ~2 KB Toast threshold on every write — Toast I/O is never compressed away.

### Ring buffer
`workload/ring.go` — INSERT workers call `ring.Push(sessionID)` after commit. UPDATE/DELETE/READ workers call `ring.Sample(rng)` to get a known session UUID. Eliminates `ORDER BY random()` full scans. If the ring is empty (cold start), workers return `nil` and skip the op.

### Metrics pattern
- **Hot path**: `metrics.RecordOp` is cheap — Counter/Histogram updates only, no locking
- **Pool stats**: custom `PoolCollector` calls `pool.Stat()` once per Prometheus scrape, never inside workers
- **Polling loops** (`RunTableStatsLoop`, `RunIndexStatsLoop`, `RunPGStatsLoop`): background goroutines that tick on `INDEX_STATS_INTERVAL_SECS`; all delta-track cumulative Postgres counters via `lastSeen` maps
- **30s summary**: per-worker struct guarded by a per-worker mutex — `Record()` locks on every op, but the lock is effectively uncontended (each worker owns its struct) except for the brief `snapshot()` the collector takes each interval; merges fixed-bucket histograms matching Prometheus bounds for p50/p95/p99

### Advisory lock DDL
`db/schema.go` — exactly one replica runs DDL. Others poll `pg_tables` (and `pg_indexes` when `CREATE_INDEXES=true`) with 500ms sleep. Lock ID: `7654321` (hardcoded constant). **Do not change this constant** — it's the coordination key between replicas.

### Transaction design
| Op | Pattern |
|----|---------|
| INSERT | BEGIN → sessions → 1–3 events → audit_log → COMMIT → ring.Push |
| UPDATE | `SELECT id FOR UPDATE SKIP LOCKED` → rewrite sessions JSONB → audit_log → COMMIT |
| DELETE | Ring-sampled session → `DELETE FROM events WHERE id IN (SELECT ... LIMIT batch_size)` |
| READ_SIMPLE | Fetch 20 most recent events for a ring-sampled session |
| READ_JOIN | 3-table join: sessions + events + audit_log filtered by severity |
| READ_BY_IP | B-tree range scan on `events.source_ip` within a deterministic /24 subnet |

No `ORDER BY random()` anywhere. No deadlocks possible on UPDATE (SKIP LOCKED).

---

## Test strategy

### Unit tests (no database)
Target pure-logic packages — run with `go test ./...`:

Coverage as of 2026-07-11: **config 100%, workload 97.1%, metrics 20.9%** (metrics' remainder is DB-querying collectors/loops, covered by integration + the live e2e, not unit tests). Residual workload gaps are defensive DB-error wrappers inside multi-step transactions.

| Package/File | What to test |
|---|---|
| `workload/ring.go` | Push/Sample behaviour, capacity wrapping, empty-ring nil return, concurrent push+sample |
| `workload/payload.go` | Pool size selection (≤4 KB → session pool), mutation uniqueness, pad sizing, `randomBase64Exact` length/edges, `findTraceIDOffset` not-found |
| `workload/stats.go` | Histogram bucket accumulation, p50/p95/p99 interpolation, snapshot reset |
| `workload/weights.go` | `ResolveWeights` (env→weights, sum==100, negatives, malformed→default), `SelectOp` boundaries + fallthrough, `OpNames` |
| `workload/profile.go` + `oltp.go` | registry get/unknown, `ProfileNames` sorted, OLTP ops-sum-100 / schema shape / Init builds ring |
| `workload/worker.go` | `RunWorker` executes + records + exits on cancel, error-log path, rate-limiter honoured (via a fake Profile/Executor) |
| `workload/ratelimit.go` | nil=unlimited, ctx-cancel unblocks, low-rate burst floor, rate is capped near target |
| `workload/ops.go` | executor happy/rollback/skip-locked/error paths via mock pool+tx (Query/Exec/QueryRow) |
| `config/config.go` | env-var defaults, `MIN>MAX` + negative-rate validation, `getEnv`/`getEnvInt`/`getEnvBool` |
| `metrics/*` | delta trackers, PG14–17 version dispatch, pool collector (via `newPoolCollectorWith`), `RecordOp` ok/error counts |

Use `go test -race ./...` — the ring buffer, stats structs, and rate-limiter have concurrent access patterns the race detector catches.

### Integration tests (live Postgres required)
Located in `db/`, opt in with `-tags integration`:
```
PG_DSN="postgres://user:pass@localhost:5432/mydb?sslmode=disable" \
  go test -tags integration ./db/...
```
Tests: schema creation idempotency, advisory lock coordination, pool config.

---

## Build and run

```bash
# Build and vet
go build ./...
go vet ./...

# Unit tests
go test ./...
go test -race ./...

# Docker Compose (Postgres 16 + one loadgen replica)
docker compose up --build

# Fresh wipe
docker compose down && rm -rf ./pgdata && docker compose up --build

# With indexes
CREATE_INDEXES=true docker compose up --build

# Multiple replicas
docker compose up --build --scale loadgen=3

# Against existing Postgres
PG_DSN="postgres://user:pass@localhost:5432/mydb?sslmode=disable" WORKERS=5 ./pgstorm
```

---

## Configuration reference

### Connection
| Variable | Default | Description |
|----------|---------|-------------|
| `PG_DSN` | `postgres://loadgen:loadgen@localhost:5432/loadtest?sslmode=disable` | Postgres DSN |

### Workload percentages (must sum to 100)
| Variable | Default | Op |
|----------|---------|-----|
| `WRITE_PCT` | 35 | INSERT transaction |
| `READ_SIMPLE_PCT` | 15 | Simple event fetch |
| `READ_JOIN_PCT` | 20 | 3-table join |
| `UPDATE_PCT` | 15 | Session metadata rewrite |
| `DELETE_PCT` | 10 | Batch event delete |
| `READ_IP_PCT` | 5 | IP range scan |

### Other key variables
| Variable | Default | Description |
|----------|---------|-------------|
| `PROFILE` | oltp-jsonb | Workload profile to run (registry in `workload/profile.go`) |
| `WORKERS` | 20 | Concurrent worker goroutines per replica |
| `MIN_PAYLOAD_KB` | 8 | Min `events.payload` size |
| `MAX_PAYLOAD_KB` | 16 | Max `events.payload` size |
| `READ_PAYLOAD` | false | Include `events.payload` in `read_simple`/`read_by_ip` to exercise TOAST reads |
| `CREATE_INDEXES` | false | Create 8 B-tree indexes (safe on live data) |
| `RING_SIZE` | 10000 | Session UUID ring buffer capacity |
| `DELETE_BATCH_SIZE` | 50 | Max events deleted per DELETE op |
| `TARGET_RATE_PER_SEC` | 0 | Per-replica ops/sec cap (closed-loop, across the process's workers); 0 = unlimited; N replicas → N× at the DB. Shared token-bucket `RateLimiter` in `workload/ratelimit.go` |
| `SCHEMA_POLL_MS` | 500 | Follower replica schema poll interval |
| `METRICS_PORT` | 9090 | `/metrics`, `/healthz`, `/readyz` |
| `SUMMARY_INTERVAL_SECS` | 30 | Stdout summary interval |
| `INDEX_STATS_INTERVAL_SECS` | 30 | Postgres stats poll interval (also controls wait event polling) |

---

## Important constraints

- **Op percentages must sum to 100**: `WRITE_PCT + READ_SIMPLE_PCT + READ_JOIN_PCT + UPDATE_PCT + DELETE_PCT + READ_IP_PCT = 100`. Validated at startup; process exits if not.
- **No GIN indexes**: intentional — GIN on 8–16 KB JSONB generates thousands of WAL entries per INSERT, shifting the bottleneck away from the workload being tested
- **PG14+ required** for `pg_stat_wal`; PG17 splits bgwriter stats into `pg_stat_checkpointer` (handled automatically by version detection)
- **`pgdata/` is gitignored and dockerignored**: local bind mount for Compose Postgres data
- **Vendor directory is committed**: run `go mod vendor` after adding new deps

---

## Prometheus metrics inventory

| Metric | Type | Condition | Notes |
|--------|------|-----------|-------|
| `pgloadgen_ops_total` | Counter | always | labels: op, status |
| `pgloadgen_op_duration_seconds` | Histogram | always | labels: op |
| `pgloadgen_workers_active` | Gauge | always | ops in flight |
| `pgloadgen_pool_acquired_conns` | Gauge | always | custom Collector |
| `pgloadgen_pool_idle_conns` | Gauge | always | |
| `pgloadgen_pool_total_conns` | Gauge | always | |
| `pgloadgen_pool_max_conns` | Gauge | always | |
| `pgloadgen_table_size_bytes` | Gauge | always | labels: table |
| `pgloadgen_table_live_tuples` | Gauge | always | |
| `pgloadgen_table_dead_tuples` | Gauge | always | |
| `pgloadgen_table_mod_since_analyze` | Gauge | always | |
| `pgloadgen_table_autovacuum_total` | Counter | always | delta-tracked |
| `pgloadgen_table_autoanalyze_total` | Counter | always | delta-tracked |
| `pgloadgen_wait_events_active` | Gauge | always | labels: wait_event_type, wait_event; Reset() each tick |
| `pgloadgen_index_size_bytes` | Gauge | CREATE_INDEXES=true | labels: index, table |
| `pgloadgen_index_scans_total` | Counter | CREATE_INDEXES=true | delta-tracked |
| `pgloadgen_bgwriter_*` | Counter | always | delta-tracked; PG14–17 via version detection |
| `pgloadgen_wal_*` | Counter | always | delta-tracked; PG14+ |

---

## How to extend

### Adding a new metric
1. Declare `prometheus.*` var in the appropriate `metrics/` file
2. Register it in the `Register*()` function called from `main.go`
3. If delta-tracking a Postgres cumulative stat, add a key to the relevant `lastSeen` map
4. New polling loop: follow `RunTableStatsLoop` pattern — create tracker inside loop, tick on context, log errors only when `ctx.Err() == nil`

### Adding a new workload op
1. Define `OpFoo = "foo"` constant in `workload/ops.go`
2. Add `OpFoo` to `allOps` slice in `workload/stats.go`
3. Add `pct` field to `Config`; wire into `SelectOp` in `workload/worker.go`
4. Add `getEnvInt("FOO_PCT", N)` in `config/config.go`; update the sum validation
5. Implement `(e *Executor) doFoo(ctx)` in `ops.go`
6. Update the sum check: the new pct must be included and total must still equal 100
