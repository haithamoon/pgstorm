# Postgres Load Generator — Claude Code Master Plan

## Project Goal

Build a Go application that generates heavy, realistic mixed-workload load against a PostgreSQL database.
Deployable on Kubernetes with horizontal scaling via replica count. Each pod exposes a Prometheus `/metrics` endpoint.

---

## Deliverables

| # | File | Description |
|---|---|---|
| 1 | `main.go` | Entry point, config loading, signal handling |
| 2 | `config/config.go` | Env-var based config struct |
| 3 | `db/schema.go` | Schema creation (DDL) + migration logic |
| 4 | `db/pool.go` | pgxpool setup and tuning |
| 5 | `workload/worker.go` | Worker goroutine: op selection loop |
| 6 | `workload/ops.go` | All DB operations (insert, read, update, delete, join-read) |
| 7 | `workload/payload.go` | Big-row payload generator (JSON blobs) |
| 8 | `metrics/metrics.go` | Prometheus metrics definitions and HTTP server |
| 9 | `docker-compose.yml` | Compose file: Postgres + load generator (N replicas) |
| 10 | `Dockerfile` | Multi-stage Go build |
| 11 | `k8s/deployment.yaml` | K8s Deployment + ConfigMap (no Helm) |
| 12 | `k8s/service.yaml` | ClusterIP Service for Prometheus scraping |

---

## Schema Design

### Principle
- Multiple tables with realistic foreign keys to force JOIN operations
- Wide rows with large JSONB columns to stress I/O and Toast storage — JSONB columns are **intentionally unindexed** (fat rows drive I/O pressure without GIN write-amplification)
- Optional B-tree indexes on scalar WHERE/JOIN columns only — controlled by `CREATE_INDEXES` env var
- No GIN indexes anywhere — GIN decomposition of 8–16 KB blobs generates thousands of WAL entries per INSERT, turning the bottleneck into index maintenance rather than the workload under test

### Tables

#### `sessions`
```sql
CREATE TABLE IF NOT EXISTS sessions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at    TIMESTAMPTZ,
    region      TEXT NOT NULL,
    metadata    JSONB NOT NULL,   -- ~4–8 KB JSON blob (fat row, NOT indexed)
    status      TEXT NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Only created when CREATE_INDEXES=true
CREATE INDEX IF NOT EXISTS idx_sessions_user_id        ON sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_status_created ON sessions (status, created_at DESC);
```

#### `events`
```sql
CREATE TABLE IF NOT EXISTS events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id    UUID NOT NULL REFERENCES sessions(id),
    event_type    TEXT NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload       JSONB NOT NULL,   -- ~8–16 KB JSON blob (fat row, NOT indexed)
    severity      TEXT NOT NULL DEFAULT 'info',
    trace_id      TEXT NOT NULL,
    source_ip     INET,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Only created when CREATE_INDEXES=true
CREATE INDEX IF NOT EXISTS idx_events_session_id          ON events (session_id);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at         ON events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_severity_occurred   ON events (severity, occurred_at DESC);
```

#### `audit_log`
```sql
CREATE TABLE IF NOT EXISTS audit_log (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type   TEXT NOT NULL,
    entity_id     UUID NOT NULL,
    action        TEXT NOT NULL,   -- INSERT / UPDATE / DELETE
    changed_by    UUID NOT NULL,
    changed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    diff          JSONB NOT NULL,  -- ~2–4 KB JSON blob (fat row, NOT indexed)
    checksum      TEXT NOT NULL
);

-- Only created when CREATE_INDEXES=true
CREATE INDEX IF NOT EXISTS idx_audit_log_entity_id  ON audit_log (entity_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at ON audit_log (changed_at DESC);
```

> UUID PK (not BIGSERIAL) — eliminates sequence hotspot contention when 200 concurrent workers all insert audit rows simultaneously.

### Index Strategy

| `CREATE_INDEXES` | Effect | Use case |
|---|---|---|
| `false` (default) | No secondary indexes. PKs only. Reads do seq scans. | Raw throughput baseline — bottleneck is heap I/O and MVCC, not index maintenance |
| `true` | B-tree indexes on all WHERE/JOIN scalar columns | Realistic production-like workload; compare latency profiles with vs without |

> **Why not GIN?** GIN decomposes each JSONB key path into individual index entries. An 8–16 KB blob with hundreds of keys generates thousands of index entries and WAL records per INSERT. At 35% write mix × 10 pods × 20 workers, GIN maintenance dominates Postgres CPU — you'd be benchmarking GIN, not your application. The JSONB columns still stress I/O, Toast storage, and MVCC without indexing them.

### Row Size Targets

| Table | Target row size | Driver |
|---|---|---|
| `sessions.metadata` | 4–8 KB | Nested JSON: user-agent, headers, feature flags, A/B config |
| `events.payload` | 8–16 KB | Log-like JSON: stack trace, request/response body, tags array |
| `audit_log.diff` | 2–4 KB | Before/after diff JSON |

---

## Operation Mix (Configurable)

| Operation | Default % | What it does |
|---|---|---|
| INSERT | 35% | Insert 1 session + 1–3 events + 1 audit row (transaction) |
| READ (simple) | 20% | SELECT latest N events for a random session |
| READ (join) | 20% | JOIN sessions + events + audit_log filtered by status/severity |
| UPDATE | 15% | UPDATE session status or event severity; insert audit row |
| DELETE | 10% | DELETE a batch of old events by `occurred_at` range (heavy scan) |

All percentages controlled via env vars and must sum to 100.

---

## Shared Session ID Ring Buffer (`workload/ring.go`)

### Why It Exists

Without it, UPDATE and DELETE ops have no way to target specific rows without either:
- `ORDER BY random()` — full table scan, O(n) cost, latency grows with table size
- `WHERE occurred_at < now() - INTERVAL X` — no rows to delete for the first N minutes of a fresh run

The ring buffer is a **shared in-process circular buffer of recently inserted session UUIDs**, populated by INSERT workers and consumed by UPDATE and DELETE workers.

### Structure

```go
// workload/ring.go
type SessionRing struct {
    mu   sync.Mutex
    buf  []uuid.UUID
    size int
    head int   // next write position
    fill int   // how many slots are populated
}

func NewSessionRing(size int) *SessionRing

// Called by INSERT workers after a successful session insert
func (r *SessionRing) Push(id uuid.UUID)

// Called by UPDATE/DELETE workers — returns a random populated slot
// Returns uuid.Nil, false if buffer is empty (worker should skip op)
func (r *SessionRing) Sample() (uuid.UUID, bool)
```

### Sizing

`RING_SIZE=10000` (env var, default 10000). At 35% INSERT mix × 20 workers, the ring fills within seconds and stays full. Workers that call `Sample()` on an empty ring (cold start race) skip their op and immediately retry — no blocking.

### Effect on Each Op

| Op | Before | After |
|---|---|---|
| UPDATE | `ORDER BY random()` full scan | `Sample()` → known UUID → direct PK lookup |
| DELETE | May find 0 rows if table is fresh | `Sample()` → `WHERE session_id = $1` batch delete targeting known session |
| READ (simple) | `ORDER BY random()` scan | `Sample()` → direct FK lookup |

### DELETE query updated to use ring

```sql
-- Delete a batch of events belonging to a sampled session
DELETE FROM events
WHERE id IN (
    SELECT id FROM events
    WHERE session_id = $1
    ORDER BY occurred_at ASC
    LIMIT $2
)
```

This is immediately effective (events exist from the INSERT that populated the ring), produces real I/O (reads event rows, writes delete markers, generates WAL), and is controlled — `LIMIT $2` caps the blast radius per op.

### Ring Buffer in `main.go`

```go
ring := workload.NewSessionRing(cfg.RingSize)

// Pass ring to all workers — INSERT workers push, UPDATE/DELETE workers sample
for i := 0; i < cfg.Workers; i++ {
    go workload.RunWorker(ctx, pool, ring, cfg, metrics)
}
```

The ring is the **only shared mutable state** between workers besides the metrics registry. All access is under `sync.Mutex` — no channels needed (Sample is O(1), lock is held for microseconds).

---

## Configuration (Environment Variables)

```
# Database
PG_DSN=postgres://user:pass@host:5432/dbname?sslmode=disable

# Schema
CREATE_INDEXES=false        # false (default): PKs only, raw throughput baseline
                            # true: create B-tree indexes on all WHERE/JOIN columns

# Workload
WORKERS=20                  # goroutines per pod
RING_SIZE=10000             # shared session UUID ring buffer capacity
WRITE_PCT=35
READ_SIMPLE_PCT=20
READ_JOIN_PCT=20
UPDATE_PCT=15
DELETE_PCT=10

# Payload sizing
MIN_PAYLOAD_KB=8            # min size of events.payload JSONB
MAX_PAYLOAD_KB=16           # max size of events.payload JSONB

# Delete batch
DELETE_BATCH_SIZE=50        # rows deleted per DELETE op
DELETE_OLDER_THAN_MINS=10   # delete events older than N minutes

# Think time
THINK_TIME_MS=0             # 0 = full throttle; >0 = simulate paced load

# Metrics
METRICS_PORT=9090

# Runtime
RUN_DURATION_SECS=0         # 0 = run forever
LOG_LEVEL=info
```

### `CREATE_INDEXES` Behaviour in Code

`db/schema.go` must implement this as two separate DDL functions:

```go
func CreateTables(ctx context.Context, pool *pgxpool.Pool) error {
    // Always runs — CREATE TABLE IF NOT EXISTS for all 3 tables
}

func CreateIndexes(ctx context.Context, pool *pgxpool.Pool) error {
    // Only called when cfg.CreateIndexes == true
    // CREATE INDEX IF NOT EXISTS for all B-tree indexes
}
```

Called from `main.go` after the advisory lock is acquired:

```go
if err := db.CreateTables(ctx, pool); err != nil { ... }
if cfg.CreateIndexes {
    if err := db.CreateIndexes(ctx, pool); err != nil { ... }
}
```

This means you can **restart pods with `CREATE_INDEXES=true`** on an already-loaded table and indexes will be built live (Postgres `CREATE INDEX IF NOT EXISTS` is safe to call on existing tables).

---

## Startup Race Condition — Advisory Lock

### The Problem

All 10 pods start simultaneously and all attempt DDL (`CREATE TABLE`, `CREATE INDEX`).
`IF NOT EXISTS` prevents errors but **does not prevent lock contention** — 9 pods queue up waiting on `AccessExclusiveLock` on the schema catalog. Index creation also takes `ShareLock` on each table, compounding the pile-up.

### Solution: Postgres Advisory Lock

Use `pg_try_advisory_lock` to elect exactly one pod as the **schema owner**. All other pods wait passively until the schema is confirmed ready, then proceed to start workers.

### Startup Sequence (all pods)

```
1. Connect pool (pgxpool.New)
2. Attempt advisory lock:  SELECT pg_try_advisory_lock(7654321)
   ├── TRUE  → I am the schema owner
   │           RunMigrations()       -- CREATE TABLE, optionally CREATE INDEX
   │           SELECT pg_advisory_unlock(7654321)
   └── FALSE → I am a follower
               WaitForSchema()       -- poll pg_tables until all 3 tables exist
3. /readyz returns 200
4. Start WORKERS goroutines
```

### Implementation in `db/schema.go`

```go
const advisoryLockID = 7654321   // arbitrary constant, all pods must agree

func MigrateWithLock(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config) error {
    conn, _ := pool.Acquire(ctx)
    defer conn.Release()

    var locked bool
    conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", advisoryLockID).Scan(&locked)

    if locked {
        defer conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", advisoryLockID)
        log.Info("acquired migration lock — running schema setup")
        if err := CreateTables(ctx, pool); err != nil {
            return err
        }
        if cfg.CreateIndexes {
            if err := CreateIndexes(ctx, pool); err != nil {
                return err
            }
        }
        log.Info("schema setup complete — releasing lock")
    } else {
        log.Info("migration lock held by another pod — waiting for schema")
        if err := WaitForSchema(ctx, pool); err != nil {
            return err
        }
        log.Info("schema ready — proceeding")
    }
    return nil
}

func WaitForSchema(ctx context.Context, pool *pgxpool.Pool) error {
    required := []string{"sessions", "events", "audit_log"}
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(500 * time.Millisecond):
            missing := checkMissingTables(ctx, pool, required)
            if len(missing) == 0 {
                return nil
            }
            log.Infof("waiting for tables: %v", missing)
        }
    }
}
```

### Key Properties

| Property | Detail |
|---|---|
| Lock is **session-scoped** | If the owner pod crashes mid-migration, Postgres releases the lock automatically when the connection closes — no manual cleanup needed |
| Lock ID is **application-defined** | `7654321` is just a convention; hardcode it in `db/schema.go` as a constant |
| Followers **never block** | `pg_try_advisory_lock` is non-blocking; followers poll `pg_tables` with 500ms sleep — zero contention |
| Idempotent DDL | `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS` — safe to re-run across restarts |
| `CREATE INDEX` timing | If `CREATE_INDEXES=true`, index creation happens inside the lock window, blocking followers until done — correct behaviour |

---

## Payload Generator (`workload/payload.go`)

Generate realistic-looking large JSON blobs, not random bytes. This ensures:
- JSONB parsing overhead hits Postgres
- Toast storage is stressed (rows exceeding ~2 KB are stored out-of-line)
- Heap I/O is the bottleneck, not index maintenance

### `events.payload` structure (~8–16 KB)
```json
{
  "request": {
    "method": "POST",
    "path": "/api/v2/...",
    "headers": { ... },        // 20–30 headers
    "body": "<base64 ~4KB>"   // simulated request body
  },
  "response": {
    "status": 200,
    "headers": { ... },
    "body": "<base64 ~4KB>"
  },
  "stack_trace": [ ... ],      // 20–40 frames
  "tags": [ ... ],             // 50–100 string tags
  "metrics": { ... },          // 30–50 numeric k/v pairs
  "context": { ... }           // nested object, 3 levels deep
}
```

### Pre-render Pool + Micro-Mutation Engine

Pre-generate a pool of **100 template payloads** at startup. At selection time, apply a **micro-mutation** before every use — do not hand the raw template to Postgres.

**Why mutation is required:**
- Without it, Postgres may cache identical Toast chunks in shared buffers — 100 templates × 10 pods means only 100 unique Toast pages in the buffer pool, reducing actual I/O pressure
- Identical JSONB bytes compress to the same representation — the load test becomes less realistic as the buffer pool warms up

**Micro-mutation implementation:**

```go
// workload/payload.go

var templatePool [100][]byte   // pre-rendered at init()

func GetMutatedPayload(rng *rand.Rand) []byte {
    // 1. Pick a template
    tmpl := templatePool[rng.Intn(100)]

    // 2. Copy it (never mutate the template in-place)
    buf := make([]byte, len(tmpl))
    copy(buf, tmpl)

    // 3. Inject 8 random bytes at a stable offset inside the "trace_id" field value
    //    The offset is pre-computed at template build time and stored alongside each template
    rng.Read(buf[traceIDOffset : traceIDOffset+8])

    return buf
}
```

**Cost:** one `make` + one `copy` + one `rand.Read(8 bytes)` per op. Negligible CPU overhead. Every row written to Postgres has a unique byte sequence — Toast deduplication is busted, buffer pool pressure is realistic.

The `traceIDOffset` for each template is computed once at startup by scanning for the byte position of `"trace_id":"` in the rendered JSON — stored as `[100]int` alongside `templatePool`.

---

## Worker Logic (`workload/worker.go`)

```
for each worker goroutine:
  loop:
    roll := rand(0,100)
    op = selectOp(roll, config)    // weighted selection
    start = time.Now()
    err = ops.Execute(ctx, pool, op)
    duration = time.Since(start)
    metrics.Record(op, duration, err)
    if THINK_TIME_MS > 0:
      sleep(THINK_TIME_MS)
```

Each worker gets its own context derived from a root context cancelled on SIGTERM.

---

## Metrics (`metrics/metrics.go`)

Expose on `GET /metrics` (Prometheus format) at `METRICS_PORT`.

### Counters
- `pgloadgen_ops_total{op="insert|read_simple|read_join|update|delete", status="ok|error"}`

### Histograms
- `pgloadgen_op_duration_seconds{op="..."}` — buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s

### Gauges
- `pgloadgen_workers_active` — goroutines currently executing a DB op

### Pool Stats — Custom Collector (not worker-updated gauges)

Pool connection stats (`acquired`, `idle`, `total`, `max`) are exposed via a **custom `prometheus.Collector`** that calls `pool.Stat()` at scrape time, not inside worker hot paths.

```go
// metrics/pool_collector.go
type PoolCollector struct {
    pool       *pgxpool.Pool
    acquired   *prometheus.Desc
    idle       *prometheus.Desc
    total      *prometheus.Desc
    maxConns   *prometheus.Desc
}

func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- c.acquired
    ch <- c.idle
    ch <- c.total
    ch <- c.maxConns
}

func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
    stat := c.pool.Stat()
    ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(stat.AcquiredConns()))
    ch <- prometheus.MustNewConstMetric(c.idle,     prometheus.GaugeValue, float64(stat.IdleConns()))
    ch <- prometheus.MustNewConstMetric(c.total,    prometheus.GaugeValue, float64(stat.TotalConns()))
    ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(stat.MaxConns()))
}
```

Registered once at startup: `prometheus.MustRegister(NewPoolCollector(pool))`

**Why not update gauges inside workers?** Worker goroutines call `pool.Stat()` thousands of times per second — one call per op per worker. That's 200+ `pool.Stat()` calls/sec of lock contention on the pool's internal mutex just to update metrics. The custom Collector calls `pool.Stat()` exactly **once per Prometheus scrape** (typically every 15s). Zero hot-path overhead.

### Metrics exposed

| Metric | Type | Labels |
|---|---|---|
| `pgloadgen_ops_total` | Counter | `op`, `status` |
| `pgloadgen_op_duration_seconds` | Histogram | `op` |
| `pgloadgen_workers_active` | Gauge | — |
| `pgloadgen_pool_acquired_conns` | Gauge (Collector) | — |
| `pgloadgen_pool_idle_conns` | Gauge (Collector) | — |
| `pgloadgen_pool_total_conns` | Gauge (Collector) | — |
| `pgloadgen_pool_max_conns` | Gauge (Collector) | — |

### Index Bloat Tracking (`metrics/index_stats.go`)

A background goroutine polls `pg_stat_user_indexes` and `pg_stat_user_tables` every `INDEX_STATS_INTERVAL_SECS` (default 30s) and updates Prometheus gauges. Gives direct feedback on B-tree write amplification when `CREATE_INDEXES=true`.

**Why a background goroutine, not a custom Collector?**
The catalog queries touch multiple system tables and can take a few milliseconds — running them synchronously on every Prometheus scrape adds latency to the scrape itself. A polling goroutine decouples collection from scraping.

**Metrics added:**

| Metric | Type | Labels | Source |
|---|---|---|---|
| `pgloadgen_index_size_bytes` | Gauge | `index`, `table` | `pg_relation_size(indexrelid)` — only when `CREATE_INDEXES=true` |
| `pgloadgen_index_scans_total` | Counter | `index`, `table` | delta-tracked against `pg_stat_user_indexes.idx_scan`; supports `rate()` in PromQL — only when `CREATE_INDEXES=true` |
| `pgloadgen_table_size_bytes` | Gauge | `table` | `pg_relation_size(relid)` — always |
| `pgloadgen_table_live_tuples` | Gauge | `table` | `pg_stat_user_tables.n_live_tup` — always |
| `pgloadgen_table_dead_tuples` | Gauge | `table` | `pg_stat_user_tables.n_dead_tup` — always |

**What to watch:**
- `pgloadgen_index_size_bytes` growing faster than `pgloadgen_table_size_bytes` → B-tree write amplification
- `pgloadgen_table_dead_tuples` rising → MVCC churn from UPDATE/DELETE ops; triggers autovacuum pressure
- Dead tuple ratio in PromQL: `pgloadgen_table_dead_tuples / (pgloadgen_table_live_tuples + pgloadgen_table_dead_tuples)`
- Compare runs with `CREATE_INDEXES=false` vs `true` to quantify the cost of maintaining indexes under load

**Config:** `INDEX_STATS_INTERVAL_SECS=30` (env var, default 30). Table stats poll always runs. Index stats poll only starts when `CREATE_INDEXES=true`.

**Goroutine lifecycle:** Both loops select on the run context derived from the main shutdown signal — they stop as part of graceful drain, same as workers.

### Health
- `GET /healthz` → 200 OK (for k8s liveness probe)
- `GET /readyz` → 200 OK once pool is connected (for k8s readiness probe)

---

## Transaction Design

### INSERT transaction (atomic, 3 tables)
```sql
BEGIN
  INSERT INTO sessions (id, user_id, started_at, region, metadata, status)
    VALUES ($1, $2, now(), $3, $4, 'active')
  INSERT INTO events (id, session_id, event_type, occurred_at, payload, severity, trace_id, source_ip)
    VALUES ($1, $2, $3, now(), $4, $5, $6, $7)   -- repeated 1–3 times
  INSERT INTO audit_log (id, entity_type, entity_id, action, changed_by, diff, checksum)
    VALUES ($1, 'session', $2, 'INSERT', $3, $4, $5)
COMMIT
-- After commit: ring.Push(sessionID)
```

### UPDATE transaction — ring-based, JSONB rewrite
```sql
BEGIN
  -- Step 1: lock the row directly by PK (no scan, no ORDER BY random())
  SELECT id FROM sessions WHERE id = $1 FOR UPDATE SKIP LOCKED

  -- Step 2: update scalar field AND rewrite JSONB blob (triggers Toast rewrite)
  UPDATE sessions
    SET status = 'closed',
        ended_at = now(),
        metadata = $2          -- fresh mutated payload from micro-mutation engine
  WHERE id = $1

  -- Step 3: audit
  INSERT INTO audit_log (id, entity_type, entity_id, action, changed_by, diff, checksum)
    VALUES ($3, 'session', $1, 'UPDATE', $4, $5, $6)
COMMIT
```

**Why JSONB rewrite on UPDATE?** Replacing `metadata` on every update forces Postgres to:
- Write a new Toast chunk (8 KB I/O per update)
- Dead-tuple the old Toast chunk (MVCC overhead)
- Generate WAL for both the heap row and Toast table

This makes UPDATE the heaviest per-op writer in the mix — more realistic than a scalar-only update and compensates for removing `ORDER BY random()`.

`FOR UPDATE SKIP LOCKED` — if the sampled session was just deleted or already locked by another worker, skip it and return immediately. No deadlock possible.

### DELETE operation — ring-based, fixed INTERVAL param
```sql
-- Delete a batch of events for a known session (ring-sampled)
DELETE FROM events
WHERE id IN (
    SELECT id FROM events
    WHERE session_id = $1
    ORDER BY occurred_at ASC
    LIMIT $2
)
```

**INTERVAL fix:** The old query passed an integer into `INTERVAL '$1 minutes'` which pgx cannot bind. The new DELETE targets `session_id` from the ring — no interval arithmetic needed. The subquery forces a scan + sort of that session's events, then batch-deletes them. Immediately effective from pod start (no waiting for rows to age out).

**Load profile:** For a session with 3 events (from INSERT), `LIMIT 50` deletes all 3 in one op. For a hot session that accumulated many events, it churns through them batch by batch — sustained delete pressure.

### READ (simple)
```sql
SELECT id, event_type, occurred_at, severity, trace_id
FROM events
WHERE session_id = $1    -- ring-sampled, direct FK lookup
ORDER BY occurred_at DESC
LIMIT 20
```

### JOIN read
```sql
SELECT
    s.id, s.user_id, s.region, s.status,
    e.id as event_id, e.event_type, e.severity, e.payload,
    al.action, al.changed_at
FROM sessions s
JOIN events e ON e.session_id = s.id
LEFT JOIN audit_log al ON al.entity_id = s.id
WHERE s.id = $1               -- ring-sampled, PK lookup
  AND e.severity = $2
ORDER BY e.occurred_at DESC
LIMIT 20
```

---

## Go Module Structure

```
pg-loadgen/
├── main.go
├── go.mod
├── go.sum
├── config/
│   └── config.go
├── db/
│   ├── pool.go
│   └── schema.go
├── workload/
│   ├── worker.go
│   ├── ops.go
│   ├── payload.go
│   ├── ring.go              ← shared session UUID ring buffer
│   └── stats.go             ← per-worker stats collector + 30s summary printer
├── metrics/
│   ├── metrics.go
│   ├── pool_collector.go    ← custom prometheus.Collector for pool.Stat()
│   └── index_stats.go       ← background goroutine polling pg_stat_user_indexes
├── Dockerfile
├── docker-compose.yml
└── k8s/
    ├── deployment.yaml
    └── service.yaml
```

---

## 30-Second Summary (`workload/stats.go`)

Every 30 seconds a summary is printed to stdout covering the window since the last tick.

### Output format

```
━━━ 30s summary [21:10:50 | +30s elapsed] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  total      1,842 ops   61.4 ops/s   errors: 3
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
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Design

- **`WorkerStats`** — per-worker struct updated on the hot path (no mutex, owned by the goroutine):
  - `counts[op]` — ops in the current window
  - `errors[op]` — errors in the current window
  - `latencyBuckets[op][bucket]` — fixed-bucket histogram (same 10 buckets as Prometheus)
- **`StatsCollector`** — holds a slice of `*WorkerStats` (one per worker, registered at start).
  - `RunSummaryLoop(ctx, interval, pool)` — goroutine that ticks every 30s, snapshots all workers' stats, resets their window counters, computes p50/p95/p99 from merged buckets, and prints the table.
- Snapshot+reset is done by calling a `Snapshot() WorkerStats` method on each worker under a per-worker mutex (held for microseconds). No shared mutex across all workers — each worker only blocks itself for the snapshot, not the whole fleet.
- Pool stats (`acquired`, `idle`, `total`, `max`) read from `pool.Stat()` once per tick.
- `SUMMARY_INTERVAL_SECS` env var (default 30) controls the tick interval.

### Latency percentile computation

p50/p95/p99 are computed from the merged fixed-bucket histogram across all workers. Buckets mirror the Prometheus histogram: `[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500] ms`. Percentile is the upper bound of the bucket where cumulative count crosses the target quantile.

### Integration

- `main.go` creates a `StatsCollector`, passes it to each `RunWorker` call.
- Each worker calls `collector.Record(op, duration, err)` after every op — updates its own `WorkerStats`.
- `main.go` launches `collector.RunSummaryLoop(ctx, interval, pool)` as a goroutine before starting workers.

---

## Go Dependencies

```
github.com/jackc/pgx/v5              # Postgres driver + pgxpool
github.com/prometheus/client_golang  # Prometheus metrics
github.com/google/uuid               # UUID generation
```

No ORM. Raw SQL via pgx for maximum control and performance visibility.

---

## Docker Compose

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: loadtest
      POSTGRES_USER: loadgen
      POSTGRES_PASSWORD: loadgen
    ports: ["5432:5432"]
    volumes: [pgdata:/var/lib/postgresql/data]

  loadgen:
    build: .
    deploy:
      replicas: 3        # ← change this to scale
    environment:
      PG_DSN: postgres://loadgen:loadgen@postgres:5432/loadtest?sslmode=disable
      WORKERS: 20
      WRITE_PCT: 35
      ...
    ports: ["9090"]      # metrics per replica (mapped dynamically)
    depends_on: [postgres]
```

> Note: For k8s, docker-compose replicas are just for local testing. On k8s use `kubectl scale`.

---

## K8s Deployment Pattern

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pg-loadgen
spec:
  replicas: 3   # ← kubectl scale deployment/pg-loadgen --replicas=N
  selector:
    matchLabels:
      app: pg-loadgen
  template:
    spec:
      containers:
      - name: pg-loadgen
        image: your-registry/pg-loadgen:latest
        ports:
        - containerPort: 9090
          name: metrics
        envFrom:
        - configMapRef:
            name: pg-loadgen-config
        - secretRef:
            name: pg-loadgen-secret   # PG_DSN
        livenessProbe:
          httpGet: { path: /healthz, port: 9090 }
        readinessProbe:
          httpGet: { path: /readyz, port: 9090 }
```

```yaml
# k8s/service.yaml — for Prometheus ServiceMonitor scraping
apiVersion: v1
kind: Service
metadata:
  name: pg-loadgen-metrics
  labels:
    app: pg-loadgen
spec:
  selector:
    app: pg-loadgen
  ports:
  - name: metrics
    port: 9090
    targetPort: 9090
```

---

## Build Instructions for Claude Code

1. Initialize Go module: `go mod init pg-loadgen`
2. Install deps: `go get github.com/jackc/pgx/v5 github.com/prometheus/client_golang github.com/google/uuid`
3. Implement files in this order:
   - `config/config.go` first (everything depends on it)
   - `metrics/metrics.go` + `metrics/pool_collector.go` second (workers depend on it)
   - `db/pool.go` + `db/schema.go`
   - `workload/ring.go` (no deps — pure data structure)
   - `workload/payload.go` (no deps)
   - `workload/ops.go` (depends on payload + metrics + ring)
   - `workload/worker.go` (depends on ops + metrics + ring)
   - `main.go` last (wires everything: pool → ring → workers → pool_collector)
4. Then write `Dockerfile` + `docker-compose.yml`
5. Then write `k8s/deployment.yaml` + `k8s/service.yaml`
6. Run `go build ./...` to verify compilation
7. Run `go vet ./...` to catch issues

---

## Coding Standards

- All DB operations must have a `context.Context` parameter (cancellable)
- No global state except the metrics registry, the pgxpool, and the session ring
- All errors logged with op type + duration even when not returned
- Payload pool: 100 pre-rendered templates at startup; every use goes through `GetMutatedPayload()` — never hand a raw template to Postgres
- Each worker has its own `*rand.Rand` (seeded at goroutine start) — no shared rand, no mutex on random number generation
- Pool stats exposed via `PoolCollector` (custom `prometheus.Collector`) — never call `pool.Stat()` inside worker hot paths
- UPDATE ops must rewrite the `metadata` JSONB column (Toast churn) — not just scalar fields
- No `ORDER BY random()` anywhere — all row targeting goes through the ring buffer
- INTERVAL parameters passed as `($1 * INTERVAL '1 minute')` — never interpolated as strings
- `audit_log.id` is UUID (`gen_random_uuid()`) — no BIGSERIAL, no sequence hotspot
- Graceful shutdown: on SIGTERM, stop workers, wait for in-flight ops, close pool
- Schema creation is idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`) and race-safe via advisory lock (`db.MigrateWithLock`)

---

## Scaling Cheat Sheet

| Goal | Action |
|---|---|
| More total concurrency | `kubectl scale deployment/pg-loadgen --replicas=N` |
| More concurrency per pod | Increase `WORKERS` env var |
| More write pressure | Increase `WRITE_PCT`, decrease others |
| More I/O pressure | Increase `MAX_PAYLOAD_KB` |
| Simulate paced load | Set `THINK_TIME_MS=50` |
| Max stress | `THINK_TIME_MS=0`, `WORKERS=50`, replicas=10 |
| Compare indexed vs raw throughput | Restart pods with `CREATE_INDEXES=true` (indexes built live on existing data) |
