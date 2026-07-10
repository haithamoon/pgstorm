# RFC: Pluggable Workload Profiles

**Status:** ACCEPTED — **Phases 1+2 implemented** (2026-07-10). Phase 3 (new profiles) pending.
**Goal:** turn the fixed schema + 6-op workload into swappable *profiles*, so the same
runner + metrics + Compose harness can benchmark other PostgreSQL capabilities. Unlocks
the P3 roadmap: a `pgvector` (vector-search) profile and a `queue` (Postgres-as-a-broker)
profile.

## Deviations from the original sketch (as-built)

Two decisions were made at implementation time to reduce risk and avoid speculative
generality (see the parent session for rationale):

1. **No `workload/profiles/oltpjsonb/` subpackage yet.** The `Profile` interface,
   registry, weight resolver, and the single `oltp-jsonb` profile all live in the
   `workload` package. Creating a subpackage for one implementation is speculative and
   multiplies mechanical risk; when profile #2 lands, all profiles move into
   `workload/profiles/` together.
2. **Engine stays in `workload`** (no separate `runner/` package) — same reasoning.
3. **Config split is partial.** Only the op *weights* moved to the generic resolver
   (`workload.ResolveWeights`, driven by `OpDef.EnvVar`). Other profile knobs
   (`MIN_PAYLOAD_KB`, `RING_SIZE`, `DELETE_BATCH_SIZE`, `READ_PAYLOAD`) remain in flat
   `config.Config` for now; move them into per-profile config when a second profile needs
   distinct knobs.
4. **`Schema` type lives in `db`** (migration input), so `db` does not import `workload`.

Everything below is the original design; §3's package layout is the eventual target, not
the current on-disk layout (see deviations #1/#2).

**Guiding constraint:** the refactor must preserve the current workload's behavior
*exactly* — it becomes the `oltp-jsonb` profile, and every existing unit test passes
unchanged. New profiles come only after the seam is in place and green.

---

## 1. The seam — what's shared vs. per-profile

**Shared runner (profile-agnostic) — stays generic:**
- Config loading of *generic* knobs; `PROFILE` selector; pool creation; advisory-lock
  migration; HTTP server (`/metrics`, `/healthz`, `/readyz`); the worker loop (select op
  by weight → `Execute` → record `ops_total`/`op_duration`/stats); signal handling +
  graceful shutdown.
- **DB-global metrics stay generic**: bgwriter/checkpointer, WAL, and wait-events come
  from `pg_stat_*` and are not schema-specific — no change needed.

**Per-profile (pluggable):**
- **Schema**: the `CREATE TABLE`/`CREATE INDEX` DDL and the list of table names to track
  for `table_stats`/`index_stats`.
- **Ops**: the operation set, each op's env-var name + default weight, and the op
  implementations (with any shared cross-worker state).
- **Config**: profile-specific env vars (e.g. `MIN_PAYLOAD_KB`, `RING_SIZE`,
  `DELETE_BATCH_SIZE`, `READ_PAYLOAD`, the `*_PCT` weights) — each profile reads its own.
- **(Optional) metrics**: a profile may register its own Prometheus collectors + poll
  loops (e.g. `queue` → queue-depth gauge).

---

## 2. Core interfaces (proposed)

Placed in a small leaf package `workload` (imported by the runner and by every profile;
no import cycle):

```go
// A Profile is a self-contained workload. Constructed once (holds shared state),
// then produces one Executor per worker goroutine.
type Profile interface {
    Name() string                              // "oltp-jsonb"
    Schema() Schema                            // DDL + tracked table names
    Ops() []OpDef                              // op names, env vars, default weights
    Init(cfg *config.Config) error             // read own env vars, build shared state
    NewExecutor(rng *rand.Rand) Executor       // per-worker; closes over shared state
    RegisterMetrics(reg prometheus.Registerer) // optional; may be a no-op
    RunStatsLoops(ctx context.Context, pool *pgxpool.Pool) // optional; may be a no-op
}

type Executor interface {
    Execute(ctx context.Context, op string) error
}

type Schema struct {
    Tables        []string // CREATE TABLE IF NOT EXISTS ... (each its own statement)
    Indexes       []string // CREATE INDEX CONCURRENTLY IF NOT EXISTS ...
    TrackedTables []string // relnames for table_stats / index_stats filters
}

type OpDef struct {
    Name          string // "insert"
    EnvVar        string // "WRITE_PCT"
    DefaultWeight int    // 35
}
```

**Weight resolution moves out of `config.go` into a generic helper** (`runner/weights.go`):
reads each `OpDef.EnvVar`, applies `DefaultWeight`, rejects negatives, and validates the
sum == 100 — exactly today's rule, but driven by the profile's `OpDef`s instead of six
hardcoded fields. Because `oltp-jsonb` declares the same env var names (`WRITE_PCT`, …)
and defaults, **existing configs keep working unchanged.**

**Registry** (`workload/profiles/registry.go`): `map[string]func() Profile`, so
`PROFILE=oltp-jsonb` (default) resolves to the profile. `main` looks it up; unknown name
→ fatal with the list of valid names.

---

## 3. Proposed package layout

```
pgstorm/
├── main.go                 — wire runner + registry.Get(cfg.Profile)
├── config/config.go        — generic knobs only + PROFILE; op-pct + payload knobs leave
├── runner/                 — NEW: profile-agnostic engine
│   ├── worker.go           — RunWorker + weighted SelectOp (moved from workload/)
│   ├── stats.go            — per-op stats; op names come from the resolved OpDefs
│   └── weights.go          — OpDef resolution + sum==100 validation
├── db/
│   ├── pool.go             — unchanged
│   └── schema.go           — advisory lock (unchanged) now runs Profile.Schema() DDL
├── workload/
│   ├── profile.go          — Profile/Executor/Schema/OpDef interfaces (leaf)
│   └── profiles/
│       ├── registry.go     — name → Profile factory
│       └── oltpjsonb/      — the CURRENT workload, behavior-identical
│           ├── profile.go  — implements Profile; owns SessionRing + payload pools
│           ├── ops.go      — the 6 ops (moved)
│           ├── ring.go     — SessionRing (moved)
│           ├── payload.go  — payload generation (moved)
│           └── schema.go   — the 3-table DDL, 8 indexes, tracked table names (moved)
└── metrics/
    ├── metrics.go          — generic ops/duration/workers (unchanged)
    ├── pool_collector.go   — unchanged
    ├── index_stats.go      — table/index loops parameterized by TrackedTables
    └── pg_stats.go         — DB-global bgwriter/WAL/wait (unchanged)
```

(`runner/` vs. keeping the engine in `workload/` is an open question — see §6.)

---

## 4. Phased execution (each phase independently reviewable + green)

**Phase 1 — introduce the seam, wrap current workload (no behavior change).**
- Add interfaces (`workload/profile.go`), registry, `runner/weights.go`.
- Move `ops.go`, `ring.go`, `payload.go` + the DDL into `workload/profiles/oltpjsonb/`,
  implementing `Profile`. Keep all logic identical.
- `main` selects the profile (default `oltp-jsonb`) and drives it through the interface.
- **Acceptance:** every existing unit test passes unchanged (moved with their code);
  `docker compose up` produces identical behavior; `git` diff is mostly moves.

**Phase 2 — parameterize the last schema-coupled bits.**
- `db/schema.go` runs `Profile.Schema()` DDL under the same advisory lock.
- `metrics/index_stats.go` filters by `Schema().TrackedTables` instead of the hardcoded
  `'sessions','events','audit_log'`.
- `config.go` slims to generic knobs; profile knobs read inside `oltpjsonb.Init`.

**Phase 3 — new profiles (separate PRs, the actual P3 payoff).**
- `profiles/pgvector/`: `vector(N)` embedding table, `ivfflat`/`hnsw` index build under
  load, ANN + exact-NN ops; extension-gated.
- `profiles/queue/`: `FOR UPDATE SKIP LOCKED` claim loop + `LISTEN`/`NOTIFY` variant;
  queue-depth/backlog metrics via `RunStatsLoops`.

---

## 5. Risk / impact assessment

- **Large but mostly mechanical.** ~15 files touched; the bulk is *moving* three files
  into a subpackage. The only genuinely new logic is the `Profile` interface + weight
  resolver. Import paths change (`pg-loadgen/workload` → `.../workload/profiles/oltpjsonb`).
- **Highest-risk edit:** the config op-pct generalization (removing 6 fields, moving
  validation). Mitigated by keeping identical env-var names/defaults and asserting sum==100
  in `runner/weights.go` with a focused test.
- **Backward compatibility:** `PROFILE` defaults to `oltp-jsonb`; all existing env vars
  and Compose/k8s manifests keep working. No user-facing break.
- **Advisory-lock DDL, pool sizing, and DB-global metrics are untouched.**

---

## 6. Open questions for you

1. **Engine package name:** `runner/` (my suggestion) vs. `engine/` vs. keep the loop in
   `workload/`. Cosmetic; I lean `runner/`.
2. **Config split:** move profile-specific env vars into each profile (my recommendation —
   clean ownership) vs. keep one flat `config.Config` with everything. The split is
   cleaner long-term but touches more of `config.go` now.
3. **First deliverable:** land Phases 1+2 as one behavior-preserving refactor PR (oltp
   tests green, zero behavior change) *before* any new profile — agreed? Or fold a minimal
   `queue` profile into the same PR to prove the seam end-to-end?
4. **Profiles per process:** keep one profile per process (selected by `PROFILE`, scale via
   replicas — matches today) vs. allow several concurrently. I recommend one-per-process.
5. **Doc home:** once approved, move this doc into the repo (e.g. `docs/`) as the RFC of
   record, or keep it git-ignored?
```
