// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Haitham Gadelrab
// Licensed under the Apache License, Version 2.0; see the LICENSE file.

package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/haithamoon/pgstorm/config"
)

// A run's console summary is destructive: every window is snapshotted, printed
// and discarded. Without a durable artifact the only record of a run is
// scrollback plus whatever Prometheus retained, which makes results impossible
// to cite or compare. RunResult is that artifact — one self-describing JSON
// file per run, holding the whole-run totals and the settings that produced
// them.

// LatencyStats summarises one op's latency distribution, in milliseconds. All
// values derive from the exact observation set, so they are real measured
// latencies rather than bucket estimates.
//
// Successful ops only. Failures return in microseconds without doing the work,
// so including them would make these numbers improve as the database degrades.
type LatencyStats struct {
	MinMS  float64 `json:"min_ms"`
	MeanMS float64 `json:"mean_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	P999MS float64 `json:"p999_ms"`
	MaxMS  float64 `json:"max_ms"`
}

// OpResult is the whole-run outcome for a single operation.
//
// OpsPerSec counts *attempts*, failures included, so a broken database reports
// a high rate next to a high error count. Latency is present iff count >
// errors: when every attempt failed there is nothing to summarise, and emitting
// an all-zero block would read as "instant", which is the same lie this
// separation exists to remove. An absent latency key therefore means "no
// successful op was measured", not "this version omits the field".
type OpResult struct {
	Op        string        `json:"op"`
	Count     int64         `json:"count"`
	Errors    int64         `json:"errors"`
	OpsPerSec float64       `json:"ops_per_sec"`
	Latency   *LatencyStats `json:"latency,omitempty"`
}

// IntervalOp is one operation's activity within a single summary window.
//
// Deliberately slimmer than OpResult: count, rate and the three percentiles the
// console prints. Min, mean, p999 and max are whole-run questions, and carrying
// them per window would multiply the size of a long run's series for little
// added insight.
type IntervalOp struct {
	Op        string  `json:"op"`
	Count     int64   `json:"count"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50MS     float64 `json:"p50_ms"`
	P95MS     float64 `json:"p95_ms"`
	P99MS     float64 `json:"p99_ms"`
}

// IntervalStats is one summary window, letting a reader see *when* during a run
// something changed — throughput sagging as the dataset grows, or latency
// stepping up when autovacuum starts work — without standing up Prometheus.
//
// Boundaries are seconds elapsed since the run started. Rates use the actual
// measured span rather than the configured interval, since the final window is
// shorter than the rest and ticker delivery drifts.
type IntervalStats struct {
	StartSeconds float64      `json:"start_seconds"`
	EndSeconds   float64      `json:"end_seconds"`
	Ops          int64        `json:"ops"`
	Errors       int64        `json:"errors"`
	OpsPerSec    float64      `json:"ops_per_sec"`
	Operations   []IntervalOp `json:"operations"`
}

// RunTotals aggregates across every operation.
type RunTotals struct {
	Ops       int64   `json:"ops"`
	Errors    int64   `json:"errors"`
	OpsPerSec float64 `json:"ops_per_sec"`
}

// RunConfig records the settings that materially change what a run measures.
// Deliberately not the whole Config: ports, poll intervals and timeouts do not
// affect the numbers, and including them would invite spurious diffs when
// comparing two results.
type RunConfig struct {
	Profile         string         `json:"profile"`
	Workers         int            `json:"workers"`
	MinPayloadKB    int            `json:"min_payload_kb"`
	MaxPayloadKB    int            `json:"max_payload_kb"`
	ToastPct        int            `json:"toast_pct"`
	ReadPayload     bool           `json:"read_payload"`
	CreateIndexes   bool           `json:"create_indexes"`
	RingSize        int            `json:"ring_size"`
	DeleteBatchSize int            `json:"delete_batch_size"`
	UserPoolSize    int            `json:"user_pool_size"`
	ActorPoolSize   int            `json:"actor_pool_size"`
	ThinkTimeMs     int            `json:"think_time_ms"`
	RunDurationSecs int            `json:"run_duration_secs"`
	OpWeights       map[string]int `json:"op_weights"`
}

// DatasetSize is how much data the tracked tables hold at one point in time.
// Sizes come from pg_total_relation_size, so they include TOAST and index
// storage — for this workload the out-of-line data is most of the point, and
// heap-only figures would understate it several-fold.
type DatasetSize struct {
	TotalBytes int64            `json:"total_bytes"`
	LiveTuples int64            `json:"live_tuples"`
	TableBytes map[string]int64 `json:"table_bytes"`
}

// DatasetSnapshot brackets a run with the dataset it operated on.
//
// This matters more here than for most load generators: pgstorm starts from
// whatever is already in the database and grows as it runs, so the volume of
// data is a function of how long the run lasted and how fast the machine is.
// Without these numbers a result cannot be compared to another one, because
// the two were not measuring the same database.
type DatasetSnapshot struct {
	Start       DatasetSize `json:"start"`
	End         DatasetSize `json:"end"`
	GrowthBytes int64       `json:"growth_bytes"`
}

// ServerInfo describes the Postgres instance the run executed against.
//
// Without it a result is not comparable to another one: whether the server was
// PG 14 or 17, had 128 MB or 16 GB of shared_buffers, or was running with
// synchronous_commit off, moves the numbers far more than most of the pgstorm
// knobs recorded in RunConfig.
type ServerInfo struct {
	Version string `json:"version"`
	// Settings maps a setting name to its value, with the unit appended where
	// Postgres reports one ("16384 8kB"). Deliberately a curated subset — see
	// capturedSettings.
	Settings map[string]string `json:"settings"`
}

// capturedSettings is an allow-list rather than everything pg_settings knows:
// a full dump is several hundred rows and would bury the handful that matter.
// Each entry here measurably changes what pgstorm reports.
//
// Never add anything carrying credentials. PG_DSN holds the password and must
// not reach a result file, which people share.
var capturedSettings = []string{
	"server_version",

	"shared_buffers", "effective_cache_size", "work_mem", "max_connections",

	// full_page_writes matters especially: pgstorm exports FPI counts, and
	// turning it off changes them fundamentally.
	"max_wal_size", "min_wal_size", "wal_compression", "full_page_writes",

	"checkpoint_timeout", "checkpoint_completion_target",
	"synchronous_commit",

	"autovacuum", "autovacuum_naptime", "autovacuum_vacuum_scale_factor",

	// default_toast_compression is the one most specific to this workload:
	// pgstorm's payloads are built to defeat pglz, so a server using lz4 is
	// exercising TOAST differently than the design assumes.
	"default_toast_compression",
}

// SnapshotServerInfo reads the version and the curated settings from the
// server under test.
//
// Settings named here but absent from the server (they may be
// version-specific) simply do not come back, which is the intended
// degradation — no error, just a missing key.
func SnapshotServerInfo(ctx context.Context, pool DBPool) (ServerInfo, error) {
	out := ServerInfo{Settings: map[string]string{}}
	rows, err := pool.Query(ctx, `
		SELECT name, setting, COALESCE(unit, '')
		FROM pg_settings
		WHERE name = ANY($1)
	`, capturedSettings)
	if err != nil {
		return ServerInfo{}, fmt.Errorf("query server settings: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, setting, unit string
		if err := rows.Scan(&name, &setting, &unit); err != nil {
			return ServerInfo{}, fmt.Errorf("scan server settings: %w", err)
		}
		if name == "server_version" {
			// Surfaced as its own field; no point repeating it in the map.
			out.Version = setting
			continue
		}
		if unit != "" {
			setting += " " + unit
		}
		out.Settings[name] = setting
	}
	if err := rows.Err(); err != nil {
		return ServerInfo{}, fmt.Errorf("read server settings: %w", err)
	}
	return out, nil
}

// How a run ended. Recorded because the primary workflow is measuring, tuning,
// then measuring again: an "after" run cut short by Ctrl-C would otherwise look
// just like one that ran its course, and quietly invalidate the comparison.
const (
	// StoppedByDuration means the configured RUN_DURATION_SECS elapsed.
	StoppedByDuration = "duration"
	// StoppedBySignal means SIGINT or SIGTERM arrived first. Always the case for
	// an unbounded run, which is not a fault — it was never going to stop on its
	// own, which is why this is not modelled as a "completed" boolean.
	StoppedBySignal = "signal"
)

// RunMeta carries what is known about a run beyond its measurements. Grouped so
// BuildRunResult keeps a readable signature as these accumulate.
type RunMeta struct {
	// Dataset and Server are nil when the corresponding measurement failed;
	// their fields are then omitted from the document rather than zeroed.
	Dataset   *DatasetSnapshot
	Server    *ServerInfo
	StoppedBy string
	// Timeseries comes from StatsCollector.Finalize.
	Timeseries []IntervalStats
}

// RunResult is the top-level document written at the end of a run.
type RunResult struct {
	Profile         string     `json:"profile"`
	StartedAt       time.Time  `json:"started_at"`
	EndedAt         time.Time  `json:"ended_at"`
	DurationSeconds float64    `json:"duration_seconds"`
	StoppedBy       string     `json:"stopped_by,omitempty"`
	Config          RunConfig  `json:"config"`
	Totals          RunTotals  `json:"totals"`
	Operations      []OpResult `json:"operations"`

	// Timeseries is every summary window in order. Its op counts sum exactly to
	// Totals.Ops — the same drain path feeds both, so a mismatch would mean
	// observations were lost or double-counted.
	Timeseries []IntervalStats `json:"timeseries,omitempty"`

	// Dataset is omitted rather than zeroed when the size could not be read, so
	// a missing measurement is never mistaken for an empty database.
	Dataset *DatasetSnapshot `json:"dataset,omitempty"`

	// Server is omitted for the same reason: absent means "not measured", not
	// "a server with no settings".
	Server *ServerInfo `json:"server,omitempty"`
}

// SnapshotDataset reads the current size of the tracked tables.
//
// Shared state: with several replicas against one database every process sees
// the same totals, unlike the per-process operation counts.
func SnapshotDataset(ctx context.Context, pool DBPool, tables []string) (DatasetSize, error) {
	out := DatasetSize{TableBytes: map[string]int64{}}
	rows, err := pool.Query(ctx, `
		SELECT relname, pg_total_relation_size(relid), n_live_tup
		FROM pg_stat_user_tables
		WHERE schemaname = 'public'
		  AND relname = ANY($1)
	`, tables)
	if err != nil {
		return DatasetSize{}, fmt.Errorf("query dataset size: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var bytes, live int64
		if err := rows.Scan(&name, &bytes, &live); err != nil {
			return DatasetSize{}, fmt.Errorf("scan dataset size: %w", err)
		}
		out.TableBytes[name] = bytes
		out.TotalBytes += bytes
		out.LiveTuples += live
	}
	if err := rows.Err(); err != nil {
		return DatasetSize{}, fmt.Errorf("read dataset size: %w", err)
	}
	return out, nil
}

// BuildRunResult assembles the document from a finalised set of totals. Pure:
// it performs no I/O and reads no clocks, so it is fully testable.
//
// Rates are computed over the wall-clock run duration. A non-positive duration
// (a run that ended in the same instant it started, which only happens in
// tests) yields zero rates rather than an infinity that would not survive a
// JSON round-trip.
// Fields of meta that were not measured are omitted from the document rather
// than written as zeros — see RunMeta.
func BuildRunResult(
	totals map[string]opStats,
	started, ended time.Time,
	cfg *config.Config,
	ops []WeightedOp,
	meta RunMeta,
) RunResult {
	duration := ended.Sub(started).Seconds()
	rate := func(n int64) float64 {
		if duration <= 0 {
			return 0
		}
		return float64(n) / duration
	}

	weights := make(map[string]int, len(ops))
	for _, o := range ops {
		weights[o.Name] = o.Weight
	}

	// Report ops in the profile's declared order so two results line up
	// field-for-field when diffed.
	names := make([]string, 0, len(totals))
	for op := range totals {
		names = append(names, op)
	}
	sort.Strings(names)

	var t RunTotals
	operations := make([]OpResult, 0, len(names))
	for _, op := range names {
		s := totals[op]
		t.Ops += s.count
		t.Errors += s.errors
		// Presence is decided by the observation count, never by comparing the
		// result against LatencyStats{}: Record truncates sub-microsecond
		// durations to 0 µs, so a genuinely measured op can produce an all-zero
		// struct that must still be reported.
		var latency *LatencyStats
		if s.samples() > 0 {
			ls := s.latencyStats()
			latency = &ls
		}
		operations = append(operations, OpResult{
			Op:        op,
			Count:     s.count,
			Errors:    s.errors,
			OpsPerSec: rate(s.count),
			Latency:   latency,
		})
	}
	t.OpsPerSec = rate(t.Ops)

	return RunResult{
		Profile:         cfg.Profile,
		StartedAt:       started.UTC(),
		EndedAt:         ended.UTC(),
		DurationSeconds: duration,
		Config: RunConfig{
			Profile:         cfg.Profile,
			Workers:         cfg.Workers,
			MinPayloadKB:    cfg.MinPayloadKB,
			MaxPayloadKB:    cfg.MaxPayloadKB,
			ToastPct:        cfg.ToastPct,
			ReadPayload:     cfg.ReadPayload,
			CreateIndexes:   cfg.CreateIndexes,
			RingSize:        cfg.RingSize,
			DeleteBatchSize: cfg.DeleteBatchSize,
			UserPoolSize:    cfg.UserPoolSize,
			ActorPoolSize:   cfg.ActorPoolSize,
			ThinkTimeMs:     cfg.ThinkTimeMs,
			RunDurationSecs: cfg.RunDurationSecs,
			OpWeights:       weights,
		},
		StoppedBy:  meta.StoppedBy,
		Timeseries: meta.Timeseries,
		Totals:     t,
		Operations: operations,
		Dataset:    meta.Dataset,
		Server:     meta.Server,
	}
}

// WriteRunResult writes r to path as indented JSON.
//
// The write goes to a temporary file in the same directory and is then renamed
// over the destination, so a reader never observes a half-written result and a
// crash mid-write cannot corrupt a previous one. Parent directories are created
// if missing.
func WriteRunResult(path string, r RunResult) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create result directory %s: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, ".pgstorm-result-*.json")
	if err != nil {
		return fmt.Errorf("create temp result file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename succeeds

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write result: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close result: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename result into place: %w", err)
	}
	return nil
}
