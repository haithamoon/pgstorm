// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Haitham Gadelrab
// Licensed under the Apache License, Version 2.0; see the LICENSE file.

package workload

import (
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
type OpResult struct {
	Op        string       `json:"op"`
	Count     int64        `json:"count"`
	Errors    int64        `json:"errors"`
	OpsPerSec float64      `json:"ops_per_sec"`
	Latency   LatencyStats `json:"latency"`
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

// RunResult is the top-level document written at the end of a run.
type RunResult struct {
	Profile         string     `json:"profile"`
	StartedAt       time.Time  `json:"started_at"`
	EndedAt         time.Time  `json:"ended_at"`
	DurationSeconds float64    `json:"duration_seconds"`
	Config          RunConfig  `json:"config"`
	Totals          RunTotals  `json:"totals"`
	Operations      []OpResult `json:"operations"`
}

// BuildRunResult assembles the document from a finalised set of totals. Pure:
// it performs no I/O and reads no clocks, so it is fully testable.
//
// Rates are computed over the wall-clock run duration. A non-positive duration
// (a run that ended in the same instant it started, which only happens in
// tests) yields zero rates rather than an infinity that would not survive a
// JSON round-trip.
func BuildRunResult(
	totals map[string]opStats,
	started, ended time.Time,
	cfg *config.Config,
	ops []WeightedOp,
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
		operations = append(operations, OpResult{
			Op:        op,
			Count:     s.count,
			Errors:    s.errors,
			OpsPerSec: rate(s.count),
			Latency:   s.latencyStats(),
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
		Totals:     t,
		Operations: operations,
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
