// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Haitham Gadelrab
// Licensed under the Apache License, Version 2.0; see the LICENSE file.

package workload

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// opStats accumulates one op's stats for the current summary window.
//
// Latencies are kept as an exact frequency map (microseconds → count) rather
// than a fixed-bucket histogram. A bucketed histogram has to assume
// observations are spread uniformly inside each bucket, which is wrong for
// right-skewed latency data, and it cannot represent anything above its top
// bound at all — the old implementation reported every value over 30 s as
// exactly 30000 ms. The map is exact at both ends, which is a prerequisite for
// publishing run results.
//
// Memory is bounded by the number of *distinct* latencies seen in one window,
// and snapshot() resets it every SUMMARY_INTERVAL_SECS, so it does not grow
// over the life of a run. The Prometheus histogram in metrics/ is unaffected
// and stays bucketed — that is the correct model for a time series.
type opStats struct {
	count     int64
	errors    int64
	latencies map[int64]int64 // latency in microseconds → number of observations
}

// WorkerStats holds one worker's window stats. Owned by the worker goroutine;
// accessed by the collector only during a snapshot (brief mutex hold).
type WorkerStats struct {
	mu   sync.Mutex
	data map[string]*opStats
}

func newWorkerStats(ops []string) *WorkerStats {
	ws := &WorkerStats{data: make(map[string]*opStats, len(ops))}
	for _, op := range ops {
		ws.data[op] = &opStats{}
	}
	return ws
}

// Record is called by the worker goroutine after every op.
func (ws *WorkerStats) Record(op string, durationSec float64, err error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	s := ws.data[op]
	s.count++
	if err != nil {
		s.errors++
	}
	// Lazily allocated: snapshot() hands the previous map off to the collector
	// and leaves a nil one behind, so ops that see no traffic in a window cost
	// no allocation.
	if s.latencies == nil {
		s.latencies = make(map[int64]int64)
	}
	us := int64(durationSec * 1e6)
	if us < 0 {
		// A monotonic clock cannot produce this, but a negative value would sort
		// below every real observation and drag the percentiles down, so pin it.
		us = 0
	}
	s.latencies[us]++
}

// snapshot atomically copies and resets the window data.
//
// The struct copy carries the latencies map header, so the returned snapshot
// takes sole ownership of that map and zeroing the original leaves a nil map
// behind for Record to re-create. Caller and worker therefore never share a
// map — do not "optimise" this into a shallow copy that keeps the reference on
// both sides, or the collector would read a map the worker is still writing.
func (ws *WorkerStats) snapshot() map[string]opStats {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	out := make(map[string]opStats, len(ws.data))
	for op, s := range ws.data {
		out[op] = *s
		*s = opStats{}
	}
	return out
}

// StatsCollector aggregates stats from all workers and prints periodic summaries.
type StatsCollector struct {
	mu      sync.Mutex
	workers []*WorkerStats
	start   time.Time
	ops     []string // op names to track/print, from the active profile
}

func NewStatsCollector(ops []string) *StatsCollector {
	return &StatsCollector{start: time.Now(), ops: ops}
}

// NewWorkerStats creates a WorkerStats registered with the collector.
// Call once per worker before starting it.
func (c *StatsCollector) NewWorkerStats() *WorkerStats {
	ws := newWorkerStats(c.ops)
	c.mu.Lock()
	c.workers = append(c.workers, ws)
	c.mu.Unlock()
	return ws
}

// RunSummaryLoop prints a summary every interval until ctx is cancelled.
func (c *StatsCollector) RunSummaryLoop(ctx context.Context, interval time.Duration, pool *pgxpool.Pool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			c.print(t, interval, pool)
		}
	}
}

func (c *StatsCollector) print(now time.Time, window time.Duration, pool *pgxpool.Pool) {
	c.mu.Lock()
	workers := make([]*WorkerStats, len(c.workers))
	copy(workers, c.workers)
	c.mu.Unlock()

	// Merge snapshots from all workers
	merged := make(map[string]*opStats, len(c.ops))
	for _, op := range c.ops {
		merged[op] = &opStats{}
	}
	for _, ws := range workers {
		snap := ws.snapshot()
		for op, s := range snap {
			m := merged[op]
			m.count += s.count
			m.errors += s.errors
			for us, n := range s.latencies {
				if m.latencies == nil {
					m.latencies = make(map[int64]int64, len(s.latencies))
				}
				m.latencies[us] += n
			}
		}
	}

	windowSecs := window.Seconds()
	elapsed := now.Sub(c.start).Round(time.Second)

	var totalOps, totalErrors int64
	for _, s := range merged {
		totalOps += s.count
		totalErrors += s.errors
	}

	sep := strings.Repeat("━", 68)
	fmt.Printf("\n%s\n", sep)
	fmt.Printf("  30s summary [%s | +%s elapsed]\n", now.Format("15:04:05"), elapsed)
	fmt.Printf("  total  %6s ops   %.1f ops/s   errors: %d\n",
		commaf(totalOps), float64(totalOps)/windowSecs, totalErrors)
	fmt.Printf("  ┌──────────────┬────────┬─────────┬────────┬────────┬────────┐\n")
	fmt.Printf("  │ %-12s │ %6s │ %7s │ %6s │ %6s │ %6s │\n",
		"op", "count", "ops/s", "p50 ms", "p95 ms", "p99 ms")
	fmt.Printf("  ├──────────────┼────────┼─────────┼────────┼────────┼────────┤\n")
	for _, op := range c.ops {
		s := merged[op]
		p50 := percentile(s, 0.50)
		p95 := percentile(s, 0.95)
		p99 := percentile(s, 0.99)
		fmt.Printf("  │ %-12s │ %6d │ %7.1f │ %6.1f │ %6.1f │ %6.1f │\n",
			op, s.count, float64(s.count)/windowSecs, p50, p95, p99)
	}
	fmt.Printf("  └──────────────┴────────┴─────────┴────────┴────────┴────────┘\n")

	if pool != nil {
		stat := pool.Stat()
		fmt.Printf("  pool  acquired=%d  idle=%d  total=%d  max=%d  │  workers=%d\n",
			stat.AcquiredConns(), stat.IdleConns(), stat.TotalConns(), stat.MaxConns(), len(workers))
	}
	fmt.Printf("%s\n\n", sep)
}

// percentile returns the exact q-th percentile (q in 0–1) in milliseconds,
// using the nearest-rank definition: the smallest observed value v such that at
// least ceil(q*N) of the N observations are ≤ v. Because every observation is
// retained, the result is a real measured latency rather than an estimate — no
// interpolation, and no ceiling to clamp against.
//
// Returns 0 for an empty window.
func percentile(s *opStats, q float64) float64 {
	var total int64
	for _, n := range s.latencies {
		total += n
	}
	if total == 0 {
		return 0
	}

	rank := int64(math.Ceil(q * float64(total)))
	if rank < 1 {
		rank = 1 // q ≤ 0 → the minimum observation
	} else if rank > total {
		rank = total // q ≥ 1 → the maximum observation
	}

	keys := make([]int64, 0, len(s.latencies))
	for us := range s.latencies {
		keys = append(keys, us)
	}
	slices.Sort(keys)

	// Walk every key but the last. If the rank is not reached by then, the
	// remaining observations are all at the largest latency, so that is the
	// answer — which also keeps this function free of an unreachable branch.
	var cum int64
	for _, us := range keys[:len(keys)-1] {
		cum += s.latencies[us]
		if cum >= rank {
			return float64(us) / 1000 // µs → ms
		}
	}
	return float64(keys[len(keys)-1]) / 1000
}

func commaf(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	rem := len(s) % 3
	if rem > 0 {
		b.WriteString(s[:rem])
	}
	for i := rem; i < len(s); i += 3 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}
