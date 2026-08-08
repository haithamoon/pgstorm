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
//
// Invariant: sum(latencies) == count - errors, at every level (worker, window,
// run total). Record maintains it, mergeInto preserves it under addition, and
// snapshot preserves it under the struct copy. Only *successful* ops contribute
// a latency — see Record for why — so an op that failed every time leaves
// latencies nil, and that is how result.go decides to omit the latency block.
type opStats struct {
	count     int64
	errors    int64
	latencies map[int64]int64 // latency in microseconds → number of observations
}

// samples returns the number of latency observations, i.e. count - errors.
// Callers use it to distinguish "no successful op was measured" from "every
// measurement happened to be zero", which an all-zero LatencyStats cannot.
//
// Value receiver on purpose: the snapshot and total maps hold opStats by value,
// and a map index is not addressable, so a pointer receiver would not be
// callable on the very values this is meant to interrogate.
func (s opStats) samples() int64 {
	var n int64
	for _, c := range s.latencies {
		n += c
	}
	return n
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
		// A failed op is counted but contributes no latency.
		//
		// This used to be the other way round, on the reasoning that an op which
		// failed slowly is still evidence about how the server behaved. True in
		// isolation, and wrong in aggregate: failures are overwhelmingly *fast*
		// — "connection refused" or "too many clients" returns in microseconds
		// without touching data — and with no backoff a failing database emits
		// them in a flood. Mixed into one population they drag every percentile
		// down, so the reported p99 IMPROVES as the server gets worse. For a
		// tool whose whole job is comparing a before-tuning run against an
		// after-tuning one, that inverts the conclusion.
		//
		// The failure is not hidden: count and errors are both reported, and
		// count > errors is exactly the condition under which a latency block
		// is emitted at all.
		s.errors++
		return
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

	// total accumulates every window and is never reset, so the whole run
	// survives to be written out at the end. The per-window snapshot that
	// drives the console summary is destructive, so without this nothing but
	// scrollback outlives the process.
	total map[string]*opStats

	// intervals is one entry per drained window, in order, so a result can show
	// when during the run something changed rather than only the average.
	// Bounded by run length: roughly 700 bytes per SUMMARY_INTERVAL_SECS.
	intervals []IntervalStats
	// lastBoundary is where the previous interval ended, so each one is measured
	// over its true span rather than the configured interval.
	lastBoundary time.Time
}

func NewStatsCollector(ops []string) *StatsCollector {
	total := make(map[string]*opStats, len(ops))
	for _, op := range ops {
		total[op] = &opStats{}
	}
	now := time.Now()
	return &StatsCollector{start: now, lastBoundary: now, ops: ops, total: total}
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

// mergeInto adds src's counts and latency observations into dst.
func mergeInto(dst, src *opStats) {
	dst.count += src.count
	dst.errors += src.errors
	for us, n := range src.latencies {
		if dst.latencies == nil {
			dst.latencies = make(map[int64]int64, len(src.latencies))
		}
		dst.latencies[us] += n
	}
}

// drain snapshots every worker and merges the result into one map, additionally
// folding it into the never-reset run total. This is the only place worker
// windows are consumed, so every observation lands in the total exactly once.
func (c *StatsCollector) drain(now time.Time) map[string]*opStats {
	// Lifting the windows out of the workers and folding them into the total
	// must be one atomic step. Releasing the lock in between allows a concurrent
	// drain (the summary goroutine's last print racing main's Finalize, both
	// woken by the same expiring context) to observe a total that is missing a
	// window which has already been taken from the workers — silently dropping a
	// whole summary interval from the result. Held across ws.snapshot(), which
	// takes each worker's own lock; the order is always c.mu → ws.mu and Record
	// only ever takes ws.mu, so there is no cycle.
	c.mu.Lock()
	defer c.mu.Unlock()

	merged := make(map[string]*opStats, len(c.ops))
	for _, op := range c.ops {
		merged[op] = &opStats{}
	}
	for _, ws := range c.workers {
		for op, s := range ws.snapshot() {
			if m, ok := merged[op]; ok {
				mergeInto(m, &s)
			}
		}
	}
	for op, m := range merged {
		if t, ok := c.total[op]; ok {
			mergeInto(t, m)
		}
	}
	c.recordIntervalLocked(now, merged)
	return merged
}

// minIntervalSeconds is the shortest span that gets its own entry in the
// series. Anything briefer is a drain-timing artifact rather than a real
// observation window.
//
// A var rather than a const only so tests can lower it and exercise the folding
// logic without real second-long sleeps. Not configurable at runtime.
var minIntervalSeconds = 1.0

// recordIntervalLocked appends this window to the series. Called from drain so
// that the trailing window Finalize consumes is captured too — otherwise the
// series would not sum to the totals.
//
// Caller must hold c.mu.
func (c *StatsCollector) recordIntervalLocked(now time.Time, merged map[string]*opStats) {
	var ops, errs int64
	for _, s := range merged {
		ops += s.count
		errs += s.errors
	}
	// A run ending just after a summary drains an empty window; recording it
	// would append a zero-length, zero-op entry that means nothing.
	if ops == 0 {
		return
	}

	startSecs := c.lastBoundary.Sub(c.start).Seconds()
	endSecs := now.Sub(c.start).Seconds()
	span := endSecs - startSecs

	// Finalize drains microseconds after the summary loop's last print, so the
	// few ops that landed in between would otherwise become an interval of
	// near-zero length — and dividing by it invents a throughput spike that
	// never happened (observed live: 4 ops over ~1 ms reported as 2971 ops/sec).
	// Fold such a sliver into the preceding interval instead of dropping it,
	// which keeps the series summing exactly to the totals. Percentiles are left
	// as they were: the sliver is a rounding error against the window it joins.
	if span < minIntervalSeconds && len(c.intervals) > 0 {
		prev := &c.intervals[len(c.intervals)-1]
		prev.Ops += ops
		prev.Errors += errs
		prev.EndSeconds = endSecs
		if prevSpan := prev.EndSeconds - prev.StartSeconds; prevSpan > 0 {
			prev.OpsPerSec = float64(prev.Ops) / prevSpan
			for i := range prev.Operations {
				if s, ok := merged[prev.Operations[i].Op]; ok {
					prev.Operations[i].Count += s.count
				}
				prev.Operations[i].OpsPerSec = float64(prev.Operations[i].Count) / prevSpan
			}
		}
		c.lastBoundary = now
		return
	}
	rate := func(n int64) float64 {
		if span <= 0 {
			return 0
		}
		return float64(n) / span
	}

	// Same order as c.ops so consecutive intervals line up when diffed.
	operations := make([]IntervalOp, 0, len(c.ops))
	for _, op := range c.ops {
		s, ok := merged[op]
		if !ok || s.count == 0 {
			continue
		}
		keys, total := s.sortedKeys()
		operations = append(operations, IntervalOp{
			Op:        op,
			Count:     s.count,
			OpsPerSec: rate(s.count),
			P50MS:     quantileFrom(s.latencies, keys, total, 0.50),
			P95MS:     quantileFrom(s.latencies, keys, total, 0.95),
			P99MS:     quantileFrom(s.latencies, keys, total, 0.99),
		})
	}

	c.intervals = append(c.intervals, IntervalStats{
		StartSeconds: startSecs,
		EndSeconds:   endSecs,
		Ops:          ops,
		Errors:       errs,
		OpsPerSec:    rate(ops),
		Operations:   operations,
	})
	c.lastBoundary = now
}

// Finalize consumes any window that has accumulated since the last summary and
// returns the whole-run totals. Call it after all workers have stopped and the
// summary loop has exited; calling it twice is safe but the second call adds
// nothing.
func (c *StatsCollector) Finalize() (totals map[string]opStats, intervals []IntervalStats, started, ended time.Time) {
	c.drain(time.Now()) // fold the trailing partial window into the total and series

	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]opStats, len(c.total))
	for op, s := range c.total {
		out[op] = *s
	}
	// Copied so a later drain cannot mutate a series the caller is writing out.
	series := slices.Clone(c.intervals)
	return out, series, c.start, time.Now()
}

// latencyStats derives the full latency summary in a single sort. Every value
// is a real observation (or, for mean, an exact average of them) — nothing here
// is interpolated or clamped.
func (s *opStats) latencyStats() LatencyStats {
	keys, total := s.sortedKeys()
	if total == 0 {
		return LatencyStats{}
	}
	var weightedSum int64
	for _, us := range keys {
		weightedSum += us * s.latencies[us]
	}
	return LatencyStats{
		MinMS:  float64(keys[0]) / 1000,
		MeanMS: float64(weightedSum) / float64(total) / 1000,
		P50MS:  quantileFrom(s.latencies, keys, total, 0.50),
		P95MS:  quantileFrom(s.latencies, keys, total, 0.95),
		P99MS:  quantileFrom(s.latencies, keys, total, 0.99),
		P999MS: quantileFrom(s.latencies, keys, total, 0.999),
		MaxMS:  float64(keys[len(keys)-1]) / 1000,
	}
}

func (c *StatsCollector) print(now time.Time, window time.Duration, pool *pgxpool.Pool) {
	merged := c.drain(now)

	c.mu.Lock()
	workerCount := len(c.workers)
	c.mu.Unlock()

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
	fmt.Printf("  ┌──────────────┬────────┬────────┬─────────┬────────┬────────┬────────┐\n")
	fmt.Printf("  │ %-12s │ %6s │ %6s │ %7s │ %6s │ %6s │ %6s │\n",
		"op", "count", "err", "ops/s", "p50 ms", "p95 ms", "p99 ms")
	fmt.Printf("  ├──────────────┼────────┼────────┼─────────┼────────┼────────┼────────┤\n")
	for _, op := range c.ops {
		s := merged[op]
		fmt.Printf("  │ %-12s │ %6d │ %6d │ %7.1f │ %6s │ %6s │ %6s │\n",
			op, s.count, s.errors, float64(s.count)/windowSecs,
			latencyCell(s, 0.50), latencyCell(s, 0.95), latencyCell(s, 0.99))
	}
	fmt.Printf("  └──────────────┴────────┴────────┴─────────┴────────┴────────┴────────┘\n")

	if pool != nil {
		stat := pool.Stat()
		fmt.Printf("  pool  acquired=%d  idle=%d  total=%d  max=%d  │  workers=%d\n",
			stat.AcquiredConns(), stat.IdleConns(), stat.TotalConns(), stat.MaxConns(), workerCount)
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
// latencyCell renders one percentile for the console table, or "—" when the op
// has no successful observations to summarise. Printing 0.0 there would read as
// "instant" for what is in fact an op that failed every time — the same
// misreading the JSON avoids by omitting the latency block entirely. An idle
// window and a wholly-failed one both render "—"; the count and err cells
// alongside distinguish them.
func latencyCell(s *opStats, q float64) string {
	if s.samples() == 0 {
		return "—"
	}
	return fmt.Sprintf("%.1f", percentile(s, q))
}

func percentile(s *opStats, q float64) float64 {
	keys, total := s.sortedKeys()
	return quantileFrom(s.latencies, keys, total, q)
}

// sortedKeys returns the distinct latencies in ascending order plus the total
// observation count, so callers that need several quantiles only sort once.
func (s *opStats) sortedKeys() ([]int64, int64) {
	keys := make([]int64, 0, len(s.latencies))
	var total int64
	for us, n := range s.latencies {
		keys = append(keys, us)
		total += n
	}
	slices.Sort(keys)
	return keys, total
}

// quantileFrom is the shared nearest-rank implementation. Kept separate so
// percentile() and latencyStats() can never drift apart.
func quantileFrom(latencies map[int64]int64, keys []int64, total int64, q float64) float64 {
	if total == 0 {
		return 0
	}

	rank := int64(math.Ceil(q * float64(total)))
	if rank < 1 {
		rank = 1 // q ≤ 0 → the minimum observation
	} else if rank > total {
		rank = total // q ≥ 1 → the maximum observation
	}

	// Walk every key but the last. If the rank is not reached by then, the
	// remaining observations are all at the largest latency, so that is the
	// answer — which also keeps this function free of an unreachable branch.
	var cum int64
	for _, us := range keys[:len(keys)-1] {
		cum += latencies[us]
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
