// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Haitham Gadelrab
// Licensed under the Apache License, Version 2.0; see the LICENSE file.

package workload

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testOps is the op set used across the stats tests (the oltp-jsonb ops).
var testOps = []string{OpInsert, OpReadSimple, OpReadJoin, OpUpdate, OpDelete, OpReadByIP}

func approxEq(a, b float64) bool { return a-b < 1e-9 && b-a < 1e-9 }

// statsWith builds an opStats from (latencyMs, count) pairs, mirroring what a
// window of Record calls would produce.
func statsWith(pairs ...[2]float64) *opStats {
	s := &opStats{latencies: map[int64]int64{}}
	for _, p := range pairs {
		us := int64(p[0] * 1000)
		s.latencies[us] += int64(p[1])
		s.count += int64(p[1])
	}
	return s
}

// ── WorkerStats.Record ───────────────────────────────────────────────────────

func TestRecord_storesExactMicroseconds(t *testing.T) {
	tests := []struct {
		name   string
		durSec float64
		wantUS int64
	}{
		{"zero", 0, 0},
		{"123 microseconds", 0.000123, 123},
		{"half a millisecond", 0.0005, 500},
		{"one millisecond", 0.001, 1000},
		{"50 ms", 0.050, 50000},
		{"30 s (old top bound)", 30.0, 30_000_000},
		{"45 s (over the old ceiling)", 45.0, 45_000_000},
		{"120 s (far over the old ceiling)", 120.0, 120_000_000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ws := newWorkerStats(testOps)
			ws.Record(OpInsert, tc.durSec, nil)
			s := ws.data[OpInsert]
			if got := s.latencies[tc.wantUS]; got != 1 {
				t.Errorf("latencies[%d]: want 1, got %d (map=%v)", tc.wantUS, got, s.latencies)
			}
			if len(s.latencies) != 1 {
				t.Errorf("want exactly one distinct latency, got %d", len(s.latencies))
			}
		})
	}
}

func TestRecord_duplicateLatenciesAccumulate(t *testing.T) {
	ws := newWorkerStats(testOps)
	for range 5 {
		ws.Record(OpInsert, 0.002, nil)
	}
	s := ws.data[OpInsert]
	if len(s.latencies) != 1 {
		t.Fatalf("want 1 distinct key, got %d", len(s.latencies))
	}
	if got := s.latencies[2000]; got != 5 {
		t.Errorf("latencies[2000]: want 5, got %d", got)
	}
	if s.count != 5 {
		t.Errorf("count: want 5, got %d", s.count)
	}
}

func TestRecord_negativeDurationPinnedToZero(t *testing.T) {
	// A monotonic clock cannot produce a negative duration, but if one ever
	// arrived it must not sort below real observations and drag percentiles down.
	ws := newWorkerStats(testOps)
	ws.Record(OpInsert, -0.5, nil)
	s := ws.data[OpInsert]
	if got := s.latencies[0]; got != 1 {
		t.Errorf("negative duration should be pinned to 0 us, got map=%v", s.latencies)
	}
	for us := range s.latencies {
		if us < 0 {
			t.Errorf("negative key %d stored", us)
		}
	}
}

func TestRecord_errorIncrements(t *testing.T) {
	ws := newWorkerStats(testOps)
	ws.Record(OpInsert, 0.001, nil)
	ws.Record(OpInsert, 0.001, fmt.Errorf("oops"))
	s := ws.data[OpInsert]
	if s.count != 2 {
		t.Errorf("count: want 2, got %d", s.count)
	}
	if s.errors != 1 {
		t.Errorf("errors: want 1, got %d", s.errors)
	}
	// Errored ops still contribute their latency — an op that failed slowly is
	// still evidence about how the server behaved.
	if got := s.latencies[1000]; got != 2 {
		t.Errorf("both observations should be recorded, got latencies[1000]=%d", got)
	}
}

func TestRecord_allOpsTracked(t *testing.T) {
	ws := newWorkerStats(testOps)
	for _, op := range testOps {
		ws.Record(op, 0.001, nil)
	}
	for _, op := range testOps {
		if ws.data[op].count != 1 {
			t.Errorf("op=%s: count should be 1, got %d", op, ws.data[op].count)
		}
	}
}

func TestRecord_reinitialisesMapAfterSnapshot(t *testing.T) {
	// snapshot() hands the map to the collector and leaves nil behind, so the
	// next Record must lazily re-create it rather than panicking on a nil map.
	ws := newWorkerStats(testOps)
	ws.Record(OpInsert, 0.001, nil)
	_ = ws.snapshot()

	if ws.data[OpInsert].latencies != nil {
		t.Fatal("snapshot should leave a nil map behind")
	}
	ws.Record(OpInsert, 0.002, nil) // must not panic
	s := ws.data[OpInsert]
	if got := s.latencies[2000]; got != 1 {
		t.Errorf("post-snapshot Record lost: latencies=%v", s.latencies)
	}
	if len(s.latencies) != 1 {
		t.Errorf("post-snapshot window should hold only the new observation, got %v", s.latencies)
	}
}

// ── WorkerStats.snapshot ─────────────────────────────────────────────────────

func TestSnapshot_returnsDataAndResets(t *testing.T) {
	ws := newWorkerStats(testOps)
	ws.Record(OpInsert, 0.001, nil)
	ws.Record(OpInsert, 0.050, nil)
	ws.Record(OpDelete, 0.100, fmt.Errorf("e"))

	snap := ws.snapshot()
	if snap[OpInsert].count != 2 {
		t.Errorf("snapshot insert count: want 2, got %d", snap[OpInsert].count)
	}
	if snap[OpDelete].errors != 1 {
		t.Errorf("snapshot delete errors: want 1, got %d", snap[OpDelete].errors)
	}
	if len(snap[OpInsert].latencies) != 2 {
		t.Errorf("snapshot should carry both insert latencies, got %v", snap[OpInsert].latencies)
	}

	// Second snapshot must return zeros — data was reset.
	snap2 := ws.snapshot()
	for _, op := range testOps {
		s := snap2[op]
		if s.count != 0 || s.errors != 0 || len(s.latencies) != 0 {
			t.Errorf("op=%s: expected zero after snapshot, got count=%d errors=%d latencies=%v",
				op, s.count, s.errors, s.latencies)
		}
	}
}

func TestSnapshot_takesSoleOwnershipOfMap(t *testing.T) {
	// The struct copy carries a map header, so a naive implementation would
	// leave collector and worker writing the same map. Later Records must not
	// mutate an already-returned snapshot.
	ws := newWorkerStats(testOps)
	ws.Record(OpInsert, 0.001, nil)

	snap := ws.snapshot()
	before := len(snap[OpInsert].latencies)
	if before != 1 {
		t.Fatalf("setup: want 1 latency in snapshot, got %d", before)
	}

	ws.Record(OpInsert, 0.002, nil)
	ws.Record(OpInsert, 0.003, nil)

	if after := len(snap[OpInsert].latencies); after != before {
		t.Errorf("snapshot mutated by later Records: had %d keys, now %d — worker and "+
			"collector are sharing a map", before, after)
	}
	if snap[OpInsert].latencies[2000] != 0 || snap[OpInsert].latencies[3000] != 0 {
		t.Errorf("post-snapshot observations leaked into the snapshot: %v", snap[OpInsert].latencies)
	}
}

func TestRecordAndSnapshot_concurrent_noLostObservations(t *testing.T) {
	// Run with -race. Verifies the mutex actually protects the map and that
	// snapshotting concurrently with writes never drops an observation.
	const writers, perWriter = 8, 500
	ws := newWorkerStats(testOps)

	var collected int64
	var collectedObs int64
	stop := make(chan struct{})
	var collectorDone sync.WaitGroup
	collectorDone.Add(1)
	go func() {
		defer collectorDone.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap := ws.snapshot()
			collected += snap[OpInsert].count
			for _, n := range snap[OpInsert].latencies {
				collectedObs += n
			}
		}
	}()

	var writersDone sync.WaitGroup
	for w := range writers {
		writersDone.Add(1)
		go func(w int) {
			defer writersDone.Done()
			for i := range perWriter {
				// Vary latency so the map holds many distinct keys.
				ws.Record(OpInsert, float64(w*perWriter+i)*1e-6, nil)
			}
		}(w)
	}
	writersDone.Wait()
	close(stop)
	collectorDone.Wait()

	// Drain whatever the collector did not pick up.
	final := ws.snapshot()
	collected += final[OpInsert].count
	for _, n := range final[OpInsert].latencies {
		collectedObs += n
	}

	want := int64(writers * perWriter)
	if collected != want {
		t.Errorf("count: want %d, got %d (lost or double-counted)", want, collected)
	}
	if collectedObs != want {
		t.Errorf("latency observations: want %d, got %d", want, collectedObs)
	}
}

// ── percentile ───────────────────────────────────────────────────────────────

func TestPercentile_empty(t *testing.T) {
	if p := percentile(&opStats{}, 0.50); p != 0 {
		t.Errorf("nil map: want 0, got %v", p)
	}
	if p := percentile(&opStats{latencies: map[int64]int64{}}, 0.50); p != 0 {
		t.Errorf("empty map: want 0, got %v", p)
	}
}

func TestPercentile_singleObservation(t *testing.T) {
	s := statsWith([2]float64{7.5, 1})
	for _, q := range []float64{0, 0.5, 0.95, 0.99, 1} {
		if p := percentile(s, q); !approxEq(p, 7.5) {
			t.Errorf("q=%v: want 7.5, got %v", q, p)
		}
	}
}

func TestPercentile_exactNearestRank(t *testing.T) {
	// 100 observations at 1..100 ms. Nearest-rank: the q-th percentile is the
	// ceil(q*N)-th smallest, so the answers are exact integers with no
	// interpolation error.
	s := &opStats{latencies: map[int64]int64{}}
	for ms := 1; ms <= 100; ms++ {
		s.latencies[int64(ms)*1000] = 1
		s.count++
	}
	tests := []struct{ q, want float64 }{
		{0.50, 50},
		{0.90, 90},
		{0.95, 95},
		{0.99, 99},
		{1.00, 100},
	}
	for _, tc := range tests {
		if p := percentile(s, tc.q); !approxEq(p, tc.want) {
			t.Errorf("q=%.2f: want %v ms, got %v", tc.q, tc.want, p)
		}
	}
}

func TestPercentile_rankBoundaryBetweenValues(t *testing.T) {
	// 50 obs at 1 ms, 50 at 5 ms. rank(0.50)=50 falls on the last 1 ms
	// observation; rank(0.51)=51 crosses into the 5 ms group.
	s := statsWith([2]float64{1, 50}, [2]float64{5, 50})
	if p := percentile(s, 0.50); !approxEq(p, 1) {
		t.Errorf("p50: want 1, got %v", p)
	}
	if p := percentile(s, 0.51); !approxEq(p, 5) {
		t.Errorf("p51: want 5, got %v", p)
	}
	if p := percentile(s, 0.99); !approxEq(p, 5) {
		t.Errorf("p99: want 5, got %v", p)
	}
}

func TestPercentile_noCeilingClamp(t *testing.T) {
	// Regression: the previous fixed-bucket implementation topped out at 30000 ms
	// and reported anything slower as exactly 30000. Real values must survive.
	s := statsWith([2]float64{1, 1}, [2]float64{45_000, 1}, [2]float64{60_000, 1})
	if p := percentile(s, 0.99); !approxEq(p, 60_000) {
		t.Errorf("p99 above the old ceiling: want 60000 ms, got %v "+
			"(30000 would mean the clamp is back)", p)
	}
	if p := percentile(s, 0.50); !approxEq(p, 45_000) {
		t.Errorf("p50: want 45000 ms, got %v", p)
	}

	// A window entirely above the old ceiling must not collapse to 30000 either.
	all := statsWith([2]float64{120_000, 100})
	if p := percentile(all, 0.99); !approxEq(p, 120_000) {
		t.Errorf("all-slow p99: want 120000 ms, got %v", p)
	}
}

func TestPercentile_subMillisecondResolution(t *testing.T) {
	// The old bucketing put everything ≤ 1 ms in one bucket and interpolated.
	// Sub-millisecond values are now distinguishable.
	s := statsWith([2]float64{0.100, 1}, [2]float64{0.250, 1}, [2]float64{0.900, 1})
	if p := percentile(s, 0.01); !approxEq(p, 0.100) {
		t.Errorf("min: want 0.1 ms, got %v", p)
	}
	if p := percentile(s, 0.50); !approxEq(p, 0.250) {
		t.Errorf("p50: want 0.25 ms, got %v", p)
	}
	if p := percentile(s, 0.99); !approxEq(p, 0.900) {
		t.Errorf("p99: want 0.9 ms, got %v", p)
	}
}

func TestPercentile_quantileOutOfRangeClamps(t *testing.T) {
	s := statsWith([2]float64{10, 1}, [2]float64{20, 1}, [2]float64{30, 1})
	if p := percentile(s, 0); !approxEq(p, 10) {
		t.Errorf("q=0 should give the minimum: want 10, got %v", p)
	}
	if p := percentile(s, -1); !approxEq(p, 10) {
		t.Errorf("q<0 should clamp to the minimum: want 10, got %v", p)
	}
	if p := percentile(s, 1); !approxEq(p, 30) {
		t.Errorf("q=1 should give the maximum: want 30, got %v", p)
	}
	if p := percentile(s, 2); !approxEq(p, 30) {
		t.Errorf("q>1 should clamp to the maximum: want 30, got %v", p)
	}
}

func TestPercentile_zeroLatencyObservations(t *testing.T) {
	s := statsWith([2]float64{0, 3}, [2]float64{10, 1})
	if p := percentile(s, 0.50); !approxEq(p, 0) {
		t.Errorf("p50 with mostly-zero latencies: want 0, got %v", p)
	}
	if p := percentile(s, 0.99); !approxEq(p, 10) {
		t.Errorf("p99: want 10, got %v", p)
	}
}

func TestPercentile_keysWithZeroCount(t *testing.T) {
	// Record never produces a zero-count key, but percentile walks keys[:len-1]
	// and must stay correct if one exists — and must not divide by, or index
	// past, an all-zero map.
	s := &opStats{latencies: map[int64]int64{1000: 0, 2000: 5, 3000: 0}}
	if p := percentile(s, 0.50); !approxEq(p, 2) {
		t.Errorf("p50 with zero-count neighbours: want 2 ms, got %v", p)
	}
	if p := percentile(s, 0.99); !approxEq(p, 2) {
		t.Errorf("p99: want 2 ms, got %v", p)
	}
	// Every key zero → no observations at all.
	empty := &opStats{latencies: map[int64]int64{1000: 0, 2000: 0}}
	if p := percentile(empty, 0.50); p != 0 {
		t.Errorf("all-zero counts: want 0, got %v", p)
	}
}

// ── commaf ────────────────────────────────────────────────────────────────────

func TestCommaf(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{999, "999"},
		{1000, "1,000"},
		{9999, "9,999"},
		{1000000, "1,000,000"},
		{1234567, "1,234,567"},
	}
	for _, tc := range tests {
		got := commaf(tc.n)
		if got != tc.want {
			t.Errorf("commaf(%d): want %q, got %q", tc.n, tc.want, got)
		}
	}
}

// ── StatsCollector ───────────────────────────────────────────────────────────

// captureStdout runs fn with os.Stdout redirected and returns what it wrote.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				sb.Write(buf[:n])
			}
			if err != nil {
				break
			}
		}
		done <- sb.String()
	}()

	fn()
	w.Close()
	os.Stdout = orig
	return <-done
}

// tableRow returns the summary-table row for op, split into trimmed cells.
func tableRow(t *testing.T, out, op string) []string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "│") || !strings.Contains(line, op) {
			continue
		}
		var cells []string
		for _, c := range strings.Split(line, "│") {
			if c = strings.TrimSpace(c); c != "" {
				cells = append(cells, c)
			}
		}
		if len(cells) > 0 && cells[0] == op {
			return cells
		}
	}
	t.Fatalf("no table row for op %q in output:\n%s", op, out)
	return nil
}

func TestStatsCollector_print_mergesWorkersIntoExactPercentiles(t *testing.T) {
	c := NewStatsCollector(testOps)
	ws1 := c.NewWorkerStats()
	ws2 := c.NewWorkerStats()

	// Merged: 50 observations at 10 ms and 50 at 20 ms, split across two workers.
	// rank(0.50)=50 → last 10 ms obs; rank(0.95)=95 and rank(0.99)=99 → 20 ms.
	for range 25 {
		ws1.Record(OpInsert, 0.010, nil)
		ws2.Record(OpInsert, 0.010, nil)
		ws1.Record(OpInsert, 0.020, nil)
		ws2.Record(OpInsert, 0.020, nil)
	}

	out := captureStdout(t, func() { c.print(time.Now(), 30*time.Second, nil) })
	cells := tableRow(t, out, OpInsert)
	// cells: op, count, ops/s, p50, p95, p99
	if len(cells) != 6 {
		t.Fatalf("want 6 cells, got %d: %q", len(cells), cells)
	}
	if cells[1] != "100" {
		t.Errorf("count: want 100, got %q", cells[1])
	}
	if cells[3] != "10.0" {
		t.Errorf("p50: want 10.0, got %q (merge across workers is wrong)", cells[3])
	}
	if cells[4] != "20.0" {
		t.Errorf("p95: want 20.0, got %q", cells[4])
	}
	if cells[5] != "20.0" {
		t.Errorf("p99: want 20.0, got %q", cells[5])
	}
}

func TestStatsCollector_print_reportsBeyondOldCeiling(t *testing.T) {
	// End-to-end proof that a slow window is no longer clamped to 30000 ms.
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()
	ws.Record(OpDelete, 90.0, nil) // 90 s

	out := captureStdout(t, func() { c.print(time.Now(), 30*time.Second, nil) })
	cells := tableRow(t, out, OpDelete)
	if cells[5] != "90000.0" {
		t.Errorf("p99: want 90000.0 ms, got %q (30000.0 would mean the clamp is back)", cells[5])
	}
}

func TestStatsCollector_print_includesPoolLineWhenPoolPresent(t *testing.T) {
	// pgxpool connects lazily, so a pool pointed at nothing still serves Stat().
	// That is enough to exercise the pool branch of print() without a database.
	pool, err := pgxpool.New(context.Background(),
		"postgres://nobody:nobody@127.0.0.1:1/nodb?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()
	ws.Record(OpInsert, 0.010, nil)

	out := captureStdout(t, func() { c.print(time.Now(), 30*time.Second, pool) })
	if !strings.Contains(out, "pool  acquired=") {
		t.Errorf("expected a pool line in the summary, got:\n%s", out)
	}
	if !strings.Contains(out, "workers=1") {
		t.Errorf("expected workers=1 in the pool line, got:\n%s", out)
	}
}

func TestStatsCollector_print_resetsAllWorkers(t *testing.T) {
	c := NewStatsCollector(testOps)
	ws1 := c.NewWorkerStats()
	ws2 := c.NewWorkerStats()

	ws1.Record(OpInsert, 0.001, nil)
	ws1.Record(OpInsert, 0.050, nil)
	ws2.Record(OpInsert, 0.010, nil)
	ws2.Record(OpReadSimple, 0.005, nil)

	captureStdout(t, func() { c.print(time.Now(), 30*time.Second, nil) })

	// After print, both workers must be fully reset.
	for _, ws := range []*WorkerStats{ws1, ws2} {
		snap := ws.snapshot()
		for _, op := range testOps {
			s := snap[op]
			if s.count != 0 || s.errors != 0 || len(s.latencies) != 0 {
				t.Errorf("op=%s: expected zero after print, got count=%d errors=%d latencies=%v",
					op, s.count, s.errors, s.latencies)
			}
		}
	}
}

func TestStatsCollector_print_emptyWindowShowsZeros(t *testing.T) {
	c := NewStatsCollector(testOps)
	c.NewWorkerStats()
	out := captureStdout(t, func() { c.print(time.Now(), 30*time.Second, nil) })
	cells := tableRow(t, out, OpInsert)
	if cells[1] != "0" {
		t.Errorf("count: want 0, got %q", cells[1])
	}
	for i, name := range map[int]string{3: "p50", 4: "p95", 5: "p99"} {
		if cells[i] != "0.0" {
			t.Errorf("%s on an empty window: want 0.0, got %q", name, cells[i])
		}
	}
}

func TestRunSummaryLoop_ticksAndExitsOnCancel(t *testing.T) {
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()
	ws.Record(OpInsert, 0.05, nil)
	ws.Record(OpReadSimple, 0.01, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	captureStdout(t, func() {
		go func() {
			defer close(done)
			c.RunSummaryLoop(ctx, time.Millisecond, nil)
		}()

		// Wait long enough for multiple ticks to fire before cancelling.
		time.Sleep(50 * time.Millisecond)
		cancel()

		select {
		case <-done:
			// clean exit
		case <-time.After(time.Second):
			t.Error("RunSummaryLoop did not exit within 1s after context cancel")
		}
	})

	// At least one tick fired, so stats must have been snapshotted (reset).
	snap := ws.snapshot()
	if snap[OpInsert].count != 0 || snap[OpReadSimple].count != 0 {
		t.Error("expected stats reset after summary loop ticked")
	}
}
