// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

package workload

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// testOps is the op set used across the stats tests (the oltp-jsonb ops).
var testOps = []string{OpInsert, OpReadSimple, OpReadJoin, OpUpdate, OpDelete, OpReadByIP}

// ── WorkerStats.Record ───────────────────────────────────────────────────────

func TestRecord_bucketPlacement(t *testing.T) {
	// bucketBounds (ms): [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000]
	tests := []struct {
		durSec float64
		bucket int // expected bucket index; -1 means inf
	}{
		{0.0005, 0}, // 0.5 ms → ≤ 1 ms → bucket 0
		{0.001, 0},  // 1.0 ms → ≤ 1 ms → bucket 0
		{0.003, 1},  // 3 ms    → ≤ 5 ms → bucket 1
		{0.050, 4},  // 50 ms   → ≤ 50 ms → bucket 4
		{0.100, 5},  // 100 ms  → ≤ 100 ms → bucket 5
		{5.0, 10},   // 5 s     → ≤ 5000 ms → bucket 10
		{10.0, 11},  // 10 s    → ≤ 10000 ms → bucket 11
		{30.0, 12},  // 30 s    → ≤ 30000 ms → bucket 12 (top)
		{45.0, -1},  // 45 s    → > 30 s → inf
		{60.0, -1},  // 60 s    → > 30 s → inf
	}
	for _, tc := range tests {
		ws := newWorkerStats(testOps)
		ws.Record(OpInsert, tc.durSec, nil)
		s := ws.data[OpInsert]
		if tc.bucket == -1 {
			if s.inf != 1 {
				t.Errorf("dur=%.4fs: want inf=1, got %d", tc.durSec, s.inf)
			}
		} else {
			if s.buckets[tc.bucket] != 1 {
				t.Errorf("dur=%.4fs: want bucket[%d]=1, got %d", tc.durSec, tc.bucket, s.buckets[tc.bucket])
			}
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

	// Second snapshot must return zeros — data was reset.
	snap2 := ws.snapshot()
	for _, op := range testOps {
		s := snap2[op]
		if s.count != 0 || s.errors != 0 || s.inf != 0 {
			t.Errorf("op=%s: expected zero after snapshot, got count=%d errors=%d inf=%d",
				op, s.count, s.errors, s.inf)
		}
	}
}

// ── percentile ───────────────────────────────────────────────────────────────

func TestPercentile_empty(t *testing.T) {
	if p := percentile(&opStats{}, 0.50); p != 0 {
		t.Errorf("empty histogram: want 0, got %v", p)
	}
}

func approxEq(a, b float64) bool { return a-b < 1e-9 && b-a < 1e-9 }

func TestPercentile_allInFirstBucket(t *testing.T) {
	s := &opStats{}
	s.buckets[0] = 100 // all in (0, 1] ms
	// First bucket spans (0, 1]; with 100 uniform obs, interpolation places the
	// median at 0.5 ms and p99 at 0.99 ms (not snapped to the 1 ms upper edge).
	if p := percentile(s, 0.50); !approxEq(p, 0.5) {
		t.Errorf("p50: want 0.5, got %v", p)
	}
	if p := percentile(s, 0.99); !approxEq(p, 0.99) {
		t.Errorf("p99: want 0.99, got %v", p)
	}
}

func TestPercentile_allInInf(t *testing.T) {
	s := &opStats{inf: 100}
	// All observations exceed the highest bound (30000 ms), so the
	// percentile function exhausts its buckets and returns 30000 (the ceiling).
	if p := percentile(s, 0.99); p != 30000 {
		t.Errorf("p99 all-inf: want 30000, got %v", p)
	}
}

func TestPercentile_knownDistribution(t *testing.T) {
	// 50 obs in (0, 1] ms (bucket 0), 50 obs in (1, 5] ms (bucket 1).
	// p50: target=50 lands exactly at the top of bucket 0 → 1 ms.
	// p99: target=99 is 49/50 through bucket 1 (1,5] → 1 + 4*0.98 = 4.92 ms.
	s := &opStats{}
	s.buckets[0] = 50
	s.buckets[1] = 50
	if p := percentile(s, 0.50); !approxEq(p, 1) {
		t.Errorf("p50: want 1, got %v", p)
	}
	if p := percentile(s, 0.99); !approxEq(p, 4.92) {
		t.Errorf("p99: want 4.92, got %v", p)
	}
}

func TestPercentile_p95(t *testing.T) {
	// 95 obs in ≤ 50 ms (bucket 4), 5 obs in ≤ 100 ms (bucket 5).
	// p95: target=95, cum after bucket 4 = 95 ≥ 95 → 50 ms
	s := &opStats{}
	s.buckets[4] = 95
	s.buckets[5] = 5
	if p := percentile(s, 0.95); p != 50 {
		t.Errorf("p95: want 50, got %v", p)
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

func TestStatsCollector_print_resetsAllWorkers(t *testing.T) {
	c := NewStatsCollector(testOps)
	ws1 := c.NewWorkerStats()
	ws2 := c.NewWorkerStats()

	ws1.Record(OpInsert, 0.001, nil)
	ws1.Record(OpInsert, 0.050, nil)
	ws2.Record(OpInsert, 0.010, nil)
	ws2.Record(OpReadSimple, 0.005, nil)

	c.print(time.Now(), 30*time.Second, nil)

	// After print, both workers must be fully reset.
	for _, ws := range []*WorkerStats{ws1, ws2} {
		snap := ws.snapshot()
		for _, op := range testOps {
			s := snap[op]
			if s.count != 0 || s.errors != 0 || s.inf != 0 {
				t.Errorf("op=%s: expected zero after print, got count=%d errors=%d inf=%d",
					op, s.count, s.errors, s.inf)
			}
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
		t.Fatal("RunSummaryLoop did not exit within 1s after context cancel")
	}

	// At least one tick fired, so stats must have been snapshotted (reset).
	snap := ws.snapshot()
	if snap[OpInsert].count != 0 || snap[OpReadSimple].count != 0 {
		t.Error("expected stats reset after summary loop ticked")
	}
}
