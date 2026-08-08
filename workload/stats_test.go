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
	// A failed op is counted but contributes no latency.
	//
	// This assertion used to be the reverse, justified as "an op that failed
	// slowly is still evidence about how the server behaved". That is true of a
	// single slow failure and false of the population: failures are dominated by
	// fast ones, so mixing them in drags the percentiles down and makes a
	// degrading server look faster. If you are here because you want error
	// timings back, add a separate population — do not merge them into this one.
	if got := s.latencies[1000]; got != 1 {
		t.Errorf("only the successful op should contribute latency, got latencies[1000]=%d", got)
	}
	if len(s.latencies) != 1 {
		t.Errorf("failed op leaked a latency key: %v", s.latencies)
	}
}

// TestPercentiles_doNotImproveAsTheServerFails is the regression guard for the
// defect this separation exists to fix. A closed-loop generator with no backoff
// retries instantly after a failure, so a database that rejects connections
// emits a flood of microsecond "latencies". Blended into one population they
// outnumber the real observations and pull every quantile toward zero — the
// reported p99 improves precisely as the server gets worse, which inverts a
// before/after tuning conclusion.
//
// Framed as the inversion rather than as "errors are excluded" so the failure
// message names the actual defect.
func TestPercentiles_doNotImproveAsTheServerFails(t *testing.T) {
	const slowMS = 500.0

	healthy := newWorkerStats(testOps)
	for i := 0; i < 10; i++ {
		healthy.Record(OpReadJoin, slowMS/1000, nil)
	}

	// Same real work, plus a database that is now failing fast.
	degraded := newWorkerStats(testOps)
	for i := 0; i < 10; i++ {
		degraded.Record(OpReadJoin, slowMS/1000, nil)
	}
	for i := 0; i < 1000; i++ {
		degraded.Record(OpReadJoin, 0.0001, fmt.Errorf("connection refused"))
	}

	h, d := healthy.data[OpReadJoin], degraded.data[OpReadJoin]

	// Against the old behaviour: total=1010, rank(0.99)=ceil(0.99*1010)=1000,
	// and the 100µs key alone holds 1000 observations — so p99 came back as
	// 0.1ms instead of 500ms, a 5000x understatement.
	hp99, dp99 := percentile(h, 0.99), percentile(d, 0.99)
	if dp99 < hp99 {
		t.Errorf("p99 improved as the server degraded: healthy=%.1fms degraded=%.1fms", hp99, dp99)
	}
	if !approxEq(dp99, slowMS) {
		t.Errorf("degraded p99: want %.1fms (the real work), got %.1fms", slowMS, dp99)
	}
	if got := percentile(d, 0.50); !approxEq(got, slowMS) {
		t.Errorf("degraded p50: want %.1fms, got %.1fms", slowMS, got)
	}

	if d.count != 1010 || d.errors != 1000 {
		t.Errorf("failures must still be counted: count=%d errors=%d, want 1010/1000", d.count, d.errors)
	}
	ls := d.latencyStats()
	if !approxEq(ls.MinMS, slowMS) || !approxEq(ls.MaxMS, slowMS) || !approxEq(ls.MeanMS, slowMS) {
		t.Errorf("every summary stat should reflect the successes only, got %+v", ls)
	}
}

// TestRecord_latencySamplesEqualSuccesses pins the invariant documented on
// opStats. It is what makes an absent latency block in the result unambiguous:
// absent iff count == errors.
func TestRecord_latencySamplesEqualSuccesses(t *testing.T) {
	tests := []struct {
		name          string
		oks, failures int
	}{
		{"all successful", 7, 0},
		{"mixed", 4, 3},
		{"all failed", 0, 5},
		{"nothing recorded", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := newWorkerStats(testOps)
			for i := 0; i < tt.oks; i++ {
				ws.Record(OpUpdate, 0.002, nil)
			}
			for i := 0; i < tt.failures; i++ {
				ws.Record(OpUpdate, 0.002, fmt.Errorf("boom"))
			}
			s := ws.data[OpUpdate]
			if got, want := s.samples(), s.count-s.errors; got != want {
				t.Errorf("sum(latencies)=%d, want count-errors=%d", got, want)
			}
			if tt.oks == 0 && s.latencies != nil {
				t.Errorf("no successful op should leave a nil map, got %v", s.latencies)
			}
		})
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
	// The errored delete is counted but must carry no latency across the
	// snapshot boundary either — the exclusion has to survive the handoff, not
	// just hold inside Record.
	if got := snap[OpDelete].samples(); got != 0 {
		t.Errorf("errored op carried %d latency samples through snapshot, want 0", got)
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

// splitCells splits one table line on the box-drawing separator into trimmed,
// non-empty cells.
func splitCells(line string) []string {
	var cells []string
	for _, c := range strings.Split(line, "│") {
		if c = strings.TrimSpace(c); c != "" {
			cells = append(cells, c)
		}
	}
	return cells
}

// tableRow returns a lookup into the summary-table row for op, keyed by the
// column heading print() emits.
//
// Deliberately header-driven rather than positional: the previous version
// returned a slice and callers indexed it, so adding one column broke three
// unrelated tests at once and told them nothing about what had changed. Look
// columns up by name and adding the next one costs nothing.
func tableRow(t *testing.T, out, op string) func(col string) string {
	t.Helper()

	var header []string
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "│") {
			continue
		}
		cells := splitCells(line)
		if len(cells) == 0 {
			continue
		}
		if header == nil && cells[0] == "op" {
			header = cells
			continue
		}
		if header == nil || cells[0] != op {
			continue
		}
		if len(cells) != len(header) {
			t.Fatalf("row for %q has %d cells but the header has %d:\n%s",
				op, len(cells), len(header), out)
		}
		row := make(map[string]string, len(header))
		for i, name := range header {
			row[name] = cells[i]
		}
		return func(col string) string {
			v, ok := row[col]
			if !ok {
				t.Fatalf("no column %q in summary table (have %v)", col, header)
			}
			return v
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
	cell := tableRow(t, out, OpInsert)
	if cell("count") != "100" {
		t.Errorf("count: want 100, got %q", cell("count"))
	}
	if cell("p50 ms") != "10.0" {
		t.Errorf("p50: want 10.0, got %q (merge across workers is wrong)", cell("p50 ms"))
	}
	if cell("p95 ms") != "20.0" {
		t.Errorf("p95: want 20.0, got %q", cell("p95 ms"))
	}
	if cell("p99 ms") != "20.0" {
		t.Errorf("p99: want 20.0, got %q", cell("p99 ms"))
	}
}

func TestStatsCollector_print_showsErrorsAndDashesFailedOnlyOps(t *testing.T) {
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()

	// read_join half-fails; delete fails outright.
	ws.Record(OpReadJoin, 0.010, nil)
	ws.Record(OpReadJoin, 0.0001, fmt.Errorf("boom"))
	for range 3 {
		ws.Record(OpDelete, 0.0001, fmt.Errorf("boom"))
	}

	out := captureStdout(t, func() { c.print(time.Now(), 30*time.Second, nil) })

	join := tableRow(t, out, OpReadJoin)
	if join("err") != "1" {
		t.Errorf("read_join err cell: want 1, got %q", join("err"))
	}
	if join("p99 ms") != "10.0" {
		t.Errorf("read_join p99 should reflect the success only: want 10.0, got %q", join("p99 ms"))
	}

	del := tableRow(t, out, OpDelete)
	if del("count") != "3" || del("err") != "3" {
		t.Errorf("delete cells: want count=3 err=3, got count=%q err=%q", del("count"), del("err"))
	}
	// 0.0 here would read as "instant" for an op that never once succeeded.
	if del("p99 ms") != "—" {
		t.Errorf("wholly-failed op p99: want the em-dash, got %q", del("p99 ms"))
	}
}

func TestStatsCollector_print_reportsBeyondOldCeiling(t *testing.T) {
	// End-to-end proof that a slow window is no longer clamped to 30000 ms.
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()
	ws.Record(OpDelete, 90.0, nil) // 90 s

	out := captureStdout(t, func() { c.print(time.Now(), 30*time.Second, nil) })
	cell := tableRow(t, out, OpDelete)
	if cell("p99 ms") != "90000.0" {
		t.Errorf("p99: want 90000.0 ms, got %q (30000.0 would mean the clamp is back)", cell("p99 ms"))
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

func TestStatsCollector_print_emptyWindowShowsNoLatency(t *testing.T) {
	c := NewStatsCollector(testOps)
	c.NewWorkerStats()
	out := captureStdout(t, func() { c.print(time.Now(), 30*time.Second, nil) })
	cell := tableRow(t, out, OpInsert)
	if cell("count") != "0" {
		t.Errorf("count: want 0, got %q", cell("count"))
	}
	if cell("err") != "0" {
		t.Errorf("err: want 0, got %q", cell("err"))
	}
	// An idle window has nothing to summarise, same as a wholly-failed one; the
	// count and err cells are what tell the two apart.
	for _, col := range []string{"p50 ms", "p95 ms", "p99 ms"} {
		if cell(col) != "—" {
			t.Errorf("%s on an empty window: want the em-dash, got %q", col, cell(col))
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
