// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Haitham Gadelrab
// Licensed under the Apache License, Version 2.0; see the LICENSE file.

package workload

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/haithamoon/pgstorm/config"
)

func testCfg() *config.Config {
	return &config.Config{
		Profile:         "oltp-jsonb",
		Workers:         4,
		MinPayloadKB:    8,
		MaxPayloadKB:    16,
		ToastPct:        20,
		ReadPayload:     true,
		CreateIndexes:   true,
		RingSize:        10000,
		DeleteBatchSize: 50,
		UserPoolSize:    10000,
		ActorPoolSize:   100,
		ThinkTimeMs:     3,
		RunDurationSecs: 900,
	}
}

var testWeights = []WeightedOp{
	{Name: OpInsert, Weight: 35},
	{Name: OpReadSimple, Weight: 15},
}

// ── latencyStats ─────────────────────────────────────────────────────────────

func TestLatencyStats_exactValues(t *testing.T) {
	// 1..100 ms, one observation each. Every field is checkable by hand.
	s := &opStats{latencies: map[int64]int64{}}
	for ms := 1; ms <= 100; ms++ {
		s.latencies[int64(ms)*1000] = 1
		s.count++
	}
	ls := s.latencyStats()
	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"min", ls.MinMS, 1},
		{"mean", ls.MeanMS, 50.5}, // (1+...+100)/100
		{"p50", ls.P50MS, 50},
		{"p95", ls.P95MS, 95},
		{"p99", ls.P99MS, 99},
		{"p999", ls.P999MS, 100}, // ceil(0.999*100)=100 → the max
		{"max", ls.MaxMS, 100},
	}
	for _, c := range checks {
		if !approxEq(c.got, c.want) {
			t.Errorf("%s: want %v, got %v", c.name, c.want, c.got)
		}
	}
}

func TestLatencyStats_empty(t *testing.T) {
	if ls := (&opStats{}).latencyStats(); ls != (LatencyStats{}) {
		t.Errorf("empty opStats should give a zero LatencyStats, got %+v", ls)
	}
	// A map whose counts are all zero has no observations either.
	z := &opStats{latencies: map[int64]int64{5000: 0}}
	if ls := z.latencyStats(); ls != (LatencyStats{}) {
		t.Errorf("all-zero counts should give a zero LatencyStats, got %+v", ls)
	}
}

func TestLatencyStats_weightedMean(t *testing.T) {
	// 3 obs at 10 ms, 1 at 50 ms → mean = (3*10 + 50)/4 = 20 ms.
	s := statsWith([2]float64{10, 3}, [2]float64{50, 1})
	ls := s.latencyStats()
	if !approxEq(ls.MeanMS, 20) {
		t.Errorf("mean: want 20, got %v", ls.MeanMS)
	}
	if !approxEq(ls.MinMS, 10) || !approxEq(ls.MaxMS, 50) {
		t.Errorf("min/max: want 10/50, got %v/%v", ls.MinMS, ls.MaxMS)
	}
}

func TestLatencyStats_noCeiling(t *testing.T) {
	s := statsWith([2]float64{120_000, 2})
	ls := s.latencyStats()
	if !approxEq(ls.MaxMS, 120_000) || !approxEq(ls.P99MS, 120_000) {
		t.Errorf("values above the old 30s bucket ceiling must survive, got max=%v p99=%v",
			ls.MaxMS, ls.P99MS)
	}
}

// ── StatsCollector totals ────────────────────────────────────────────────────

func TestFinalize_accumulatesAcrossWindows(t *testing.T) {
	// The per-window snapshot is destructive, so the run total must be built up
	// as windows are printed rather than read from the workers at the end.
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()

	ws.Record(OpInsert, 0.010, nil)
	ws.Record(OpInsert, 0.020, nil)
	captureStdout(t, func() { c.print(time.Now(), time.Second, nil) }) // window 1 consumed

	ws.Record(OpInsert, 0.030, nil)
	captureStdout(t, func() { c.print(time.Now(), time.Second, nil) }) // window 2 consumed

	ws.Record(OpInsert, 0.040, nil) // trailing, never printed

	totals, _, _ := c.Finalize()
	got := totals[OpInsert]
	if got.count != 4 {
		t.Errorf("count: want 4 across two windows plus the trailing one, got %d", got.count)
	}
	ls := got.latencyStats()
	if !approxEq(ls.MinMS, 10) || !approxEq(ls.MaxMS, 40) {
		t.Errorf("want min 10 / max 40 ms, got %v/%v", ls.MinMS, ls.MaxMS)
	}
}

func TestFinalize_capturesTrailingWindowWithNoPrint(t *testing.T) {
	// A run shorter than one summary interval prints nothing at all; its data
	// must still reach the result.
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()
	ws.Record(OpDelete, 0.005, nil)
	ws.Record(OpDelete, 0.015, errors.New("boom"))

	totals, _, _ := c.Finalize()
	got := totals[OpDelete]
	if got.count != 2 {
		t.Errorf("count: want 2, got %d", got.count)
	}
	if got.errors != 1 {
		t.Errorf("errors: want 1, got %d", got.errors)
	}
}

func TestFinalize_isIdempotent(t *testing.T) {
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()
	ws.Record(OpInsert, 0.010, nil)

	first, _, _ := c.Finalize()
	second, _, _ := c.Finalize()
	if first[OpInsert].count != 1 {
		t.Fatalf("first Finalize: want 1, got %d", first[OpInsert].count)
	}
	if second[OpInsert].count != 1 {
		t.Errorf("second Finalize double-counted: want 1, got %d", second[OpInsert].count)
	}
}

func TestFinalize_returnsCopyNotLiveTotals(t *testing.T) {
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()
	ws.Record(OpInsert, 0.010, nil)

	totals, _, _ := c.Finalize()
	before := totals[OpInsert].count

	ws.Record(OpInsert, 0.020, nil)
	c.Finalize()

	if totals[OpInsert].count != before {
		t.Errorf("previously returned totals mutated: had %d, now %d",
			before, totals[OpInsert].count)
	}
}

func TestFinalize_timesSpanTheRun(t *testing.T) {
	c := NewStatsCollector(testOps)
	_, started, ended := c.Finalize()
	if ended.Before(started) {
		t.Errorf("ended (%v) before started (%v)", ended, started)
	}
	if d := ended.Sub(started); d < 0 || d > time.Minute {
		t.Errorf("implausible run duration: %v", d)
	}
}

func TestFinalize_concurrentWithPrint_noLostOrDoubleCountedOps(t *testing.T) {
	// Real shutdown ordering: the summary goroutine can still be inside print()
	// when main calls Finalize(). Both consume worker snapshots, so every
	// observation must land in the run total exactly once regardless of who
	// gets there first. Run with -race.
	const writers, perWriter = 4, 400
	c := NewStatsCollector(testOps)

	workers := make([]*WorkerStats, writers)
	for i := range workers {
		workers[i] = c.NewWorkerStats()
	}

	stopPrinting := make(chan struct{})
	var printer sync.WaitGroup
	printer.Add(1)
	go func() {
		defer printer.Done()
		captureStdout(t, func() {
			for {
				select {
				case <-stopPrinting:
					return
				default:
				}
				c.print(time.Now(), time.Second, nil)
			}
		})
	}()

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range perWriter {
				workers[w].Record(OpInsert, float64(i)*1e-6, nil)
			}
		}(w)
	}
	wg.Wait()
	close(stopPrinting)
	printer.Wait()

	totals, _, _ := c.Finalize()
	want := int64(writers * perWriter)
	if got := totals[OpInsert].count; got != want {
		t.Errorf("run total: want %d, got %d (observations lost or double-counted "+
			"between print and Finalize)", want, got)
	}
	var observed int64
	for _, n := range totals[OpInsert].latencies {
		observed += n
	}
	if observed != want {
		t.Errorf("latency observations: want %d, got %d", want, observed)
	}
}

func TestFinalize_racesPrintWithoutLosingOps(t *testing.T) {
	// The real shutdown has no ordering between the summary goroutine's last
	// print and main's Finalize: runCtx expiring wakes both. If drain() is not
	// atomic, print can lift a window out of the workers while Finalize copies
	// the total before that window is folded in — silently dropping a whole
	// summary interval from the result. Observed live as a result of 12,315 ops
	// when the console had printed 17,880.
	//
	// Many short iterations, each racing one print against one Finalize.
	// Many workers, each holding many distinct latencies, so the snapshot-and-
	// merge phase of print() takes long enough for Finalize to land inside it.
	const workers, perWorker = 300, 40
	const n = workers * perWorker

	for iter := range 40 {
		c := NewStatsCollector(testOps)
		for w := range workers {
			ws := c.NewWorkerStats()
			for i := range perWorker {
				ws.Record(OpInsert, float64(w*perWorker+i)*1e-6, nil)
			}
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			captureStdout(t, func() { c.print(time.Now(), time.Second, nil) })
		}()

		totals, _, _ := c.Finalize()
		wg.Wait()

		// Whichever order they run in, the totals Finalize *returns* — the ones
		// that get written to the result file — must already hold every
		// observation. Checking after the fact would hide the bug, because the
		// lost window does land in the total eventually, just too late to be
		// written out.
		if got := totals[OpInsert].count; got != n {
			t.Fatalf("iteration %d: Finalize returned %d ops, want %d — a window was "+
				"lifted out of the workers by print but not folded into the total "+
				"before Finalize copied it", iter, got, n)
		}
	}
}

// ── BuildRunResult ───────────────────────────────────────────────────────────

func TestBuildRunResult_totalsAndRates(t *testing.T) {
	started := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	ended := started.Add(10 * time.Second)

	totals := map[string]opStats{
		OpInsert:     *statsWith([2]float64{10, 90}, [2]float64{20, 10}),
		OpReadSimple: *statsWith([2]float64{5, 50}),
	}
	totals[OpInsert] = opStats{count: 100, errors: 3, latencies: totals[OpInsert].latencies}
	totals[OpReadSimple] = opStats{count: 50, errors: 0, latencies: totals[OpReadSimple].latencies}

	r := BuildRunResult(totals, started, ended, testCfg(), testWeights)

	if r.DurationSeconds != 10 {
		t.Errorf("duration: want 10, got %v", r.DurationSeconds)
	}
	if r.Totals.Ops != 150 {
		t.Errorf("total ops: want 150, got %d", r.Totals.Ops)
	}
	if r.Totals.Errors != 3 {
		t.Errorf("total errors: want 3, got %d", r.Totals.Errors)
	}
	if !approxEq(r.Totals.OpsPerSec, 15) {
		t.Errorf("total ops/sec: want 15, got %v", r.Totals.OpsPerSec)
	}
	if len(r.Operations) != 2 {
		t.Fatalf("want 2 operations, got %d", len(r.Operations))
	}
	// Sorted by name: delete < insert alphabetically is irrelevant here; the two
	// ops present are insert and read_simple.
	if r.Operations[0].Op != OpInsert {
		t.Errorf("operations should be name-sorted for stable diffs, got %q first",
			r.Operations[0].Op)
	}
	if !approxEq(r.Operations[0].OpsPerSec, 10) {
		t.Errorf("insert ops/sec: want 10, got %v", r.Operations[0].OpsPerSec)
	}
	if !approxEq(r.Operations[0].Latency.P99MS, 20) {
		t.Errorf("insert p99: want 20, got %v", r.Operations[0].Latency.P99MS)
	}
}

func TestBuildRunResult_zeroDurationDoesNotProduceInfinity(t *testing.T) {
	now := time.Now()
	totals := map[string]opStats{OpInsert: *statsWith([2]float64{10, 5})}
	r := BuildRunResult(totals, now, now, testCfg(), testWeights)

	if r.Totals.OpsPerSec != 0 {
		t.Errorf("zero duration should give 0 ops/sec, got %v", r.Totals.OpsPerSec)
	}
	// Infinity does not survive encoding/json — guard the whole document.
	if _, err := json.Marshal(r); err != nil {
		t.Errorf("result is not JSON-encodable: %v", err)
	}
}

func TestBuildRunResult_capturesConfigAndWeights(t *testing.T) {
	cfg := testCfg()
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second), cfg, testWeights)

	if r.Config.Workers != cfg.Workers || r.Config.ToastPct != cfg.ToastPct {
		t.Errorf("config not carried through: %+v", r.Config)
	}
	if !r.Config.ReadPayload || !r.Config.CreateIndexes {
		t.Errorf("bool config knobs lost: %+v", r.Config)
	}
	if r.Config.OpWeights[OpInsert] != 35 || r.Config.OpWeights[OpReadSimple] != 15 {
		t.Errorf("op weights not recorded: %v", r.Config.OpWeights)
	}
	if r.Profile != "oltp-jsonb" {
		t.Errorf("profile: want oltp-jsonb, got %q", r.Profile)
	}
}

func TestBuildRunResult_timesAreUTC(t *testing.T) {
	loc := time.FixedZone("UTC+7", 7*3600)
	started := time.Date(2026, 8, 8, 12, 0, 0, 0, loc)
	r := BuildRunResult(map[string]opStats{}, started, started.Add(time.Second), testCfg(), testWeights)
	if r.StartedAt.Location() != time.UTC || r.EndedAt.Location() != time.UTC {
		t.Errorf("timestamps should be normalised to UTC so results compare across machines")
	}
}

func TestBuildRunResult_emptyRun(t *testing.T) {
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second), testCfg(), nil)
	if r.Totals.Ops != 0 || len(r.Operations) != 0 {
		t.Errorf("empty run should produce zero totals and no operations, got %+v", r.Totals)
	}
	if _, err := json.Marshal(r); err != nil {
		t.Errorf("empty result not encodable: %v", err)
	}
}

// ── WriteRunResult ───────────────────────────────────────────────────────────

func TestWriteRunResult_roundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "result.json")

	started := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	totals := map[string]opStats{OpInsert: *statsWith([2]float64{10, 4})}
	want := BuildRunResult(totals, started, started.Add(4*time.Second), testCfg(), testWeights)

	if err := WriteRunResult(path, want); err != nil {
		t.Fatalf("WriteRunResult: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !strings.HasSuffix(string(raw), "\n") {
		t.Error("file should end with a newline")
	}
	if !strings.Contains(string(raw), "\n  ") {
		t.Error("expected indented JSON for human readability")
	}

	var got RunResult
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Totals.Ops != want.Totals.Ops {
		t.Errorf("ops: want %d, got %d", want.Totals.Ops, got.Totals.Ops)
	}
	if !got.StartedAt.Equal(want.StartedAt) {
		t.Errorf("started_at: want %v, got %v", want.StartedAt, got.StartedAt)
	}
	if len(got.Operations) != 1 || got.Operations[0].Op != OpInsert {
		t.Fatalf("operations did not round-trip: %+v", got.Operations)
	}
	if !approxEq(got.Operations[0].Latency.P50MS, 10) {
		t.Errorf("latency did not round-trip: %+v", got.Operations[0].Latency)
	}
}

func TestWriteRunResult_createsParentDirectories(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "deeper", "result.json")
	if err := WriteRunResult(path, RunResult{}); err != nil {
		t.Fatalf("WriteRunResult should create missing parents: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("result not written: %v", err)
	}
}

func TestWriteRunResult_overwritesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "result.json")

	first := BuildRunResult(map[string]opStats{OpInsert: {count: 1}},
		time.Now(), time.Now().Add(time.Second), testCfg(), testWeights)
	if err := WriteRunResult(path, first); err != nil {
		t.Fatalf("first write: %v", err)
	}
	second := BuildRunResult(map[string]opStats{OpInsert: {count: 999}},
		time.Now(), time.Now().Add(time.Second), testCfg(), testWeights)
	if err := WriteRunResult(path, second); err != nil {
		t.Fatalf("second write: %v", err)
	}

	var got RunResult
	raw, _ := os.ReadFile(path)
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Totals.Ops != 999 {
		t.Errorf("second write did not replace the first: got %d ops", got.Totals.Ops)
	}

	// The temp file must not be left behind next to the result.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("expected only result.json in the directory, found %v", names)
	}
}

func TestWriteRunResult_errorsOnUnmarshalableValue(t *testing.T) {
	// encoding/json rejects Inf and NaN. BuildRunResult guards against producing
	// them, but the writer must surface the failure rather than write a partial
	// file or panic.
	r := RunResult{Totals: RunTotals{OpsPerSec: math.Inf(1)}}
	err := WriteRunResult(filepath.Join(t.TempDir(), "result.json"), r)
	if err == nil {
		t.Fatal("expected an error for a non-encodable value")
	}
	if !strings.Contains(err.Error(), "marshal result") {
		t.Errorf("error should name the failing step, got: %v", err)
	}
}

func TestWriteRunResult_errorsWhenDirectoryNotWritable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits are not enforced")
	}
	dir := filepath.Join(t.TempDir(), "readonly")
	if err := os.Mkdir(dir, 0o500); err != nil { // r-x: cannot create files inside
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) }) // so TempDir cleanup can remove it

	err := WriteRunResult(filepath.Join(dir, "result.json"), RunResult{})
	if err == nil {
		t.Fatal("expected an error writing into a read-only directory")
	}
	if !strings.Contains(err.Error(), "temp result file") {
		t.Errorf("error should name the failing step, got: %v", err)
	}
}

func TestWriteRunResult_errorsWhenTargetIsADirectory(t *testing.T) {
	// rename() cannot replace a directory with a file.
	path := filepath.Join(t.TempDir(), "result.json")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("setup: %v", err)
	}
	err := WriteRunResult(path, RunResult{})
	if err == nil {
		t.Fatal("expected an error when the destination is a directory")
	}
	if !strings.Contains(err.Error(), "rename result") {
		t.Errorf("error should name the failing step, got: %v", err)
	}
	// The temp file must still be cleaned up on the failure path.
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Errorf("temp file leaked after a failed rename: %d entries", len(entries))
	}
}

func TestWriteRunResult_errorsOnUnwritableLocation(t *testing.T) {
	// A path whose parent is an existing *file* cannot be created as a directory.
	dir := t.TempDir()
	blocker := filepath.Join(dir, "notadir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	err := WriteRunResult(filepath.Join(blocker, "result.json"), RunResult{})
	if err == nil {
		t.Fatal("expected an error when the parent path is a file")
	}
	if !strings.Contains(err.Error(), "result directory") {
		t.Errorf("error should name the failing step, got: %v", err)
	}
}
