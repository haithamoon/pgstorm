// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Haitham Gadelrab
// Licensed under the Apache License, Version 2.0; see the LICENSE file.

package workload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/haithamoon/pgstorm/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

	totals, _, _, _ := c.Finalize()
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

	totals, _, _, _ := c.Finalize()
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

	first, _, _, _ := c.Finalize()
	second, _, _, _ := c.Finalize()
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

	totals, _, _, _ := c.Finalize()
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
	_, _, started, ended := c.Finalize()
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

	totals, _, _, _ := c.Finalize()
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

		totals, _, _, _ := c.Finalize()
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

	r := BuildRunResult(totals, started, ended, testCfg(), testWeights, RunMeta{})

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
	r := BuildRunResult(totals, now, now, testCfg(), testWeights, RunMeta{})

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
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second), cfg, testWeights, RunMeta{})

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
	r := BuildRunResult(map[string]opStats{}, started, started.Add(time.Second), testCfg(), testWeights, RunMeta{})
	if r.StartedAt.Location() != time.UTC || r.EndedAt.Location() != time.UTC {
		t.Errorf("timestamps should be normalised to UTC so results compare across machines")
	}
}

func TestBuildRunResult_emptyRun(t *testing.T) {
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second), testCfg(), nil, RunMeta{})
	if r.Totals.Ops != 0 || len(r.Operations) != 0 {
		t.Errorf("empty run should produce zero totals and no operations, got %+v", r.Totals)
	}
	if _, err := json.Marshal(r); err != nil {
		t.Errorf("empty result not encodable: %v", err)
	}
}

// ── SnapshotDataset ──────────────────────────────────────────────────────────

// scriptedRows returns pre-set values from Scan. The shared mockRows in
// ops_test.go treats Scan as a no-op, which cannot exercise a query that reads
// values back.
type scriptedRows struct {
	rows    [][]any
	idx     int
	scanErr error
	err     error
	closed  bool
}

func (r *scriptedRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}

func (r *scriptedRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.idx-1]
	for i, d := range dest {
		switch v := d.(type) {
		case *string:
			*v = row[i].(string)
		case *int64:
			*v = row[i].(int64)
		default:
			return fmt.Errorf("scriptedRows: unsupported dest type %T", d)
		}
	}
	return nil
}

func (r *scriptedRows) Close()                                       { r.closed = true }
func (r *scriptedRows) Err() error                                   { return r.err }
func (r *scriptedRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *scriptedRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *scriptedRows) Values() ([]any, error)                       { return nil, nil }
func (r *scriptedRows) RawValues() [][]byte                          { return nil }
func (r *scriptedRows) Conn() *pgx.Conn                              { return nil }

// scriptedPool serves one scriptedRows (or an error) from Query.
type scriptedPool struct {
	rows     *scriptedRows
	queryErr error
	lastSQL  string
	lastArgs []any
}

func (p *scriptedPool) Begin(ctx context.Context) (pgx.Tx, error) {
	panic("unexpected Begin")
}
func (p *scriptedPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.lastSQL, p.lastArgs = sql, args
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	return p.rows, nil
}
func (p *scriptedPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	panic("unexpected Exec")
}

func TestSnapshotDataset_sumsTablesAndTuples(t *testing.T) {
	pool := &scriptedPool{rows: &scriptedRows{rows: [][]any{
		{"sessions", int64(1000), int64(10)},
		{"events", int64(2500), int64(40)},
		{"audit_log", int64(500), int64(5)},
	}}}

	got, err := SnapshotDataset(context.Background(), pool, []string{"sessions", "events", "audit_log"})
	if err != nil {
		t.Fatalf("SnapshotDataset: %v", err)
	}
	if got.TotalBytes != 4000 {
		t.Errorf("total bytes: want 4000, got %d", got.TotalBytes)
	}
	if got.LiveTuples != 55 {
		t.Errorf("live tuples: want 55, got %d", got.LiveTuples)
	}
	if got.TableBytes["events"] != 2500 {
		t.Errorf("per-table bytes: want events=2500, got %v", got.TableBytes)
	}
	if !pool.rows.closed {
		t.Error("rows should be closed")
	}
	// Size must include out-of-line storage: heap-only would understate this
	// workload several-fold, since TOAST is the point of it.
	if !strings.Contains(pool.lastSQL, "pg_total_relation_size") {
		t.Errorf("expected pg_total_relation_size, got query: %s", pool.lastSQL)
	}
}

func TestSnapshotDataset_emptyDatabase(t *testing.T) {
	pool := &scriptedPool{rows: &scriptedRows{}}
	got, err := SnapshotDataset(context.Background(), pool, []string{"sessions"})
	if err != nil {
		t.Fatalf("SnapshotDataset: %v", err)
	}
	if got.TotalBytes != 0 || got.LiveTuples != 0 {
		t.Errorf("want zeroes for an empty result set, got %+v", got)
	}
	if got.TableBytes == nil {
		t.Error("TableBytes should be an empty map, not nil, so it encodes as {}")
	}
}

func TestSnapshotDataset_errorPaths(t *testing.T) {
	tests := []struct {
		name string
		pool *scriptedPool
		want string
	}{
		{"query fails", &scriptedPool{queryErr: errors.New("boom")}, "query dataset size"},
		{"scan fails", &scriptedPool{rows: &scriptedRows{
			rows: [][]any{{"sessions", int64(1), int64(1)}}, scanErr: errors.New("bad"),
		}}, "scan dataset size"},
		{"iteration fails", &scriptedPool{rows: &scriptedRows{err: errors.New("torn")}}, "read dataset size"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := SnapshotDataset(context.Background(), tc.pool, []string{"sessions"})
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should name the failing step %q, got: %v", tc.want, err)
			}
		})
	}
}

// TestBuildRunResult_latencyPresentIffAnOpSucceeded pins the equivalence stated
// on OpResult: the latency block is present iff count > errors. An op that
// failed every time has nothing to summarise, and emitting an all-zero block
// would read as "instant" — the same misreading this whole change removes.
func TestBuildRunResult_latencyPresentIffAnOpSucceeded(t *testing.T) {
	// opJSON returns the marshalled object for one op, so key presence can be
	// checked per op. strings.Contains would be useless here: any other op in
	// the document supplies the "latency" key.
	opJSON := func(t *testing.T, r RunResult, op string) map[string]any {
		t.Helper()
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var doc struct {
			Operations []map[string]any `json:"operations"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		for _, o := range doc.Operations {
			if o["op"] == op {
				return o
			}
		}
		t.Fatalf("op %q not in document: %s", op, raw)
		return nil
	}

	t.Run("every attempt failed", func(t *testing.T) {
		totals := map[string]opStats{OpDelete: {count: 5, errors: 5}}
		r := BuildRunResult(totals, time.Now(), time.Now().Add(time.Second),
			testCfg(), testWeights, RunMeta{})
		if r.Operations[0].Latency != nil {
			t.Errorf("latency should be nil, got %+v", r.Operations[0].Latency)
		}
		if _, ok := opJSON(t, r, OpDelete)["latency"]; ok {
			t.Error("latency key should be omitted entirely, not zeroed")
		}
		// The failure itself is still fully reported.
		if r.Operations[0].Count != 5 || r.Operations[0].Errors != 5 {
			t.Errorf("count/errors must survive: %+v", r.Operations[0])
		}
	})

	t.Run("some attempts succeeded", func(t *testing.T) {
		s := statsWith([2]float64{10, 3})
		s.count, s.errors = 5, 2
		totals := map[string]opStats{OpInsert: *s}
		r := BuildRunResult(totals, time.Now(), time.Now().Add(time.Second),
			testCfg(), testWeights, RunMeta{})
		if r.Operations[0].Latency == nil {
			t.Fatal("latency should be present when an op succeeded")
		}
		if !approxEq(r.Operations[0].Latency.P99MS, 10) {
			t.Errorf("p99: want 10, got %v", r.Operations[0].Latency.P99MS)
		}
	})

	t.Run("one success measured at zero microseconds", func(t *testing.T) {
		// The trap case. Record truncates sub-microsecond durations to 0 us, so a
		// real observation yields an all-zero LatencyStats. An implementation that
		// tested `ls == LatencyStats{}` instead of the sample count would wrongly
		// drop this op's latency.
		totals := map[string]opStats{OpReadSimple: {count: 1, latencies: map[int64]int64{0: 1}}}
		r := BuildRunResult(totals, time.Now(), time.Now().Add(time.Second),
			testCfg(), testWeights, RunMeta{})
		if r.Operations[0].Latency == nil {
			t.Fatal("a genuine 0 us observation must still be reported")
		}
		if *r.Operations[0].Latency != (LatencyStats{}) {
			t.Errorf("want an all-zero block, got %+v", r.Operations[0].Latency)
		}
		if _, ok := opJSON(t, r, OpReadSimple)["latency"]; !ok {
			t.Error("latency key should be present")
		}
	})
}

func TestBuildRunResult_datasetOmittedWhenUnavailable(t *testing.T) {
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second),
		testCfg(), testWeights, RunMeta{})
	if r.Dataset != nil {
		t.Error("dataset should be nil when it could not be measured")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Absent, not zeroed — a missing measurement must not read as an empty database.
	if strings.Contains(string(raw), "dataset") {
		t.Errorf("dataset key should be omitted entirely, got: %s", raw)
	}
}

func TestBuildRunResult_datasetIncludedWhenPresent(t *testing.T) {
	ds := &DatasetSnapshot{
		Start:       DatasetSize{TotalBytes: 1000, LiveTuples: 10, TableBytes: map[string]int64{"events": 1000}},
		End:         DatasetSize{TotalBytes: 4000, LiveTuples: 44, TableBytes: map[string]int64{"events": 4000}},
		GrowthBytes: 3000,
	}
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second),
		testCfg(), testWeights, RunMeta{Dataset: ds})

	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got RunResult
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Dataset == nil {
		t.Fatal("dataset lost in round-trip")
	}
	if got.Dataset.GrowthBytes != 3000 {
		t.Errorf("growth: want 3000, got %d", got.Dataset.GrowthBytes)
	}
	if got.Dataset.Start.TotalBytes != 1000 || got.Dataset.End.TotalBytes != 4000 {
		t.Errorf("start/end sizes lost: %+v", got.Dataset)
	}
	if got.Dataset.End.TableBytes["events"] != 4000 {
		t.Errorf("per-table breakdown lost: %+v", got.Dataset.End.TableBytes)
	}
}

// ── SnapshotServerInfo ───────────────────────────────────────────────────────

func TestSnapshotServerInfo_versionAndSettings(t *testing.T) {
	pool := &scriptedPool{rows: &scriptedRows{rows: [][]any{
		{"server_version", "16.4", ""},
		{"shared_buffers", "16384", "8kB"},
		{"synchronous_commit", "on", ""},
		{"default_toast_compression", "pglz", ""},
		{"autovacuum_naptime", "60", "s"},
	}}}

	got, err := SnapshotServerInfo(context.Background(), pool)
	if err != nil {
		t.Fatalf("SnapshotServerInfo: %v", err)
	}
	if got.Version != "16.4" {
		t.Errorf("version: want 16.4, got %q", got.Version)
	}
	// server_version is surfaced as its own field, not duplicated in the map.
	if _, dup := got.Settings["server_version"]; dup {
		t.Error("server_version should not also appear in Settings")
	}
	// Units are appended when Postgres reports one...
	if got.Settings["shared_buffers"] != "16384 8kB" {
		t.Errorf("shared_buffers: want %q, got %q", "16384 8kB", got.Settings["shared_buffers"])
	}
	if got.Settings["autovacuum_naptime"] != "60 s" {
		t.Errorf("autovacuum_naptime: want %q, got %q", "60 s", got.Settings["autovacuum_naptime"])
	}
	// ...and never leave a trailing space when it is null.
	if got.Settings["synchronous_commit"] != "on" {
		t.Errorf("synchronous_commit: want %q, got %q", "on", got.Settings["synchronous_commit"])
	}
	if !pool.rows.closed {
		t.Error("rows should be closed")
	}
}

func TestSnapshotServerInfo_queriesTheCuratedAllowList(t *testing.T) {
	pool := &scriptedPool{rows: &scriptedRows{}}
	if _, err := SnapshotServerInfo(context.Background(), pool); err != nil {
		t.Fatalf("SnapshotServerInfo: %v", err)
	}
	if !strings.Contains(pool.lastSQL, "pg_settings") {
		t.Errorf("expected a pg_settings query, got: %s", pool.lastSQL)
	}
	// COALESCE matters: unit is NULL for every non-numeric setting, and without
	// it the scan fails on most of the list.
	if !strings.Contains(pool.lastSQL, "COALESCE(unit") {
		t.Errorf("unit must be coalesced or non-numeric settings fail to scan: %s", pool.lastSQL)
	}
	if len(pool.lastArgs) != 1 {
		t.Fatalf("want the allow-list passed as one arg, got %d", len(pool.lastArgs))
	}
	names, ok := pool.lastArgs[0].([]string)
	if !ok {
		t.Fatalf("want []string arg, got %T", pool.lastArgs[0])
	}
	for _, required := range []string{
		"server_version", "shared_buffers", "max_wal_size",
		"synchronous_commit", "full_page_writes", "default_toast_compression",
	} {
		if !slices.Contains(names, required) {
			t.Errorf("allow-list is missing %q, which materially changes results", required)
		}
	}
}

func TestSnapshotServerInfo_absentSettingIsNotAnError(t *testing.T) {
	// Version-specific settings simply do not come back on older servers. That
	// must degrade to a missing key, not a failure.
	pool := &scriptedPool{rows: &scriptedRows{rows: [][]any{
		{"server_version", "13.9", ""},
	}}}
	got, err := SnapshotServerInfo(context.Background(), pool)
	if err != nil {
		t.Fatalf("a server missing some settings must not error: %v", err)
	}
	if got.Version != "13.9" {
		t.Errorf("version: want 13.9, got %q", got.Version)
	}
	if _, present := got.Settings["default_toast_compression"]; present {
		t.Error("a setting the server did not report should be absent, not fabricated")
	}
	if got.Settings == nil {
		t.Error("Settings should be an empty map, not nil, so it encodes as {}")
	}
}

func TestSnapshotServerInfo_errorPaths(t *testing.T) {
	tests := []struct {
		name string
		pool *scriptedPool
		want string
	}{
		{"query fails", &scriptedPool{queryErr: errors.New("boom")}, "query server settings"},
		{"scan fails", &scriptedPool{rows: &scriptedRows{
			rows: [][]any{{"shared_buffers", "1", ""}}, scanErr: errors.New("bad"),
		}}, "scan server settings"},
		{"iteration fails", &scriptedPool{rows: &scriptedRows{err: errors.New("torn")}}, "read server settings"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := SnapshotServerInfo(context.Background(), tc.pool)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should name the failing step %q, got: %v", tc.want, err)
			}
		})
	}
}

func TestBuildRunResult_serverOmittedWhenUnavailable(t *testing.T) {
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second),
		testCfg(), testWeights, RunMeta{})
	if r.Server != nil {
		t.Error("server should be nil when it could not be read")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Absent, not zeroed — "not measured" must not read as "a server with no
	// settings".
	if strings.Contains(string(raw), "\"server\"") {
		t.Errorf("server key should be omitted entirely, got: %s", raw)
	}
}

func TestBuildRunResult_serverRoundTrips(t *testing.T) {
	info := &ServerInfo{
		Version:  "16.4",
		Settings: map[string]string{"shared_buffers": "16384 8kB", "synchronous_commit": "on"},
	}
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second),
		testCfg(), testWeights, RunMeta{Server: info})

	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got RunResult
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Server == nil {
		t.Fatal("server lost in round-trip")
	}
	if got.Server.Version != "16.4" {
		t.Errorf("version: want 16.4, got %q", got.Server.Version)
	}
	if got.Server.Settings["shared_buffers"] != "16384 8kB" {
		t.Errorf("settings lost: %v", got.Server.Settings)
	}
}

func TestRunResult_neverLeaksCredentials(t *testing.T) {
	// Result files get shared. PG_DSN carries the password, so no field may
	// carry it — directly or via a setting name that happens to hold a
	// connection string. This guards against a future addition to
	// capturedSettings or RunConfig quietly leaking one.
	const secret = "sup3rs3cr3t-pw"
	cfg := testCfg()
	cfg.PGDSN = "postgres://loadgen:" + secret + "@db.internal:5432/loadtest?sslmode=disable"

	info := &ServerInfo{Version: "16.4", Settings: map[string]string{}}
	for _, name := range capturedSettings {
		info.Settings[name] = "value-for-" + name
	}

	r := BuildRunResult(
		map[string]opStats{OpInsert: *statsWith([2]float64{1, 1})},
		time.Now(), time.Now().Add(time.Second), cfg, testWeights,
		RunMeta{Dataset: &DatasetSnapshot{}, Server: info, StoppedBy: StoppedByDuration},
	)
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), secret) {
		t.Errorf("result document contains the database password:\n%s", raw)
	}
	if strings.Contains(string(raw), "postgres://") {
		t.Errorf("result document contains a connection string:\n%s", raw)
	}
	// The allow-list itself must never name a credential-bearing setting.
	for _, name := range capturedSettings {
		for _, banned := range []string{"password", "conninfo", "connection_string", "primary_conninfo"} {
			if strings.Contains(name, banned) {
				t.Errorf("capturedSettings includes %q, which can carry credentials", name)
			}
		}
	}
}

// ── timeseries ───────────────────────────────────────────────────────────────

// withMinInterval lowers the fold-in threshold so tests can exercise interval
// boundaries without real second-long sleeps.
func withMinInterval(t *testing.T, seconds float64) {
	t.Helper()
	orig := minIntervalSeconds
	minIntervalSeconds = seconds
	t.Cleanup(func() { minIntervalSeconds = orig })
}

func TestTimeseries_oneEntryPerDrainedWindow(t *testing.T) {
	withMinInterval(t, 0) // record every window, however brief
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()

	ws.Record(OpInsert, 0.010, nil)
	captureStdout(t, func() { c.print(time.Now(), time.Second, nil) })
	ws.Record(OpInsert, 0.020, nil)
	captureStdout(t, func() { c.print(time.Now(), time.Second, nil) })
	ws.Record(OpInsert, 0.030, nil) // trailing, consumed by Finalize

	_, intervals, _, _ := c.Finalize()
	if len(intervals) != 3 {
		t.Fatalf("want 3 intervals (two printed windows plus the trailing one), got %d", len(intervals))
	}
	for i, iv := range intervals {
		if iv.Ops != 1 {
			t.Errorf("interval %d: want 1 op, got %d", i, iv.Ops)
		}
	}
}

func TestTimeseries_sumsExactlyToTotals(t *testing.T) {
	// The invariant that matters: both print() and Finalize() drain, and every
	// drained window must appear in the series exactly once. If these diverge,
	// observations were lost or double-counted.
	c := NewStatsCollector(testOps)
	workers := []*WorkerStats{c.NewWorkerStats(), c.NewWorkerStats()}

	var written int64
	for round := range 5 {
		for _, ws := range workers {
			for i := range 20 {
				ws.Record(testOps[i%len(testOps)], float64(round+1)*0.001, nil)
				written++
			}
		}
		captureStdout(t, func() { c.print(time.Now(), time.Second, nil) })
	}
	// A final batch with no print, so Finalize's drain has real work.
	for _, ws := range workers {
		ws.Record(OpInsert, 0.005, nil)
		written++
	}

	totals, intervals, _, _ := c.Finalize()

	var totalOps int64
	for _, s := range totals {
		totalOps += s.count
	}
	var seriesOps int64
	for _, iv := range intervals {
		seriesOps += iv.Ops
		var perOp int64
		for _, o := range iv.Operations {
			perOp += o.Count
		}
		if perOp != iv.Ops {
			t.Errorf("interval per-op counts (%d) do not sum to its own Ops (%d)", perOp, iv.Ops)
		}
	}
	if totalOps != written {
		t.Errorf("totals: want %d, got %d", written, totalOps)
	}
	if seriesOps != written {
		t.Errorf("timeseries sums to %d, totals to %d — a window was lost or "+
			"double-counted between print and Finalize", seriesOps, written)
	}
}

// TestTimeseries_perOpErrorsSumToIntervalErrors is deliberately multi-op and
// deliberately includes a sliver fold. Both existing sum-invariant tests use a
// single op, so neither can catch the fold path dropping an operation: the fold
// iterates the *predecessor's* entries, so an op idle during that window has its
// ops added to the interval total while appearing against no operation at all.
func TestTimeseries_perOpErrorsSumToIntervalErrors(t *testing.T) {
	withMinInterval(t, 5) // make the final trailing window fold, not stand alone
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()

	// Window 1: insert only, and it half-fails. read_join is deliberately absent
	// so the fold below has to append it rather than find it.
	ws.Record(OpInsert, 0.010, nil)
	ws.Record(OpInsert, 0.001, errBoom)
	captureStdout(t, func() { c.print(time.Now(), time.Second, nil) })

	// Trailing sliver: an op the predecessor never saw, plus more of one it did.
	ws.Record(OpReadJoin, 0.030, nil)
	ws.Record(OpReadJoin, 0.001, errBoom)
	ws.Record(OpInsert, 0.020, errBoom)

	totals, intervals, _, _ := c.Finalize()

	if len(intervals) != 1 {
		t.Fatalf("the sliver should have folded into the single interval, got %d", len(intervals))
	}

	var wantOps, wantErrs int64
	for _, s := range totals {
		wantOps += s.count
		wantErrs += s.errors
	}

	for _, iv := range intervals {
		var perOpCount, perOpErrs int64
		for _, o := range iv.Operations {
			perOpCount += o.Count
			perOpErrs += o.Errors
		}
		if perOpCount != iv.Ops {
			t.Errorf("per-op counts (%d) do not sum to the interval's Ops (%d) — "+
				"an op was dropped by the fold", perOpCount, iv.Ops)
		}
		if perOpErrs != iv.Errors {
			t.Errorf("per-op errors (%d) do not sum to the interval's Errors (%d)",
				perOpErrs, iv.Errors)
		}
	}

	if intervals[0].Ops != wantOps || intervals[0].Errors != wantErrs {
		t.Errorf("interval totals: want ops=%d errors=%d, got ops=%d errors=%d",
			wantOps, wantErrs, intervals[0].Ops, intervals[0].Errors)
	}

	// read_join existed only in the folded sliver, so its presence is the proof
	// the append path ran.
	var sawReadJoin bool
	for _, o := range intervals[0].Operations {
		if o.Op == OpReadJoin {
			sawReadJoin = true
			if o.Count != 2 || o.Errors != 1 {
				t.Errorf("read_join: want count=2 errors=1, got count=%d errors=%d", o.Count, o.Errors)
			}
			if !approxEq(o.P99MS, 30) {
				t.Errorf("read_join p99 should come from its one success: want 30, got %v", o.P99MS)
			}
		}
	}
	if !sawReadJoin {
		t.Error("read_join appeared only in the folded sliver and was lost entirely")
	}
}

// TestTimeseries_zeroSpanWindowReportsNoRate covers the degenerate window that
// the sliver fold normally absorbs. With folding disabled a window can be
// recorded with a span of exactly zero, and the rate must come back as 0 rather
// than dividing by it. Pre-existing gap, covered here because a NaN or +Inf
// ops_per_sec would poison a result document.
func TestTimeseries_zeroSpanWindowReportsNoRate(t *testing.T) {
	withMinInterval(t, 0) // never fold, so a zero-length window is recorded as-is
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()

	at := time.Now()
	ws.Record(OpInsert, 0.010, nil)
	captureStdout(t, func() { c.print(at, time.Second, nil) })
	ws.Record(OpInsert, 0.010, nil)
	captureStdout(t, func() { c.print(at, time.Second, nil) }) // same instant → span 0

	_, intervals, _, _ := c.Finalize()
	if len(intervals) < 2 {
		t.Fatalf("want at least 2 intervals, got %d", len(intervals))
	}
	zero := intervals[1]
	if zero.EndSeconds != zero.StartSeconds {
		t.Fatalf("expected a zero-span interval, got %.6f-%.6f", zero.StartSeconds, zero.EndSeconds)
	}
	if zero.OpsPerSec != 0 {
		t.Errorf("zero-span interval rate: want 0, got %v", zero.OpsPerSec)
	}
	for _, o := range zero.Operations {
		if o.OpsPerSec != 0 {
			t.Errorf("op %s rate in a zero-span window: want 0, got %v", o.Op, o.OpsPerSec)
		}
	}
}

func TestTimeseries_skipsEmptyWindows(t *testing.T) {
	// A run ending just after a summary drains nothing; recording that would
	// append a meaningless zero-length, zero-op entry.
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()
	ws.Record(OpInsert, 0.010, nil)

	captureStdout(t, func() { c.print(time.Now(), time.Second, nil) }) // consumes the op
	captureStdout(t, func() { c.print(time.Now(), time.Second, nil) }) // nothing left
	_, intervals, _, _ := c.Finalize()                                 // still nothing

	if len(intervals) != 1 {
		t.Errorf("want only the one non-empty window, got %d intervals", len(intervals))
	}
}

func TestTimeseries_boundariesAreContiguousAndIncreasing(t *testing.T) {
	withMinInterval(t, 0) // keep each brief window as its own entry
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()
	for range 3 {
		ws.Record(OpInsert, 0.001, nil)
		time.Sleep(2 * time.Millisecond) // ensure measurable, distinct spans
		captureStdout(t, func() { c.print(time.Now(), time.Second, nil) })
	}

	_, intervals, _, _ := c.Finalize()
	if len(intervals) < 2 {
		t.Fatalf("need at least 2 intervals, got %d", len(intervals))
	}
	if intervals[0].StartSeconds < 0 {
		t.Errorf("first interval should start at or after the run start, got %v",
			intervals[0].StartSeconds)
	}
	for i, iv := range intervals {
		if iv.EndSeconds <= iv.StartSeconds {
			t.Errorf("interval %d does not advance: %v -> %v", i, iv.StartSeconds, iv.EndSeconds)
		}
		if i > 0 && iv.StartSeconds != intervals[i-1].EndSeconds {
			t.Errorf("gap between interval %d and %d: %v then %v — windows must be "+
				"contiguous or the series misrepresents the run",
				i-1, i, intervals[i-1].EndSeconds, iv.StartSeconds)
		}
		// Rate must come from the measured span, not the configured interval.
		if iv.Ops > 0 {
			want := float64(iv.Ops) / (iv.EndSeconds - iv.StartSeconds)
			if !approxEq(iv.OpsPerSec, want) {
				t.Errorf("interval %d rate: want %v, got %v", i, want, iv.OpsPerSec)
			}
		}
	}
}

func TestTimeseries_trailingSliverFoldsIntoPreviousInterval(t *testing.T) {
	// Finalize drains microseconds after the last print. Ops landing in that gap
	// must not become an interval of their own: dividing them by a near-zero span
	// invents a throughput spike. Observed live before this was handled — 4 ops
	// over ~1 ms reported as 2971 ops/sec.
	withMinInterval(t, 0.05)
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()

	for range 100 {
		ws.Record(OpInsert, 0.001, nil)
	}
	time.Sleep(60 * time.Millisecond) // exceed minIntervalSeconds so this is a real window
	captureStdout(t, func() { c.print(time.Now(), time.Second, nil) })

	// The sliver: a few ops, then an immediate Finalize.
	for range 4 {
		ws.Record(OpInsert, 0.001, nil)
	}
	_, intervals, _, _ := c.Finalize()

	if len(intervals) != 1 {
		t.Fatalf("the sliver should have merged, want 1 interval, got %d", len(intervals))
	}
	iv := intervals[0]
	if iv.Ops != 104 {
		t.Errorf("merged interval should hold every op: want 104, got %d", iv.Ops)
	}
	// The real check: a plausible rate. 104 ops over ~1.1s is ~95/sec; the bug
	// produced thousands.
	span := iv.EndSeconds - iv.StartSeconds
	if span < minIntervalSeconds {
		t.Errorf("merged interval span collapsed: %v", span)
	}
	if iv.OpsPerSec > 100000 {
		t.Errorf("implausible rate %v ops/sec over %vs — the sliver was divided by "+
			"a near-zero span instead of being merged", iv.OpsPerSec, span)
	}
	if !approxEq(iv.OpsPerSec, float64(iv.Ops)/span) {
		t.Errorf("rate not recomputed after merge: %v vs %v", iv.OpsPerSec, float64(iv.Ops)/span)
	}
	var perOp int64
	for _, o := range iv.Operations {
		perOp += o.Count
	}
	if perOp != iv.Ops {
		t.Errorf("per-op counts (%d) lost the merged ops (interval has %d)", perOp, iv.Ops)
	}
}

func TestTimeseries_noIntervalReportsAnImplausibleRate(t *testing.T) {
	// Guards the whole series, not just the trailing entry: any window short
	// enough to distort its rate should have been folded rather than emitted.
	withMinInterval(t, 0.05)
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()

	for round := range 4 {
		for range 10 {
			ws.Record(OpInsert, 0.001, nil)
		}
		// Alternate real windows with immediate back-to-back drains.
		if round%2 == 0 {
			time.Sleep(60 * time.Millisecond)
		}
		captureStdout(t, func() { c.print(time.Now(), time.Second, nil) })
	}
	_, intervals, _, _ := c.Finalize()

	for i, iv := range intervals {
		span := iv.EndSeconds - iv.StartSeconds
		if span < minIntervalSeconds {
			t.Errorf("interval %d spans only %vs — shorter than minIntervalSeconds, "+
				"so it should have been folded into its predecessor", i, span)
		}
	}
}

func TestTimeseries_finalizeReturnsACopy(t *testing.T) {
	c := NewStatsCollector(testOps)
	ws := c.NewWorkerStats()
	ws.Record(OpInsert, 0.010, nil)

	_, first, _, _ := c.Finalize()
	n := len(first)

	ws.Record(OpInsert, 0.020, nil)
	c.Finalize()

	if len(first) != n {
		t.Errorf("a previously returned series was mutated: had %d, now %d", n, len(first))
	}
}

func TestBuildRunResult_timeseriesRoundTrips(t *testing.T) {
	series := []IntervalStats{{
		StartSeconds: 0, EndSeconds: 30, Ops: 100, Errors: 1, OpsPerSec: 3.3,
		Operations: []IntervalOp{{Op: OpInsert, Count: 60, OpsPerSec: 2, P99MS: 12.5}},
	}}
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second),
		testCfg(), testWeights, RunMeta{Timeseries: series})

	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got RunResult
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Timeseries) != 1 {
		t.Fatalf("timeseries lost in round-trip: %+v", got.Timeseries)
	}
	if got.Timeseries[0].Ops != 100 || got.Timeseries[0].EndSeconds != 30 {
		t.Errorf("interval fields lost: %+v", got.Timeseries[0])
	}
	if len(got.Timeseries[0].Operations) != 1 || got.Timeseries[0].Operations[0].P99MS != 12.5 {
		t.Errorf("per-op detail lost: %+v", got.Timeseries[0].Operations)
	}
}

func TestBuildRunResult_timeseriesOmittedWhenEmpty(t *testing.T) {
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second),
		testCfg(), testWeights, RunMeta{})
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "timeseries") {
		t.Errorf("timeseries should be omitted when there are no intervals, got: %s", raw)
	}
}

// ── stopped_by ───────────────────────────────────────────────────────────────

func TestBuildRunResult_stoppedByRoundTrips(t *testing.T) {
	// The tuning workflow is measure, change something, measure again. A run cut
	// short by Ctrl-C must be distinguishable from one that ran its course, or
	// the comparison is quietly wrong.
	for _, want := range []string{StoppedByDuration, StoppedBySignal} {
		t.Run(want, func(t *testing.T) {
			r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second),
				testCfg(), testWeights, RunMeta{StoppedBy: want})
			if r.StoppedBy != want {
				t.Errorf("StoppedBy: want %q, got %q", want, r.StoppedBy)
			}
			raw, err := json.Marshal(r)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var got RunResult
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got.StoppedBy != want {
				t.Errorf("after round-trip: want %q, got %q", want, got.StoppedBy)
			}
		})
	}
}

func TestBuildRunResult_stoppedByOmittedWhenUnset(t *testing.T) {
	r := BuildRunResult(map[string]opStats{}, time.Now(), time.Now().Add(time.Second),
		testCfg(), testWeights, RunMeta{})
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "stopped_by") {
		t.Errorf("stopped_by should be omitted when unset, got: %s", raw)
	}
}

func TestStoppedByConstants_areDistinct(t *testing.T) {
	// Guards a copy-paste that would make every run look the same.
	if StoppedByDuration == StoppedBySignal {
		t.Fatal("the two stop reasons must differ")
	}
}

// ── WriteRunResult ───────────────────────────────────────────────────────────

func TestWriteRunResult_roundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "result.json")

	started := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	totals := map[string]opStats{OpInsert: *statsWith([2]float64{10, 4})}
	want := BuildRunResult(totals, started, started.Add(4*time.Second), testCfg(), testWeights, RunMeta{})

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
		time.Now(), time.Now().Add(time.Second), testCfg(), testWeights, RunMeta{})
	if err := WriteRunResult(path, first); err != nil {
		t.Fatalf("first write: %v", err)
	}
	second := BuildRunResult(map[string]opStats{OpInsert: {count: 999}},
		time.Now(), time.Now().Add(time.Second), testCfg(), testWeights, RunMeta{})
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
