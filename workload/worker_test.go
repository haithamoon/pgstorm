// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Haitham Gadelrab
// Licensed under the Apache License, Version 2.0; see the LICENSE file.

package workload

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haithamoon/pgstorm/config"
	"github.com/haithamoon/pgstorm/db"
	"github.com/haithamoon/pgstorm/metrics"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// counterValue reads a Prometheus counter's current value without the (unvendored)
// testutil package.
func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return 0
	}
	return m.GetCounter().GetValue()
}

var errBoom = errors.New("boom")

// fakeExecutor counts Execute calls; used to drive RunWorker without a database.
// When err is set, Execute returns it (to exercise the worker's error-log path).
//
// onCall, when set, takes precedence and receives the 1-based call number. It
// exists because RunWorker re-checks the context at the top of every iteration,
// so a test that merely cancels from the outside races the loop and may see the
// worker exit before any op observes the cancellation. A hook that cancels and
// *then* returns makes that ordering deterministic.
type fakeExecutor struct {
	count  *int64
	err    error
	onCall func(n int64) error
}

func (f fakeExecutor) Execute(ctx context.Context, op string) error {
	n := atomic.AddInt64(f.count, 1)
	if f.onCall != nil {
		return f.onCall(n)
	}
	return f.err
}

// fakeProfile satisfies Profile; only NewExecutor is used by RunWorker.
type fakeProfile struct {
	count   *int64
	execErr error
	onCall  func(n int64) error
}

func (fakeProfile) Name() string                             { return "fake" }
func (fakeProfile) Schema() db.Schema                        { return db.Schema{} }
func (fakeProfile) Ops() []OpDef                             { return []OpDef{{OpInsert, "FAKE_PCT", 100}} }
func (fakeProfile) Init(*config.Config, *pgxpool.Pool) error { return nil }
func (f fakeProfile) NewExecutor(*rand.Rand) Executor {
	return fakeExecutor{count: f.count, err: f.execErr, onCall: f.onCall}
}

// runWorkerUntilDone drives one worker to completion with the given executor
// hook, returning its recorded window.
func runWorkerUntilDone(t *testing.T, ctx context.Context, onCall func(n int64) error) map[string]opStats {
	t.Helper()
	var count int64
	ws := newWorkerStats([]string{OpInsert})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunWorker(ctx, fakeProfile{count: &count, onCall: onCall},
			[]WeightedOp{{OpInsert, 100}}, &config.Config{}, 0, ws)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunWorker did not exit")
	}
	return ws.snapshot()
}

func TestRunWorker_executesAndExitsOnCancel(t *testing.T) {
	var count int64
	ws := newWorkerStats([]string{OpInsert})
	cfg := &config.Config{ThinkTimeMs: 1} // exercise the think-time branch too

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunWorker(ctx, fakeProfile{count: &count}, []WeightedOp{{OpInsert, 100}}, cfg, 0, ws)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunWorker did not exit within 1s after cancel")
	}

	if atomic.LoadInt64(&count) == 0 {
		t.Error("RunWorker executed 0 ops")
	}
	if snap := ws.snapshot(); snap[OpInsert].count == 0 {
		t.Error("RunWorker recorded 0 ops in stats")
	}
}

func TestRunWorker_recordsAndContinuesOnOpError(t *testing.T) {
	var count int64
	ws := newWorkerStats([]string{OpInsert})
	cfg := &config.Config{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunWorker(ctx, fakeProfile{count: &count, execErr: errBoom}, []WeightedOp{{OpInsert, 100}}, cfg, 7, ws)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	// The worker keeps looping after an error rather than returning, so many ops
	// accumulate. Asserting > 1 is what distinguishes "kept running" from "exited
	// on the first failure"; the old `!= 0` passed either way.
	snap := ws.snapshot()
	if snap[OpInsert].errors < 2 {
		t.Errorf("worker should keep running after an error, recorded %d", snap[OpInsert].errors)
	}
	// errBoom is not a context error, so it survives the shutdown filter.
	if snap[OpInsert].count != snap[OpInsert].errors {
		t.Errorf("every op failed, so count should equal errors: count=%d errors=%d",
			snap[OpInsert].count, snap[OpInsert].errors)
	}
}

// TestIsShutdownCancellation guards the conjunction. Simplifying it in either
// direction breaks one of these rows: dropping the errors.Is half swallows real
// failures that happen during shutdown, and dropping the ctx.Err() half would
// misclassify a future per-op timeout as shutdown noise.
func TestIsShutdownCancellation(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	live := context.Background()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"wrapped cancel during shutdown", cancelled,
			fmt.Errorf("insert session: %w", context.Canceled), true},
		{"wrapped deadline during shutdown", cancelled,
			fmt.Errorf("query timeout: %w", context.DeadlineExceeded), true},
		{"bare cancel during shutdown", cancelled, context.Canceled, true},

		{"real DB error during shutdown", cancelled, errBoom, false},
		{"server-side cancel is a real error", cancelled,
			&pgconn.PgError{Code: "57014", Message: "canceling statement due to user request"}, false},
		{"cancel while the run is live", live, context.Canceled, false},
		{"deadline while the run is live", live, context.DeadlineExceeded, false},
		{"no error", cancelled, nil, false},
		{"skip is not a cancellation", cancelled, errSkipped, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isShutdownCancellation(tt.ctx, tt.err); got != tt.want {
				t.Errorf("want %v, got %v", tt.want, got)
			}
		})
	}
}

func TestRunWorker_shutdownCancellationIsNotRecorded(t *testing.T) {
	// Every clean run used to report roughly WORKERS errors from ops caught
	// mid-flight by the shutdown, which taught operators to ignore the error
	// count entirely.
	ctx, cancel := context.WithCancel(context.Background())
	before := counterValue(metrics.OpsTotal.WithLabelValues(OpInsert, "error"))

	snap := runWorkerUntilDone(t, ctx, func(n int64) error {
		if n < 3 {
			return nil // some real work first
		}
		cancel() // the run ends while this op is in flight
		return fmt.Errorf("insert session: %w", context.Canceled)
	})

	s := snap[OpInsert]
	if s.errors != 0 {
		t.Errorf("shutdown cancellation recorded as an error: errors=%d", s.errors)
	}
	if s.count != 2 {
		t.Errorf("only the completed ops should be counted, got %d", s.count)
	}
	if got := counterValue(metrics.OpsTotal.WithLabelValues(OpInsert, "error")) - before; got != 0 {
		t.Errorf("shutdown cancellation reached ops_total: delta=%v", got)
	}
}

func TestRunWorker_realErrorDuringShutdownIsStillRecorded(t *testing.T) {
	// The dangerous direction: a database failing at the moment the run ends is
	// exactly when it is most interesting, and must not be filtered away.
	ctx, cancel := context.WithCancel(context.Background())

	snap := runWorkerUntilDone(t, ctx, func(n int64) error {
		cancel()
		return errBoom
	})

	s := snap[OpInsert]
	if s.errors != 1 || s.count != 1 {
		t.Errorf("a real error during shutdown must be recorded: count=%d errors=%d", s.count, s.errors)
	}
}

// A ring-skip (errSkipped) must NOT be counted as an executed op or a ~0ms latency
// sample; it is surfaced via ops_skipped_total instead.
func TestRunWorker_skipsNotCountedAsOps(t *testing.T) {
	var count int64
	ws := newWorkerStats([]string{OpInsert})
	cfg := &config.Config{}

	before := counterValue(metrics.OpsSkipped.WithLabelValues(OpInsert))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunWorker(ctx, fakeProfile{count: &count, execErr: errSkipped}, []WeightedOp{{OpInsert, 100}}, cfg, 0, ws)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	if atomic.LoadInt64(&count) == 0 {
		t.Fatal("executor never ran")
	}
	snap := ws.snapshot()
	if snap[OpInsert].count != 0 || snap[OpInsert].errors != 0 {
		t.Errorf("skip recorded as op: count=%d errors=%d (want 0/0)", snap[OpInsert].count, snap[OpInsert].errors)
	}
	if after := counterValue(metrics.OpsSkipped.WithLabelValues(OpInsert)); after <= before {
		t.Errorf("ops_skipped_total not incremented: before=%v after=%v", before, after)
	}
}
