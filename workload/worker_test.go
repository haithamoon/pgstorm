package workload

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"pg-loadgen/config"
	"pg-loadgen/db"
)

var errBoom = errors.New("boom")

// fakeExecutor counts Execute calls; used to drive RunWorker without a database.
// When err is set, Execute returns it (to exercise the worker's error-log path).
type fakeExecutor struct {
	count *int64
	err   error
}

func (f fakeExecutor) Execute(ctx context.Context, op string) error {
	atomic.AddInt64(f.count, 1)
	return f.err
}

// fakeProfile satisfies Profile; only NewExecutor is used by RunWorker.
type fakeProfile struct {
	count   *int64
	execErr error
}

func (fakeProfile) Name() string                             { return "fake" }
func (fakeProfile) Schema() db.Schema                        { return db.Schema{} }
func (fakeProfile) Ops() []OpDef                             { return []OpDef{{OpInsert, "FAKE_PCT", 100}} }
func (fakeProfile) Init(*config.Config, *pgxpool.Pool) error { return nil }
func (f fakeProfile) NewExecutor(*rand.Rand) Executor {
	return fakeExecutor{count: f.count, err: f.execErr}
}

func TestRunWorker_executesAndExitsOnCancel(t *testing.T) {
	var count int64
	ws := newWorkerStats([]string{OpInsert})
	cfg := &config.Config{ThinkTimeMs: 1} // exercise the think-time branch too

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunWorker(ctx, fakeProfile{count: &count}, []WeightedOp{{OpInsert, 100}}, cfg, nil, 0, ws)
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

func TestRunWorker_logsAndContinuesOnOpError(t *testing.T) {
	var count int64
	ws := newWorkerStats([]string{OpInsert})
	cfg := &config.Config{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunWorker(ctx, fakeProfile{count: &count, execErr: errBoom}, []WeightedOp{{OpInsert, 100}}, cfg, nil, 7, ws)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	// Errors are recorded and the worker keeps running (doesn't exit on error).
	if snap := ws.snapshot(); snap[OpInsert].errors == 0 {
		t.Error("expected recorded op errors")
	}
}

func TestRunWorker_honorsRateLimiter(t *testing.T) {
	var count int64
	ws := newWorkerStats([]string{OpInsert})
	cfg := &config.Config{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	limiter := NewRateLimiter(ctx, 100) // ~100 ops/s

	done := make(chan struct{})
	go func() {
		defer close(done)
		RunWorker(ctx, fakeProfile{count: &count}, []WeightedOp{{OpInsert, 100}}, cfg, limiter, 0, ws)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	// At 100/s the feeder accrues ~15 tokens over 150ms (burst caps buffering at
	// 10), so a correct limiter yields ~15-25. An unlimited worker would do many
	// thousands. Upper bound 50 catches any ~2.5x+ throttling regression; because
	// accrual is elapsed-time based, a slow/contended host yields *fewer* tokens,
	// never more, so 50 cannot flake. Lower bound 3 is safely above the floor.
	if n := atomic.LoadInt64(&count); n < 3 || n > 50 {
		t.Errorf("rate-limited worker did %d ops in 150ms at 100/s; expected ~15-25 (bounds [3,50])", n)
	}
}
