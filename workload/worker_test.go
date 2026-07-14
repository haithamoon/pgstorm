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

func TestRunWorker_logsAndContinuesOnOpError(t *testing.T) {
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

	// Errors are recorded and the worker keeps running (doesn't exit on error).
	if snap := ws.snapshot(); snap[OpInsert].errors == 0 {
		t.Error("expected recorded op errors")
	}
}
