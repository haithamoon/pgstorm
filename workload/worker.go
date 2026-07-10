package workload

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"pg-loadgen/config"
	"pg-loadgen/metrics"
)

func SelectOp(roll int, cfg *config.Config) string {
	cumulative := 0
	ops := []struct {
		name string
		pct  int
	}{
		{OpInsert, cfg.WritePct},
		{OpReadSimple, cfg.ReadSimplePct},
		{OpReadJoin, cfg.ReadJoinPct},
		{OpUpdate, cfg.UpdatePct},
		{OpDelete, cfg.DeletePct},
		{OpReadByIP, cfg.ReadIPPct},
	}
	for _, o := range ops {
		cumulative += o.pct
		if roll < cumulative {
			return o.name
		}
	}
	return OpInsert
}

func RunWorker(ctx context.Context, pool *pgxpool.Pool, ring *SessionRing, cfg *config.Config, id int, ws *WorkerStats) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	exec := NewExecutor(pool, ring, cfg, rng)
	thinkTime := time.Duration(cfg.ThinkTimeMs) * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		roll := rng.Intn(100)
		op := SelectOp(roll, cfg)

		start := time.Now()
		err := runOp(ctx, exec, op)
		duration := time.Since(start).Seconds()

		metrics.RecordOp(op, duration, err)
		ws.Record(op, duration, err)

		if err != nil && ctx.Err() == nil {
			log.Printf("worker %d op=%s duration=%.3fs err=%v", id, op, duration, err)
		}

		if thinkTime > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(thinkTime):
			}
		}
	}
}

// runOp executes a single operation while accounting for it in the WorkersActive
// gauge. The deferred Dec keeps the gauge balanced on every return path, including
// a panic — the previous inline Dec was skipped if the op panicked. Panics are
// deliberately NOT recovered: a bug that panics should fail loudly and take the
// process down rather than be silently masked as per-op error noise while /readyz
// stays green and the load test quietly produces meaningless results.
func runOp(ctx context.Context, exec *Executor, op string) error {
	metrics.WorkersActive.Inc()
	defer metrics.WorkersActive.Dec()
	return exec.Execute(ctx, op)
}
