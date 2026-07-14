package workload

import (
	"context"
	"log"
	"math/rand"
	"time"

	"pg-loadgen/config"
	"pg-loadgen/metrics"
)

// RunWorker runs one worker goroutine: build a per-worker executor from the
// profile, then loop — pick an op by weight, execute it, record latency/outcome —
// until the context is cancelled. The op set and weights are profile-defined and
// resolved once by the caller.
func RunWorker(ctx context.Context, profile Profile, ops []WeightedOp, cfg *config.Config, id int, ws *WorkerStats) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	exec := profile.NewExecutor(rng)
	thinkTime := time.Duration(cfg.ThinkTimeMs) * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		roll := rng.Intn(100)
		op := SelectOp(roll, ops)

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
// a panic. Panics are deliberately NOT recovered: a bug that panics should fail
// loudly and take the process down rather than be silently masked as per-op error
// noise while /readyz stays green and the load test quietly produces meaningless
// results.
func runOp(ctx context.Context, exec Executor, op string) error {
	metrics.WorkersActive.Inc()
	defer metrics.WorkersActive.Dec()
	return exec.Execute(ctx, op)
}
