// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Haitham Gadelrab
// Licensed under the Apache License, Version 2.0; see the LICENSE file.

package workload

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/haithamoon/pgstorm/config"
	"github.com/haithamoon/pgstorm/metrics"
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

		switch {
		case errors.Is(err, errSkipped):
			// The op did no DB work (e.g. empty ring at cold start). Don't record it
			// as an executed op or a ~0ms latency sample — that would deflate
			// percentiles and inflate throughput. Surface it as a skip instead.
			metrics.RecordSkip(op)

		case isShutdownCancellation(ctx, err):
			// The run ended while this op was in flight. It is an artifact of
			// stopping, not something the database did, so it is not recorded at
			// all — deliberately not as a skip either, since it did real work.
			//
			// Left uncounted, every clean run reported roughly WORKERS errors. The
			// harm was never the handful of bogus rows; it was teaching the
			// operator that a few errors are normal, which is exactly the habit
			// that makes a real fault invisible later.

		default:
			metrics.RecordOp(op, duration, err)
			ws.Record(op, duration, err)

			if err != nil {
				log.Printf("worker %d op=%s duration=%.3fs err=%v", id, op, duration, err)
			}
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

// isShutdownCancellation reports whether err is this op being interrupted by the
// run ending, rather than anything the database did.
//
// Both halves of the conjunction are load-bearing and neither may be dropped:
//
//   - ctx.Err() != nil alone would swallow every genuine failure that happens to
//     land during shutdown, which is the window a struggling database is most
//     likely to fail in.
//   - errors.Is alone would misread a per-op timeout as shutdown. There is no
//     per-op context today, but the moment a profile adds one, its
//     DeadlineExceeded must still count as a real error — the worker-context
//     check is what preserves that.
//
// pgx normalises a cancelled query back to context.Canceled or
// context.DeadlineExceeded (pgconn.normalizeTimeoutError), and every wrap in
// ops.go uses %w, so errors.Is traverses the whole chain. Note this does not
// catch every shutdown artifact: after pgx force-closes a connection a follow-up
// op can return "use of closed network connection", which is a net.Error but not
// a timeout and so never becomes a context error. Those still count as errors,
// which is the conservative direction — reporting a real-looking error that was
// only shutdown noise is recoverable; silently dropping a real one is not.
func isShutdownCancellation(ctx context.Context, err error) bool {
	return ctx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
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
