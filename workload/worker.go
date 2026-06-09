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
	}
	for _, o := range ops {
		cumulative += o.pct
		if roll < cumulative {
			return o.name
		}
	}
	return OpInsert
}

func RunWorker(ctx context.Context, pool *pgxpool.Pool, ring *SessionRing, cfg *config.Config, id int) {
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

		metrics.WorkersActive.Inc()
		start := time.Now()
		err := exec.Execute(ctx, op)
		duration := time.Since(start).Seconds()
		metrics.WorkersActive.Dec()

		metrics.RecordOp(op, duration, err)

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
