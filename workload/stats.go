package workload

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// bucketBounds are upper bounds in milliseconds, matching the Prometheus histogram.
// A fixed array (not a slice) so numBuckets is a compile-time constant derived from it,
// keeping the buckets array size below in lockstep with the bounds.
var bucketBounds = [...]float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000}

const numBuckets = len(bucketBounds)

type opStats struct {
	count   int64
	errors  int64
	buckets [numBuckets]int64 // counts per latency bucket
	inf     int64             // observations above the highest bucket
}

// WorkerStats holds one worker's window stats. Owned by the worker goroutine;
// accessed by the collector only during a snapshot (brief mutex hold).
type WorkerStats struct {
	mu   sync.Mutex
	data map[string]*opStats
}

func newWorkerStats(ops []string) *WorkerStats {
	ws := &WorkerStats{data: make(map[string]*opStats, len(ops))}
	for _, op := range ops {
		ws.data[op] = &opStats{}
	}
	return ws
}

// Record is called by the worker goroutine after every op.
func (ws *WorkerStats) Record(op string, durationSec float64, err error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	s := ws.data[op]
	s.count++
	if err != nil {
		s.errors++
	}
	ms := durationSec * 1000
	placed := false
	for i, bound := range bucketBounds {
		if ms <= bound {
			s.buckets[i]++
			placed = true
			break
		}
	}
	if !placed {
		s.inf++
	}
}

// snapshot atomically copies and resets the window data.
func (ws *WorkerStats) snapshot() map[string]opStats {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	out := make(map[string]opStats, len(ws.data))
	for op, s := range ws.data {
		out[op] = *s
		*s = opStats{}
	}
	return out
}

// StatsCollector aggregates stats from all workers and prints periodic summaries.
type StatsCollector struct {
	mu      sync.Mutex
	workers []*WorkerStats
	start   time.Time
	ops     []string // op names to track/print, from the active profile
}

func NewStatsCollector(ops []string) *StatsCollector {
	return &StatsCollector{start: time.Now(), ops: ops}
}

// NewWorkerStats creates a WorkerStats registered with the collector.
// Call once per worker before starting it.
func (c *StatsCollector) NewWorkerStats() *WorkerStats {
	ws := newWorkerStats(c.ops)
	c.mu.Lock()
	c.workers = append(c.workers, ws)
	c.mu.Unlock()
	return ws
}

// RunSummaryLoop prints a summary every interval until ctx is cancelled.
func (c *StatsCollector) RunSummaryLoop(ctx context.Context, interval time.Duration, pool *pgxpool.Pool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			c.print(t, interval, pool)
		}
	}
}

func (c *StatsCollector) print(now time.Time, window time.Duration, pool *pgxpool.Pool) {
	c.mu.Lock()
	workers := make([]*WorkerStats, len(c.workers))
	copy(workers, c.workers)
	c.mu.Unlock()

	// Merge snapshots from all workers
	merged := make(map[string]*opStats, len(c.ops))
	for _, op := range c.ops {
		merged[op] = &opStats{}
	}
	for _, ws := range workers {
		snap := ws.snapshot()
		for op, s := range snap {
			m := merged[op]
			m.count += s.count
			m.errors += s.errors
			m.inf += s.inf
			for i := range s.buckets {
				m.buckets[i] += s.buckets[i]
			}
		}
	}

	windowSecs := window.Seconds()
	elapsed := now.Sub(c.start).Round(time.Second)

	var totalOps, totalErrors int64
	for _, s := range merged {
		totalOps += s.count
		totalErrors += s.errors
	}

	sep := strings.Repeat("━", 68)
	fmt.Printf("\n%s\n", sep)
	fmt.Printf("  30s summary [%s | +%s elapsed]\n", now.Format("15:04:05"), elapsed)
	fmt.Printf("  total  %6s ops   %.1f ops/s   errors: %d\n",
		commaf(totalOps), float64(totalOps)/windowSecs, totalErrors)
	fmt.Printf("  ┌──────────────┬────────┬─────────┬────────┬────────┬────────┐\n")
	fmt.Printf("  │ %-12s │ %6s │ %7s │ %6s │ %6s │ %6s │\n",
		"op", "count", "ops/s", "p50 ms", "p95 ms", "p99 ms")
	fmt.Printf("  ├──────────────┼────────┼─────────┼────────┼────────┼────────┤\n")
	for _, op := range c.ops {
		s := merged[op]
		p50 := percentile(s, 0.50)
		p95 := percentile(s, 0.95)
		p99 := percentile(s, 0.99)
		fmt.Printf("  │ %-12s │ %6d │ %7.1f │ %6.1f │ %6.1f │ %6.1f │\n",
			op, s.count, float64(s.count)/windowSecs, p50, p95, p99)
	}
	fmt.Printf("  └──────────────┴────────┴─────────┴────────┴────────┴────────┘\n")

	if pool != nil {
		stat := pool.Stat()
		fmt.Printf("  pool  acquired=%d  idle=%d  total=%d  max=%d  │  workers=%d\n",
			stat.AcquiredConns(), stat.IdleConns(), stat.TotalConns(), stat.MaxConns(), len(workers))
	}
	fmt.Printf("%s\n\n", sep)
}

// percentile estimates a percentile value (q in 0–1) from the fixed-bucket
// histogram using linear interpolation within the bucket that contains the
// target rank — the same approach as Prometheus's histogram_quantile. Each
// bucket i spans (lo, hi] where hi = bucketBounds[i] and lo = bucketBounds[i-1]
// (lo = 0 for the first bucket); observations are assumed uniform within it, so
// the estimate lands between the bounds rather than snapping to the upper edge.
func percentile(s *opStats, q float64) float64 {
	total := s.inf
	for _, b := range s.buckets {
		total += b
	}
	if total == 0 {
		return 0
	}
	target := q * float64(total)
	var cum float64
	for i, b := range s.buckets {
		if b == 0 {
			continue
		}
		if cum+float64(b) >= target {
			lo := 0.0
			if i > 0 {
				lo = bucketBounds[i-1]
			}
			hi := bucketBounds[i]
			frac := (target - cum) / float64(b)
			if frac < 0 {
				frac = 0
			} else if frac > 1 {
				frac = 1
			}
			return lo + (hi-lo)*frac
		}
		cum += float64(b)
	}
	// Target falls in the +Inf overflow bucket (> the highest finite bound):
	// there is no upper edge to interpolate to, so report that bound as a floor.
	return bucketBounds[numBuckets-1]
}

func commaf(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	rem := len(s) % 3
	if rem > 0 {
		b.WriteString(s[:rem])
	}
	for i := rem; i < len(s); i += 3 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}
