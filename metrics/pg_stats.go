package metrics

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// bgwriter metrics — from pg_stat_bgwriter (PG14–16).
// Note: in PG17 checkpoint columns moved to pg_stat_checkpointer.
var (
	bgwriterCheckpointsTimed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "bgwriter_checkpoints_timed_total",
		Help:      "Checkpoints triggered by checkpoint_timeout.",
	})
	bgwriterCheckpointsReq = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "bgwriter_checkpoints_req_total",
		Help:      "Checkpoints triggered by WAL segment demand; high rate indicates checkpoint pressure.",
	})
	bgwriterBuffersCheckpoint = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "bgwriter_buffers_checkpoint_total",
		Help:      "Shared buffers written during checkpoints.",
	})
	bgwriterBuffersClean = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "bgwriter_buffers_clean_total",
		Help:      "Shared buffers written by the background writer.",
	})
	bgwriterBuffersBackend = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "bgwriter_buffers_backend_total",
		Help:      "Shared buffers written directly by backends; high rate means bgwriter cannot keep up.",
	})
	bgwriterCheckpointWriteSeconds = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "bgwriter_checkpoint_write_seconds_total",
		Help:      "Total time spent writing files to disk during checkpoints, in seconds.",
	})
	bgwriterCheckpointSyncSeconds = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "bgwriter_checkpoint_sync_seconds_total",
		Help:      "Total time spent syncing files to disk during checkpoints, in seconds.",
	})

	// WAL metrics — from pg_stat_wal (requires PG14+).
	walBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "wal_bytes_total",
		Help:      "Total bytes of WAL generated; rate() gives write amplification.",
	})
	walRecordsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "wal_records_total",
		Help:      "Total WAL records generated.",
	})
	walFPITotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "wal_fpi_total",
		Help:      "Full-page images written to WAL; spikes after each checkpoint.",
	})
	walBuffersFullTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "wal_buffers_full_total",
		Help:      "Times WAL was flushed to disk because WAL buffers were full.",
	})
)

func RegisterPGStats() {
	prometheus.MustRegister(
		bgwriterCheckpointsTimed,
		bgwriterCheckpointsReq,
		bgwriterBuffersCheckpoint,
		bgwriterBuffersClean,
		bgwriterBuffersBackend,
		bgwriterCheckpointWriteSeconds,
		bgwriterCheckpointSyncSeconds,
		walBytesTotal,
		walRecordsTotal,
		walFPITotal,
		walBuffersFullTotal,
	)
}

// RunPGStatsLoop polls pg_stat_bgwriter and pg_stat_wal on interval until ctx is done.
func RunPGStatsLoop(ctx context.Context, pool *pgxpool.Pool, interval time.Duration) {
	tracker := newPGStatsTracker()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := collectBgwriterStats(ctx, pool, tracker); err != nil && ctx.Err() == nil {
				log.Printf("bgwriter stats error: %v", err)
			}
			if err := collectWALStats(ctx, pool, tracker); err != nil && ctx.Err() == nil {
				log.Printf("WAL stats error: %v", err)
			}
		}
	}
}

// pgStatsTracker delta-tracks cumulative float64 values from Postgres system views.
type pgStatsTracker struct {
	mu       sync.Mutex
	lastSeen map[string]float64
}

func newPGStatsTracker() *pgStatsTracker {
	return &pgStatsTracker{lastSeen: make(map[string]float64)}
}

func (t *pgStatsTracker) delta(key string, current float64) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	prev, ok := t.lastSeen[key]
	t.lastSeen[key] = current
	if !ok || current < prev {
		return 0 // first observation or pg_stat_reset()
	}
	return current - prev
}

func collectBgwriterStats(ctx context.Context, pool *pgxpool.Pool, tracker *pgStatsTracker) error {
	var cpTimed, cpReq, bufCheckpoint, bufClean, bufBackend, writeTime, syncTime float64
	err := pool.QueryRow(ctx, `
		SELECT
			checkpoints_timed,
			checkpoints_req,
			buffers_checkpoint,
			buffers_clean,
			buffers_backend,
			checkpoint_write_time,
			checkpoint_sync_time
		FROM pg_stat_bgwriter
	`).Scan(&cpTimed, &cpReq, &bufCheckpoint, &bufClean, &bufBackend, &writeTime, &syncTime)
	if err != nil {
		return err
	}

	bgwriterCheckpointsTimed.Add(tracker.delta("cp_timed", cpTimed))
	bgwriterCheckpointsReq.Add(tracker.delta("cp_req", cpReq))
	bgwriterBuffersCheckpoint.Add(tracker.delta("buf_checkpoint", bufCheckpoint))
	bgwriterBuffersClean.Add(tracker.delta("buf_clean", bufClean))
	bgwriterBuffersBackend.Add(tracker.delta("buf_backend", bufBackend))
	// pg reports checkpoint times in milliseconds; convert to seconds
	bgwriterCheckpointWriteSeconds.Add(tracker.delta("cp_write_ms", writeTime) / 1000)
	bgwriterCheckpointSyncSeconds.Add(tracker.delta("cp_sync_ms", syncTime) / 1000)
	return nil
}

func collectWALStats(ctx context.Context, pool *pgxpool.Pool, tracker *pgStatsTracker) error {
	var walBytes, walRecords, walFPI, walBuffersFull float64
	err := pool.QueryRow(ctx, `
		SELECT wal_bytes, wal_records, wal_fpi, wal_buffers_full
		FROM pg_stat_wal
	`).Scan(&walBytes, &walRecords, &walFPI, &walBuffersFull)
	if err != nil {
		return err
	}

	walBytesTotal.Add(tracker.delta("wal_bytes", walBytes))
	walRecordsTotal.Add(tracker.delta("wal_records", walRecords))
	walFPITotal.Add(tracker.delta("wal_fpi", walFPI))
	walBuffersFullTotal.Add(tracker.delta("wal_buffers_full", walBuffersFull))
	return nil
}
