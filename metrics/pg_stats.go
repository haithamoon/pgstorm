package metrics

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// bgwriter / checkpoint metrics.
// PG14–16: all sourced from pg_stat_bgwriter.
// PG17+:   checkpoint columns moved to pg_stat_checkpointer;
//          buffers_backend removed entirely (no equivalent view).
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
		Help:      "Shared buffers written directly by backends (PG14–16 only; not available on PG17+).",
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

	// Wait event metrics — snapshot of pg_stat_activity, not cumulative.
	waitEventsActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "wait_events_active",
		Help:      "Number of sessions currently waiting on a specific PostgreSQL wait event (snapshot, not cumulative).",
	}, []string{"wait_event_type", "wait_event"})

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
		waitEventsActive,
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

// RunPGStatsLoop polls pg_stat_bgwriter/pg_stat_checkpointer and pg_stat_wal on interval.
func RunPGStatsLoop(ctx context.Context, pool *pgxpool.Pool, interval time.Duration) {
	pgMajor, err := detectPGMajorVersion(ctx, pool)
	if err != nil {
		log.Printf("could not detect postgres version: %v — assuming PG16 query paths", err)
		pgMajor = 16
	}
	log.Printf("postgres major version: %d", pgMajor)

	tracker := newPGStatsTracker()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := collectBgwriterStats(ctx, pool, tracker, pgMajor); err != nil && ctx.Err() == nil {
				log.Printf("bgwriter stats error: %v", err)
			}
			if err := collectWALStats(ctx, pool, tracker); err != nil && ctx.Err() == nil {
				log.Printf("WAL stats error: %v", err)
			}
			if err := collectWaitEventStats(ctx, pool); err != nil && ctx.Err() == nil {
				log.Printf("wait event stats error: %v", err)
			}
		}
	}
}

func detectPGMajorVersion(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var major int
	err := pool.QueryRow(ctx, "SELECT current_setting('server_version_num')::int / 10000").Scan(&major)
	return major, err
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

func collectBgwriterStats(ctx context.Context, pool *pgxpool.Pool, tracker *pgStatsTracker, pgMajor int) error {
	if pgMajor >= 17 {
		return collectBgwriterStatsPG17(ctx, pool, tracker)
	}
	return collectBgwriterStatsPG16(ctx, pool, tracker)
}

// collectBgwriterStatsPG16 queries pg_stat_bgwriter which holds all columns on PG14–16.
func collectBgwriterStatsPG16(ctx context.Context, pool *pgxpool.Pool, tracker *pgStatsTracker) error {
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
	bgwriterCheckpointWriteSeconds.Add(tracker.delta("cp_write_ms", writeTime) / 1000)
	bgwriterCheckpointSyncSeconds.Add(tracker.delta("cp_sync_ms", syncTime) / 1000)
	return nil
}

// collectBgwriterStatsPG17 uses pg_stat_checkpointer (new in PG17) for checkpoint metrics
// and pg_stat_bgwriter for the remaining bgwriter-only metrics.
// buffers_backend was removed in PG17 with no direct replacement, so it is not collected.
func collectBgwriterStatsPG17(ctx context.Context, pool *pgxpool.Pool, tracker *pgStatsTracker) error {
	// Checkpoint metrics — moved to pg_stat_checkpointer in PG17.
	// Column renames: checkpoints_timed→num_timed, checkpoints_req→num_requested,
	// buffers_checkpoint→buffers_written; write_time/sync_time kept same names.
	var cpTimed, cpReq, bufCheckpoint, writeTime, syncTime float64
	err := pool.QueryRow(ctx, `
		SELECT num_timed, num_requested, buffers_written, write_time, sync_time
		FROM pg_stat_checkpointer
	`).Scan(&cpTimed, &cpReq, &bufCheckpoint, &writeTime, &syncTime)
	if err != nil {
		return err
	}
	bgwriterCheckpointsTimed.Add(tracker.delta("cp_timed", cpTimed))
	bgwriterCheckpointsReq.Add(tracker.delta("cp_req", cpReq))
	bgwriterBuffersCheckpoint.Add(tracker.delta("buf_checkpoint", bufCheckpoint))
	bgwriterCheckpointWriteSeconds.Add(tracker.delta("cp_write_ms", writeTime) / 1000)
	bgwriterCheckpointSyncSeconds.Add(tracker.delta("cp_sync_ms", syncTime) / 1000)

	// bgwriter-only metrics — still in pg_stat_bgwriter on PG17.
	var bufClean float64
	err = pool.QueryRow(ctx, `SELECT buffers_clean FROM pg_stat_bgwriter`).Scan(&bufClean)
	if err != nil {
		return err
	}
	bgwriterBuffersClean.Add(tracker.delta("buf_clean", bufClean))

	return nil
}

func collectWaitEventStats(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT wait_event_type, wait_event, count(*)::float8
		FROM pg_stat_activity
		WHERE wait_event IS NOT NULL
		GROUP BY 1, 2
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	waitEventsActive.Reset()
	for rows.Next() {
		var evType, ev string
		var count float64
		if err := rows.Scan(&evType, &ev, &count); err != nil {
			return err
		}
		waitEventsActive.WithLabelValues(evType, ev).Set(count)
	}
	return rows.Err()
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
