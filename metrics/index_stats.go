package metrics

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	indexSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "index_size_bytes",
		Help:      "Size of each B-tree index in bytes.",
	}, []string{"index", "table"})

	// Counter: tracks deltas against pg_stat_user_indexes.idx_scan so rate() works in PromQL.
	indexScansTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "index_scans_total",
		Help:      "Index scans observed since pod start (delta-tracked against pg_stat_user_indexes.idx_scan).",
	}, []string{"index", "table"})

	tableSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "table_size_bytes",
		Help:      "Size of each table heap in bytes.",
	}, []string{"table"})

	tableLiveTuples = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "table_live_tuples",
		Help:      "Estimated live tuples per table (pg_stat_user_tables.n_live_tup).",
	}, []string{"table"})

	tableDeadTuples = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "table_dead_tuples",
		Help:      "Estimated dead tuples per table — proxy for MVCC bloat (pg_stat_user_tables.n_dead_tup).",
	}, []string{"table"})

	tableModSinceAnalyze = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "table_mod_since_analyze",
		Help:      "Rows modified since the last analyze; high value means stale statistics.",
	}, []string{"table"})

	tableAutovacuumTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "table_autovacuum_total",
		Help:      "Autovacuum runs observed since pod start (delta-tracked against pg_stat_user_tables.autovacuum_count).",
	}, []string{"table"})

	tableAutoanalyzeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "table_autoanalyze_total",
		Help:      "Autoanalyze runs observed since pod start (delta-tracked against pg_stat_user_tables.autoanalyze_count).",
	}, []string{"table"})
)

func RegisterTableStats() {
	prometheus.MustRegister(
		tableSizeBytes, tableLiveTuples, tableDeadTuples,
		tableModSinceAnalyze, tableAutovacuumTotal, tableAutoanalyzeTotal,
	)
}

func RegisterIndexStats() {
	prometheus.MustRegister(indexSizeBytes, indexScansTotal)
}

// RunTableStatsLoop always runs — polls pg_stat_user_tables for MVCC metrics
// regardless of whether indexes are enabled. Cancels when ctx is done.
func RunTableStatsLoop(ctx context.Context, pool *pgxpool.Pool, interval time.Duration, tables []string) {
	tracker := newIndexScanTracker() // reuse same delta-tracker type for autovacuum/autoanalyze counts
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := collectTableStats(ctx, pool, tracker, tables); err != nil && ctx.Err() == nil {
				log.Printf("table stats collection error: %v", err)
			}
		}
	}
}

// indexScanTracker holds the last observed idx_scan value per index to compute deltas.
type indexScanTracker struct {
	mu       sync.Mutex
	lastSeen map[string]int64
}

func newIndexScanTracker() *indexScanTracker {
	return &indexScanTracker{lastSeen: make(map[string]int64)}
}

func (t *indexScanTracker) delta(key string, current int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	prev, ok := t.lastSeen[key]
	t.lastSeen[key] = current
	if !ok {
		return 0 // first observation — no delta yet
	}
	if current < prev {
		return 0 // pg_stat was reset (pg_stat_reset()); skip this tick
	}
	return current - prev
}

// RunIndexStatsLoop polls pg_stat_user_indexes for index sizes and scan counts.
// Only start this when CREATE_INDEXES=true. Cancels when ctx is done.
func RunIndexStatsLoop(ctx context.Context, pool *pgxpool.Pool, interval time.Duration, tables []string) {
	tracker := newIndexScanTracker()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := collectIndexStats(ctx, pool, tracker, tables); err != nil && ctx.Err() == nil {
				log.Printf("index stats collection error: %v", err)
			}
		}
	}
}

func collectTableStats(ctx context.Context, pool *pgxpool.Pool, tracker *indexScanTracker, tables []string) error {
	rows, err := pool.Query(ctx, `
		SELECT
			relname,
			pg_relation_size(relid),
			n_live_tup,
			n_dead_tup,
			n_mod_since_analyze,
			autovacuum_count,
			autoanalyze_count
		FROM pg_stat_user_tables
		WHERE schemaname = 'public'
		  AND relname = ANY($1)
	`, tables)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		var sizeBytes, liveTup, deadTup, modSinceAnalyze, autovacuumCount, autoanalyzeCount int64
		if err := rows.Scan(&tableName, &sizeBytes, &liveTup, &deadTup, &modSinceAnalyze, &autovacuumCount, &autoanalyzeCount); err != nil {
			return err
		}
		tableSizeBytes.WithLabelValues(tableName).Set(float64(sizeBytes))
		tableLiveTuples.WithLabelValues(tableName).Set(float64(liveTup))
		tableDeadTuples.WithLabelValues(tableName).Set(float64(deadTup))
		tableModSinceAnalyze.WithLabelValues(tableName).Set(float64(modSinceAnalyze))
		if d := tracker.delta(tableName+":autovacuum", autovacuumCount); d > 0 {
			tableAutovacuumTotal.WithLabelValues(tableName).Add(float64(d))
		}
		if d := tracker.delta(tableName+":autoanalyze", autoanalyzeCount); d > 0 {
			tableAutoanalyzeTotal.WithLabelValues(tableName).Add(float64(d))
		}
	}
	return rows.Err()
}

func collectIndexStats(ctx context.Context, pool *pgxpool.Pool, tracker *indexScanTracker, tables []string) error {
	rows, err := pool.Query(ctx, `
		SELECT
			ui.indexrelname,
			ui.relname,
			pg_relation_size(ui.indexrelid),
			ui.idx_scan
		FROM pg_stat_user_indexes ui
		WHERE ui.schemaname = 'public'
		  AND ui.relname = ANY($1)
	`, tables)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var indexName, tableName string
		var sizeBytes, scans int64
		if err := rows.Scan(&indexName, &tableName, &sizeBytes, &scans); err != nil {
			return err
		}
		indexSizeBytes.WithLabelValues(indexName, tableName).Set(float64(sizeBytes))
		if delta := tracker.delta(indexName, scans); delta > 0 {
			indexScansTotal.WithLabelValues(indexName, tableName).Add(float64(delta))
		}
	}
	return rows.Err()
}
