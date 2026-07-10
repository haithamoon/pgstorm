package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"pg-loadgen/config"
	"pg-loadgen/db"
	"pg-loadgen/metrics"
	"pg-loadgen/workload"
)

var ready atomic.Bool

func main() {
	// Load .env if present; existing env vars take priority over .env values.
	if err := godotenv.Load(); err == nil {
		log.Println("loaded .env file")
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	profile, err := workload.GetProfile(cfg.Profile)
	if err != nil {
		log.Fatalf("select profile: %v", err)
	}
	ops, err := workload.ResolveWeights(profile.Ops())
	if err != nil {
		log.Fatalf("resolve op weights: %v", err)
	}
	log.Printf("workload profile: %s", profile.Name())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		log.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	if err := db.MigrateWithLock(ctx, pool, cfg, profile.Schema()); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	// Build the profile's shared state (pools, rings, payload templates) after the
	// schema exists and before any worker starts.
	if err := profile.Init(cfg, pool); err != nil {
		log.Fatalf("init profile: %v", err)
	}

	metrics.Register()
	prometheus.MustRegister(metrics.NewPoolCollector(pool))
	metrics.RegisterTableStats()
	metrics.RegisterPGStats()
	if cfg.CreateIndexes {
		metrics.RegisterIndexStats()
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler: mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server: %v", err)
		}
	}()

	log.Printf("starting %d workers", cfg.Workers)

	collector := workload.NewStatsCollector(workload.OpNames(ops))

	var runCtx context.Context
	var runCancel context.CancelFunc
	if cfg.RunDurationSecs > 0 {
		runCtx, runCancel = context.WithTimeout(ctx, time.Duration(cfg.RunDurationSecs)*time.Second)
	} else {
		runCtx, runCancel = context.WithCancel(ctx)
	}
	defer runCancel()

	summaryInterval := time.Duration(cfg.SummaryIntervalSecs) * time.Second
	go collector.RunSummaryLoop(runCtx, summaryInterval, pool)

	indexStatsInterval := time.Duration(cfg.IndexStatsIntervalSecs) * time.Second
	trackedTables := profile.Schema().TrackedTables
	go metrics.RunTableStatsLoop(runCtx, pool, indexStatsInterval, trackedTables)
	go metrics.RunPGStatsLoop(runCtx, pool, indexStatsInterval)
	if cfg.CreateIndexes {
		go metrics.RunIndexStatsLoop(runCtx, pool, indexStatsInterval, trackedTables)
	}

	// Shared closed-loop rate limiter (nil when TARGET_RATE_PER_SEC is 0/unset).
	// The limit is per replica (this process, across its workers) — with N replicas
	// the database sees up to N × TARGET_RATE_PER_SEC.
	limiter := workload.NewRateLimiter(runCtx, cfg.TargetRatePerSec)
	if limiter != nil {
		log.Printf("rate limiting enabled: target %d ops/s per replica (across %d workers)", cfg.TargetRatePerSec, cfg.Workers)
		if cfg.ThinkTimeMs > 0 {
			log.Printf("warning: THINK_TIME_MS=%d and TARGET_RATE_PER_SEC=%d are both set; effective rate is the tighter of the two", cfg.ThinkTimeMs, cfg.TargetRatePerSec)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		ws := collector.NewWorkerStats()
		go func(id int, ws *workload.WorkerStats) {
			defer wg.Done()
			workload.RunWorker(runCtx, profile, ops, cfg, limiter, id, ws)
		}(i, ws)
	}

	// Signal readiness only after the workers are actually launched, so /readyz
	// reflects "workers started" as documented rather than flipping true while
	// setup is still in progress.
	ready.Store(true)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		log.Printf("received signal %v — shutting down", sig)
		cancel()
	case <-runCtx.Done():
		log.Println("run duration elapsed — shutting down")
		cancel()
	}

	wg.Wait()
	log.Println("all workers stopped — goodbye")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeoutSecs)*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx) //nolint
}
