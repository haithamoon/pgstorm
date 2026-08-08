// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Haitham Gadelrab
// Licensed under the Apache License, Version 2.0; see the LICENSE file.

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

	"github.com/haithamoon/pgstorm/config"
	"github.com/haithamoon/pgstorm/db"
	"github.com/haithamoon/pgstorm/metrics"
	"github.com/haithamoon/pgstorm/workload"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	// Measure the dataset before any worker touches it. pgstorm grows the
	// database as it runs, so without a starting size a result cannot be
	// compared against another — the two were not measuring the same database.
	// Only needed when a result will actually be written.
	var datasetStart workload.DatasetSize
	var datasetOK bool
	var server *workload.ServerInfo
	if cfg.ResultJSONPath != "" {
		if cfg.RunDurationSecs == 0 {
			// The result will still be written on Ctrl-C, but its duration is
			// whatever happened to elapse — which makes it useless for comparing
			// a before-tuning run against an after-tuning one.
			log.Println("warning: RUN_DURATION_SECS=0 with a result path — the run " +
				"length will be however long until you stop it, so the result will " +
				"not be comparable against another run")
		}

		datasetStart, err = workload.SnapshotDataset(ctx, pool, profile.Schema().TrackedTables)
		if err != nil {
			log.Printf("dataset size at start: %v (result will omit dataset)", err)
		} else {
			datasetOK = true
			log.Printf("dataset at start: %.1f MB, %d live tuples",
				float64(datasetStart.TotalBytes)/(1<<20), datasetStart.LiveTuples)
		}

		// Which Postgres, and how it is configured, moves the numbers more than
		// most of pgstorm's own knobs. Read once here: these settings are static
		// for the run, and this is the state it actually executed under.
		if info, sErr := workload.SnapshotServerInfo(ctx, pool); sErr != nil {
			log.Printf("server settings: %v (result will omit server)", sErr)
		} else {
			server = &info
			log.Printf("postgres %s, %d settings recorded", info.Version, len(info.Settings))
		}
	}

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

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		ws := collector.NewWorkerStats()
		go func(id int, ws *workload.WorkerStats) {
			defer wg.Done()
			workload.RunWorker(runCtx, profile, ops, cfg, id, ws)
		}(i, ws)
	}

	// Signal readiness only after the workers are actually launched, so /readyz
	// reflects "workers started" as documented rather than flipping true while
	// setup is still in progress.
	ready.Store(true)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// Recorded in the result: an "after" run cut short by Ctrl-C must not be
	// mistaken for one that ran its configured course.
	stoppedBy := workload.StoppedBySignal
	select {
	case sig := <-sigCh:
		log.Printf("received signal %v — shutting down", sig)
		cancel()
	case <-runCtx.Done():
		stoppedBy = workload.StoppedByDuration
		log.Println("run duration elapsed — shutting down")
		cancel()
	}

	wg.Wait()
	log.Println("all workers stopped")

	// Write the result only after every worker has returned, so Finalize picks
	// up the trailing window since the last summary and nothing is still being
	// recorded. A failure here must not change the exit path — the run itself
	// already happened, and the console summaries were printed.
	if cfg.ResultJSONPath != "" {
		totals, started, ended := collector.Finalize()

		// ctx was cancelled to stop the workers, so the closing measurement needs
		// its own short-lived context or the query would fail immediately.
		var dataset *workload.DatasetSnapshot
		if datasetOK {
			dsCtx, dsCancel := context.WithTimeout(context.Background(), 10*time.Second)
			datasetEnd, dsErr := workload.SnapshotDataset(dsCtx, pool, profile.Schema().TrackedTables)
			dsCancel()
			if dsErr != nil {
				log.Printf("dataset size at end: %v (result will omit dataset)", dsErr)
			} else {
				dataset = &workload.DatasetSnapshot{
					Start:       datasetStart,
					End:         datasetEnd,
					GrowthBytes: datasetEnd.TotalBytes - datasetStart.TotalBytes,
				}
			}
		}

		result := workload.BuildRunResult(totals, started, ended, cfg, ops, workload.RunMeta{
			Dataset:   dataset,
			Server:    server,
			StoppedBy: stoppedBy,
		})
		if err := workload.WriteRunResult(cfg.ResultJSONPath, result); err != nil {
			log.Printf("write result json: %v", err)
		} else {
			log.Printf("wrote result json: %s (%d ops over %.0fs)",
				cfg.ResultJSONPath, result.Totals.Ops, result.DurationSeconds)
		}
	}

	log.Println("goodbye")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeoutSecs)*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx) //nolint
}
